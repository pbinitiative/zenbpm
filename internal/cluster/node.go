package cluster

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/pbinitiative/zenbpm/internal/log"

	"github.com/bwmarrin/snowflake"
	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/controller"
	"github.com/pbinitiative/zenbpm/internal/cluster/jobmanager"
	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	"github.com/pbinitiative/zenbpm/internal/cluster/partition"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/server"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/cluster/store"
	"github.com/pbinitiative/zenbpm/internal/cluster/types"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"github.com/rqlite/rqlite/v8/cluster"
	"github.com/rqlite/rqlite/v8/tcp"
)

// ZenNode is the main node of the zen cluster.
// It serves as a controller of the underlying RqLite clusters as partitions of the main cluster.
//
//	     ┌─────────────────┐   ┌─────────────────┐
//	     │Zen cluster 1(L) │   │Zen cluster 1(F) │
//	     │ ┌────────────┐  ◄───┼ ┌────────────┐  │
//	     │ │Partition 1 │  ┼───► │Partition 1 │  │
//	     │ │RqLite 1 (L)┼──┼───┼─►RqLite 1 (F)│  │
//	┌────┼ │            ◄──┼───┼─┼            │  ┼──┐
//	│┌───► └────────────┘  │   │ └────────────┘  ◄─┐│
//	││   └───────┬───▲─────┘   └───▲───┬─────────┘ ││
//	││   ┌───────▼───┼─────┐   ┌───┴───▼─────────┐ ││
//	││   │Zen cluster 1(F) │   │Zen cluster 1(F) │ ││
//	││   │ ┌────────────┐  │   │ ┌────────────┐  │ ││
//	││   │ │Partition 2 │  │   │ │Partition 2 │  │ ││
//	││   │ │RqLite 2 (L)┼──┼───┼─►RqLite 2 (F)│  │ ││
//	││   │ │            ◄──┼───┼─┼            │  │ ││
//	││   │ └────────────┘  │   │ └────────────┘  │ ││
//	││   └──────▲─┬────────┘   └─┬──▲────────────┘ ││
//	│└──────────┼─┼──────────────┘  │              ││
//	└───────────┼─┼─────────────────┘              ││
//	            │ └────────────────────────────────┘│
//	            └───────────────────────────────────┘
//
// Changes in the cluster are always directed towards a leader of the Zen cluster.
// When leader decides that this change should take place it writes it into the raft log.
// Change takes effect only after it has been applied to the state from the raft log.
type ZenNode struct {
	ctx        context.Context
	store      *store.Store
	controller *controller.Controller
	server     *server.Server
	client     *client.ClientManager
	logger     hclog.Logger
	muxLn      net.Listener
	JobManager *jobmanager.JobManager
	// TODO: add tracing to all the methods on ZenNode where it makes sense
}

type DeployResult struct {
	Key         int64
	IsDuplicate bool
}

// StartZenNode Starts a cluster node
func StartZenNode(mainCtx context.Context, conf config.Config) (*ZenNode, error) {
	node := &ZenNode{
		logger: hclog.Default().Named(fmt.Sprintf("zen-node-%s", conf.Cluster.NodeId)),
		ctx:    mainCtx,
	}

	mux, muxLn, err := network.NewNodeMux(conf.Cluster.Addr)
	if err != nil {
		return nil, fmt.Errorf("failed to create ZenNode mux on %s: %w", conf.Cluster.Addr, err)
	}

	node.muxLn = muxLn
	node.controller, err = controller.NewController(mux, conf.Cluster)
	if err != nil {
		return nil, fmt.Errorf("failed to create node controller: %w", err)
	}

	zenRaftLn := network.NewZenBpmRaftListener(mux)
	raftTn := tcp.NewLayer(zenRaftLn, network.NewZenBpmRaftDialer())
	node.store = store.New(
		raftTn,
		node.controller.ClusterStateChangeNotification,
		store.DefaultConfig(conf.Cluster),
	)
	if err = node.store.Open(); err != nil {
		return nil, fmt.Errorf("failed to open store: %w", err)
	}

	node.client = client.NewClientManager(node.store)
	err = node.controller.Start(node.store, node.client)
	if err != nil {
		return nil, fmt.Errorf("failed to start node controller: %w", err)
	}

	node.JobManager = jobmanager.New(mainCtx, node.store, node.client, node, node)
	node.controller.AddClusterStateChangeHook(node.JobManager.OnClusterStateChange)

	clusterSrvLn := network.NewZenBpmClusterListener(mux)
	clusterSrv := server.New(clusterSrvLn, node.store, node.controller, node.JobManager)
	if err = clusterSrv.Open(); err != nil {
		return nil, fmt.Errorf("failed to open cluster GRPC server: %w", err)
	}
	node.server = clusterSrv

	// bootstrapping logic
	nodes, err := node.store.Nodes()
	if err != nil {
		errInfo := fmt.Errorf("failed to get nodes %s", err.Error())
		node.logger.Error(errInfo.Error())
		return nil, errInfo
	}
	if len(nodes) > 0 {
		node.logger.Info("Preexisting configuration detected. Skipping bootstrap.")

		err = node.store.WaitForAllApplied(120 * time.Second) // TODO: pull out to config
		if err != nil {
			node.logger.Error("Failed to apply log until timeout was reached: %s", err)
		}

		node.JobManager.Start()
		return node, nil
	}

	bootDoneFn := func() bool {
		leader, _ := node.store.LeaderAddr()
		return leader != ""
	}
	clusterSuf := cluster.VoterSuffrage(!conf.Cluster.Raft.NonVoter)

	joiner := NewJoiner(node.client, conf.Cluster.Raft.JoinAttempts, conf.Cluster.Raft.JoinInterval)
	if len(conf.Cluster.Raft.JoinAddresses) > 0 && conf.Cluster.Raft.BootstrapExpect == 0 {
		// Explicit join operation requested, so do it.
		j, err := joiner.Do(mainCtx, conf.Cluster.Raft.JoinAddresses, node.store.ID(), conf.Cluster.Adv, clusterSuf)
		if err != nil {
			return nil, fmt.Errorf("failed to join cluster: %s", err.Error())
		}
		node.logger.Info(fmt.Sprintf("successfully joined cluster at %s", j))
	} else if len(conf.Cluster.Raft.JoinAddresses) > 0 && conf.Cluster.Raft.BootstrapExpect > 0 {
		// Bootstrap with explicit join addresses requests.
		bs := NewBootstrapper(
			cluster.NewAddressProviderString(conf.Cluster.Raft.JoinAddresses),
			node.client,
		)
		err := bs.Boot(mainCtx, node.store.ID(), conf.Cluster.Adv, clusterSuf, bootDoneFn, conf.Cluster.Raft.BootstrapExpectTimeout)
		if err != nil {
			return nil, fmt.Errorf("failed to bootstrap cluster: %s", err.Error())
		}
	}
	_, err = node.store.WaitForLeader(5 * time.Second)
	if err != nil {
		return nil, fmt.Errorf("timeout expired before leader information was received")
	}
	err = node.store.WaitForAllApplied(120 * time.Second) // TODO: pull out to config
	if err != nil {
		node.logger.Error("Failed to apply log until timeout was reached: %s", err)
	}

	node.JobManager.Start()

	return node, nil
}

func (node *ZenNode) Stop() error {
	var joinErr error
	err := node.controller.NotifyShutdown()
	if err != nil {
		joinErr = errors.Join(joinErr, fmt.Errorf("failed to notify cluster about node shutdown: %w", err))
	}
	err = node.server.Close()
	if err != nil {
		joinErr = errors.Join(joinErr, fmt.Errorf("failed to stop grpc server: %w", err))
	}
	err = node.client.Close()
	if err != nil {
		joinErr = errors.Join(joinErr, fmt.Errorf("failed to close client manager: %w", err))
	}
	err = node.controller.Stop()
	if err != nil {
		joinErr = errors.Join(joinErr, fmt.Errorf("failed to stop node controller: %w", err))
	}
	err = node.store.Close(true)
	if err != nil {
		joinErr = errors.Join(joinErr, fmt.Errorf("failed to close zen node store: %w", err))
	}
	err = node.muxLn.Close()
	if err != nil {
		joinErr = errors.Join(joinErr, fmt.Errorf("failed to close mux listner: %w", err))
	}
	return joinErr
}

func (node *ZenNode) IsPartitionLeader(ctx context.Context, partition uint32) bool {
	return node.controller.IsPartitionLeader(ctx, partition)
}

func (node *ZenNode) IsAnyPartitionLeader(ctx context.Context) bool {
	return node.controller.IsAnyPartitionLeader(ctx)
}

func (node *ZenNode) LeastStressedPartitionLeader(ctx context.Context) (proto.ZenServiceClient, error) {
	partition, err := node.store.ClusterState().LeastStressedPartition()
	if err != nil {
		return nil, fmt.Errorf("failed to get least stressed partition leader: %w", err)
	}
	return node.client.PartitionLeader(partition.Id)
}

// GetPartitionStore exposes Storage interface for use in engines
func (node *ZenNode) GetPartitionStore(ctx context.Context, partition uint32) (storage.Storage, error) {
	partitionNode := node.controller.GetPartition(ctx, partition)
	if partitionNode == nil {
		return nil, fmt.Errorf("partition %d storage not available in zen node", partition)
	}
	return partitionNode.DB, nil
}

// GetReadOnlyDB returns a database object preferably on partition where node is a follower
func (node *ZenNode) GetReadOnlyDB(ctx context.Context) (*partition.DB, error) {
	return node.controller.GetReadOnlyDB(ctx)
}

// GetDmnResourceDefinitions does not have to go through the grpc as all partitions should have the same definitions so it can just read it from any of its partitions
func (node *ZenNode) GetDmnResourceDefinitions(ctx context.Context) ([]proto.DmnResourceDefinition, error) {
	db, err := node.GetReadOnlyDB(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get dmn resource definitions: %w", err)
	}
	definitions, err := db.Queries.FindAllDmnResourceDefinitions(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read dmn resource definitions from database: %w", err)
	}
	resp := make([]proto.DmnResourceDefinition, 0, len(definitions))
	for _, def := range definitions {
		resp = append(resp, proto.DmnResourceDefinition{
			Key:                  &def.Key,
			Version:              ptr.To(int32(def.Version)),
			DecisionDefinitionId: &def.DmnID,
			Definition:           []byte(def.DmnData),
			ResourceName:         &def.DmnResourceName,
		})
	}
	return resp, nil
}

// GetDmnResourceDefinition does not have to go through the grpc as all partitions should have the same definitions so it can just read it from any of its partitions
func (node *ZenNode) GetDmnResourceDefinition(ctx context.Context, key int64) (proto.DmnResourceDefinition, error) {
	db, err := node.GetReadOnlyDB(ctx)
	if err != nil {
		return proto.DmnResourceDefinition{}, fmt.Errorf("failed to get dmn resource definition: %w", err)
	}
	def, err := db.Queries.FindDmnResourceDefinitionByKey(ctx, key)
	if err != nil {
		return proto.DmnResourceDefinition{}, fmt.Errorf("failed to read dmn resource definition by key: %d from database: %w", key, err)
	}
	return proto.DmnResourceDefinition{
		Key:                  &def.Key,
		Version:              ptr.To(int32(def.Version)),
		DecisionDefinitionId: &def.DmnID,
		Definition:           []byte(def.DmnData),
		ResourceName:         &def.DmnResourceName,
	}, nil
}

func (node *ZenNode) DeployDmnResourceDefinitionToAllPartitions(ctx context.Context, data []byte) (DeployResult, error) {
	key, err := node.GetDmnResourceDefinitionKeyByBytes(ctx, data)
	if err != nil {
		log.Error("Failed to get dmn resource definition key by bytes: %s", err)
	}
	if key != 0 {
		return DeployResult{key, true}, err
	}
	gen, _ := snowflake.NewNode(0)
	definitionKey := gen.Generate()
	state := node.store.ClusterState()
	var errJoin error
	for _, partition := range state.Partitions {
		pLeader := state.Nodes[partition.LeaderId]
		client, err := node.client.For(pLeader.Addr)
		if err != nil {
			errJoin = errors.Join(errJoin, fmt.Errorf("failed to get client: %w", err))
		}
		resp, err := client.DeployDmnResourceDefinition(ctx, &proto.DeployDmnResourceDefinitionRequest{
			Key:  ptr.To(definitionKey.Int64()),
			Data: data,
		})
		if err != nil || resp.Error != nil {
			e := fmt.Errorf("client call to deploy dmn resource definition failed")
			if err != nil {
				errJoin = errors.Join(errJoin, fmt.Errorf("%w: %w", e, err))
			} else if resp.Error != nil {
				errJoin = errors.Join(errJoin, fmt.Errorf("%w: %w", e, errors.New(resp.Error.GetMessage())))
			}
		}
	}
	if errJoin != nil {
		return DeployResult{definitionKey.Int64(), false}, errJoin
	}
	return DeployResult{definitionKey.Int64(), false}, nil
}

func (node *ZenNode) GetDmnResourceDefinitionKeyByBytes(ctx context.Context, data []byte) (int64, error) {
	db, err := node.GetReadOnlyDB(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get database for dmn resource definition key lookup: %w", err)
	}
	md5sum := md5.Sum(data)
	key, err := db.Queries.GetDmnResourceDefinitionKeyByChecksum(ctx, md5sum[:])
	if err != nil && err.Error() != "No result row" {
		return 0, fmt.Errorf("failed to find dmn resource definition by checksum: %w", err)
	}
	return key, nil
}

func (node *ZenNode) EvaluateDecision(ctx context.Context, bindingType string, decisionId string, versionTag string, variables map[string]any) (*proto.EvaluatedDRDResult, error) {
	candidateNode, err := node.GetStatus().GetLeastStressedPartitionLeader()
	if err != nil {
		return nil, fmt.Errorf("failed to get node to evaluate decision: %w", err)
	}
	client, err := node.client.For(candidateNode.Addr)
	if err != nil {
		return nil, fmt.Errorf("failed to get client to evaluate decision: %w", err)
	}
	vars, err := json.Marshal(variables)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal variables to evaluate decision: %w", err)
	}
	resp, err := client.EvaluateDecision(ctx, &proto.EvaluateDecisionRequest{
		BindingType: &bindingType,
		DecisionId:  &decisionId,
		VersionTag:  &versionTag,
		Variables:   vars,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", fmt.Errorf("failed to find and evaluate decision %s", decisionId), err)
	}

	return resp, nil
}

func (node *ZenNode) DeployProcessDefinitionToAllPartitions(ctx context.Context, data []byte) (DeployResult, error) {
	key, err := node.GetDefinitionKeyByBytes(ctx, data)
	if err != nil {
		log.Error("Failed to get definition key by bytes: %s", err)
	}
	if key != 0 {
		return DeployResult{key, true}, err
	}
	gen, _ := snowflake.NewNode(0)
	definitionKey := gen.Generate()
	state := node.store.ClusterState()
	var errJoin error
	for _, partition := range state.Partitions {
		pLeader := state.Nodes[partition.LeaderId]
		client, err := node.client.For(pLeader.Addr)
		if err != nil {
			errJoin = errors.Join(errJoin, fmt.Errorf("failed to get client: %w", err))
		}
		resp, err := client.DeployProcessDefinition(ctx, &proto.DeployProcessDefinitionRequest{
			Key:  ptr.To(definitionKey.Int64()),
			Data: data,
		})
		if err != nil || resp.Error != nil {
			e := fmt.Errorf("client call to deploy process definition failed")
			if err != nil {
				errJoin = errors.Join(errJoin, fmt.Errorf("%w: %w", e, err))
			} else if resp.Error != nil {
				errJoin = errors.Join(errJoin, fmt.Errorf("%w: %w", e, errors.New(resp.Error.GetMessage())))
			}
		}
	}
	if errJoin != nil {
		return DeployResult{definitionKey.Int64(), false}, errJoin
	}
	return DeployResult{definitionKey.Int64(), false}, nil
}

func (node *ZenNode) GetDefinitionKeyByBytes(ctx context.Context, data []byte) (int64, error) {
	db, err := node.GetReadOnlyDB(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get database for definition key lookup: %w", err)
	}
	md5sum := md5.Sum(data)
	key, err := db.Queries.GetDefinitionKeyByChecksum(ctx, md5sum[:])
	if err != nil && err.Error() != "No result row" {
		return 0, fmt.Errorf("failed to find process definition by checksum: %w", err)
	}
	return key, nil
}

func (node *ZenNode) CompleteJob(ctx context.Context, key int64, variables map[string]any) error {
	partition := zenflake.GetPartitionId(key)
	client, err := node.client.PartitionLeader(partition)
	if err != nil {
		return fmt.Errorf("failed to get client: %w", err)
	}
	vars, err := json.Marshal(variables)
	if err != nil {
		return fmt.Errorf("failed marshal variables: %w", err)
	}
	resp, err := client.CompleteJob(ctx, &proto.CompleteJobRequest{
		Key:       &key,
		Variables: vars,
	})
	if err != nil || resp.Error != nil {
		e := fmt.Errorf("client call to complete job failed")
		if err != nil {
			return fmt.Errorf("%w: %w", e, err)
		} else if resp.Error != nil {
			return fmt.Errorf("%w: %w", e, errors.New(resp.Error.GetMessage()))
		}
	}
	return nil
}

func (node *ZenNode) ResolveIncident(ctx context.Context, key int64) error {
	partition := zenflake.GetPartitionId(key)
	client, err := node.client.PartitionLeader(partition)
	if err != nil {
		return fmt.Errorf("failed to get client: %w", err)
	}
	resp, err := client.ResolveIncident(ctx, &proto.ResolveIncidentRequest{
		IncidentKey: &key,
	})
	if err != nil || resp.Error != nil {
		e := fmt.Errorf("client call to resolve incident failed")
		if err != nil {
			return fmt.Errorf("%w: %w", e, err)
		} else if resp.Error != nil {
			return fmt.Errorf("%w: %w", e, errors.New(resp.Error.GetMessage()))
		}
	}
	return nil
}

func (node *ZenNode) PublishMessage(ctx context.Context, name string, correlationKey string, variables map[string]any) error {
	partitionId := node.store.ClusterState().GetPartitionIdFromString(correlationKey)
	partitionNode := node.controller.GetPartition(ctx, partitionId)
	if partitionNode == nil {
		return fmt.Errorf("partition %d not found for correlation key %s", partitionId, correlationKey)
	}
	msPointer, err := partitionNode.DB.FindActiveMessageSubscriptionPointer(
		ctx,
		name,
		correlationKey,
	)
	if err != nil {
		return err
	}

	partitionId = zenflake.GetPartitionId(msPointer.MessageSubscriptionKey)
	client, err := node.client.PartitionLeader(partitionId)
	if err != nil {
		return fmt.Errorf("failed to get partition leader: %w", err)
	}

	vars, err := json.Marshal(variables)
	if err != nil {
		return fmt.Errorf("failed marshal variables: %w", err)
	}

	resp, err := client.PublishMessage(ctx, &proto.PublishMessageRequest{
		Key:       &msPointer.MessageSubscriptionKey,
		Variables: vars,
	})
	if err != nil || resp.Error != nil {
		e := fmt.Errorf("client call to publish message failed")
		if err != nil {
			return fmt.Errorf("%w: %w", e, err)
		} else if resp.Error != nil {
			return fmt.Errorf("%w: %w", e, errors.New(resp.Error.GetMessage()))
		}
	}

	return nil
}

// GetProcessDefinitions does not have to go through the grpc as all partitions should have the same definitions so it can just read it from any of its partitions
func (node *ZenNode) GetProcessDefinitions(ctx context.Context, page int32, size int32) (proto.ProcessDefinitionsPage, error) {
	db, err := node.GetReadOnlyDB(ctx)
	if err != nil {
		return proto.ProcessDefinitionsPage{}, fmt.Errorf("failed to get process definitions: %w", err)
	}
	definitions, err := db.Queries.GetProcessDefinitionsPage(ctx, sql.GetProcessDefinitionsPageParams{
		Offset: int64((page - 1) * size),
		Limit:  int64(size),
	})
	if err != nil {
		return proto.ProcessDefinitionsPage{}, fmt.Errorf("failed to read process definitions from database: %w", err)
	}
	totalCount, err := db.Queries.GetProcessDefinitionsCount(ctx)
	if err != nil {
		return proto.ProcessDefinitionsPage{}, fmt.Errorf("failed to get count of process definitions from database: %w", err)
	}

	resp := make([]*proto.ProcessDefinition, 0, len(definitions))
	for _, def := range definitions {
		resp = append(resp, &proto.ProcessDefinition{
			Key:        &def.Key,
			Version:    ptr.To(int32(def.Version)),
			ProcessId:  &def.BpmnProcessID,
			Definition: []byte(def.BpmnData),
		})
	}
	return proto.ProcessDefinitionsPage{
		Items:      resp,
		TotalCount: ptr.To(int32(totalCount)),
	}, nil
}

// GetLatestProcessDefinition does not have to go through the grpc as all partitions should have the same definitions so it can just read it from any of its partitions
func (node *ZenNode) GetLatestProcessDefinition(ctx context.Context, processId string) (proto.ProcessDefinition, error) {
	db, err := node.GetReadOnlyDB(ctx)
	if err != nil {
		return proto.ProcessDefinition{}, fmt.Errorf("failed to get process definition: %w", err)
	}
	def, err := db.Queries.FindLatestProcessDefinitionById(ctx, processId)
	if err != nil {
		return proto.ProcessDefinition{}, fmt.Errorf("failed to read process definition from database: %w", err)
	}
	return proto.ProcessDefinition{
		Key:        &def.Key,
		Version:    ptr.To(int32(def.Version)),
		ProcessId:  &def.BpmnProcessID,
		Definition: []byte(def.BpmnData),
	}, nil
}

// GetProcessDefinition does not have to go through the grpc as all partitions should have the same definitions so it can just read it from any of its partitions
func (node *ZenNode) GetProcessDefinition(ctx context.Context, key int64) (proto.ProcessDefinition, error) {
	db, err := node.GetReadOnlyDB(ctx)
	if err != nil {
		return proto.ProcessDefinition{}, fmt.Errorf("failed to get process definition: %w", err)
	}
	def, err := db.Queries.FindProcessDefinitionByKey(ctx, key)
	if err != nil {
		return proto.ProcessDefinition{}, fmt.Errorf("failed to read process definition from database: %w", err)
	}
	return proto.ProcessDefinition{
		Key:        &def.Key,
		Version:    ptr.To(int32(def.Version)),
		ProcessId:  &def.BpmnProcessID,
		Definition: []byte(def.BpmnData),
	}, nil
}

func (node *ZenNode) StartCpuProfile(ctx context.Context, nodeId string) error {
	state := node.store.ClusterState()
	targetNode, err2 := state.GetNode(nodeId)
	if err2 != nil {
		return err2
	}
	client, err := node.client.For(targetNode.Addr)
	if err != nil {
		return err
	}

	resp, err := client.StartCpuProfiler(ctx, &proto.CpuProfilerRequest{})
	if err != nil || resp.Error != nil {
		e := fmt.Errorf("failed to start cpu profiler")
		if err != nil {
			return fmt.Errorf("%w: %w", e, err)
		} else if resp.Error != nil {
			return fmt.Errorf("%w: %w", e, errors.New(resp.Error.GetMessage()))
		}
	}

	return nil
}

func (node *ZenNode) StopCpuProfile(ctx context.Context, nodeId string) ([]byte, error) {
	state := node.store.ClusterState()
	targetNode, err2 := state.GetNode(nodeId)
	if err2 != nil {
		return nil, err2
	}
	client, err := node.client.For(targetNode.Addr)
	if err != nil {
		return nil, err
	}

	resp, err := client.StopCpuProfiler(ctx, &proto.CpuProfilerRequest{})
	if err != nil || resp.Error != nil {
		e := fmt.Errorf("failed to stop cpu profiler")
		if err != nil {
			return nil, fmt.Errorf("%w: %w", e, err)
		} else if resp.Error != nil {
			return nil, fmt.Errorf("%w: %w", e, errors.New(resp.Error.GetMessage()))
		}
	}

	return resp.Pprof, nil
}

func (node *ZenNode) CreateInstance(
	ctx context.Context,
	processDefinitionKey int64,
	variables map[string]any,
	timeToLive *types.TTL,
	businessKey *string,
) (*proto.ProcessInstance, error) {
	state := node.store.ClusterState()
	candidateNode, err := state.GetLeastStressedPartitionLeader()
	if err != nil {
		return nil, fmt.Errorf("failed to get node to create process instance: %w", err)
	}
	client, err := node.client.For(candidateNode.Addr)
	if err != nil {
		return nil, fmt.Errorf("failed to get client to create process instance: %w", err)
	}
	vars, err := json.Marshal(variables)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal variables to create process instance: %w", err)
	}
	if timeToLive == nil {
		if node.controller.Config.Persistence.InstanceHistoryTTL != 0 {
			timeToLive = &node.controller.Config.Persistence.InstanceHistoryTTL
		}
	}
	resp, err := client.CreateInstance(ctx, &proto.CreateInstanceRequest{
		StartBy: &proto.CreateInstanceRequest_DefinitionKey{
			DefinitionKey: processDefinitionKey,
		},
		Variables:   vars,
		HistoryTTL:  (*int64)(timeToLive),
		BusinessKey: businessKey,
	})
	if err != nil || resp.Error != nil {
		e := fmt.Errorf("failed to create process instance")
		if err != nil {
			return nil, fmt.Errorf("%w: %w", e, err)
		} else if resp.Error != nil {
			return nil, fmt.Errorf("%w: %w", e, errors.New(resp.Error.GetMessage()))
		}
	}
	return resp.Process, nil
}

func (node *ZenNode) ModifyProcessInstance(ctx context.Context, processInstanceKey int64, elementInstanceIdsToTerminate []int64, elementIdsToStartInstance []string, variables map[string]any) (*proto.ProcessInstance, []*proto.ExecutionToken, error) {
	state := node.store.ClusterState()
	partitionId := zenflake.GetPartitionId(processInstanceKey)
	follower, err := state.GetPartitionFollower(partitionId)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to follower node to modify process instance: %w", err)
	}
	client, err := node.client.For(follower.Addr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get client to modify process instance: %w", err)
	}

	vars, err := json.Marshal(variables)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal variables to modify process instance: %w", err)
	}

	resp, err := client.ModifyProcessInstance(ctx, &proto.ModifyProcessInstanceRequest{
		ProcessInstanceKey:            &processInstanceKey,
		ElementInstanceIdsToTerminate: elementInstanceIdsToTerminate,
		ElementIdsToStartInstance:     elementIdsToStartInstance,
		Variables:                     vars,
	})
	if err != nil || resp.Error != nil {
		e := fmt.Errorf("failed to modify process instance")
		if err != nil {
			return nil, nil, fmt.Errorf("%w: %w", e, err)
		} else if resp.Error != nil {
			return nil, nil, fmt.Errorf("%w: %w", e, errors.New(resp.Error.GetMessage()))
		}
	}
	return resp.Process, resp.ExecutionTokens, nil
}

// GetJobs will contact follower nodes and return jobs in partitions they are following
func (node *ZenNode) GetJobs(ctx context.Context, page int32, size int32, jobType *string, jobState *runtime.ActivityState) ([]*proto.PartitionedJobs, error) {
	state := node.store.ClusterState()
	result := make([]*proto.PartitionedJobs, 0, len(state.Partitions))

	for partitionID := range state.Partitions {
		// TODO: we can smack these into goroutines
		follower, err := state.GetPartitionFollower(partitionID)
		if err != nil {
			return result, fmt.Errorf("failed to read follower node to get jobs: %w", err)
		}
		client, err := node.client.For(follower.Addr)
		if err != nil {
			return result, fmt.Errorf("failed to get client to get jobs: %w", err)
		}
		var reqState *int64
		if jobState != nil {
			reqState = ptr.To(int64(*jobState))
		}
		resp, err := client.GetJobs(ctx, &proto.GetJobsRequest{
			Page:       &page,
			Size:       &size,
			Partitions: []uint32{partitionID},
			JobType:    jobType,
			State:      reqState,
		})
		if err != nil || resp.Error != nil {
			e := fmt.Errorf("failed to get jobs from partition %d", partitionID)
			if err != nil {
				return nil, fmt.Errorf("%w: %w", e, err)
			} else if resp.Error != nil {
				return nil, fmt.Errorf("%w: %w", e, errors.New(resp.Error.GetMessage()))
			}
		}
		result = append(result, resp.Partitions...)
	}
	return result, nil
}

// GetProcessInstances will contact follower nodes and return instances in partitions they are following
func (node *ZenNode) GetProcessInstances(
	ctx context.Context,
	processDefinitionKey *int64, businessKey *string,
	parentProcessInstanceKey int64,
	page int32,
	size int32,
) ([]*proto.PartitionedProcessInstances, error) {
	state := node.store.ClusterState()
	result := make([]*proto.PartitionedProcessInstances, 0, len(state.Partitions))

	for partitionId := range state.Partitions {
		// TODO: we can smack these into goroutines
		follower, err := state.GetPartitionFollower(partitionId)
		if err != nil {
			return result, fmt.Errorf("failed to get follower node to get process instances: %w", err)
		}
		client, err := node.client.For(follower.Addr)
		if err != nil {
			return result, fmt.Errorf("failed to get client to get process instances: %w", err)
		}
		resp, err := client.GetProcessInstances(ctx, &proto.GetProcessInstancesRequest{
			Page:          &page,
			Size:          &size,
			Partitions:    []uint32{partitionId},
			DefinitionKey: processDefinitionKey,
			ParentKey:     &parentProcessInstanceKey,
			BusinessKey:   businessKey,
		})
		if err != nil || resp.Error != nil {
			e := fmt.Errorf("failed to get process instances from partition %d", partitionId)
			if err != nil {
				return nil, fmt.Errorf("%w: %w", e, err)
			} else if resp.Error != nil {
				return nil, fmt.Errorf("%w: %w", e, errors.New(resp.Error.GetMessage()))
			}
		}
		result = append(result, resp.Partitions...)
	}

	return result, nil
}

// GetProcessInstance will contact follower node of partition that contains process instance
func (node *ZenNode) GetProcessInstance(ctx context.Context, processInstanceKey int64) (*proto.ProcessInstance, []*proto.ExecutionToken, error) {
	state := node.store.ClusterState()
	partitionId := zenflake.GetPartitionId(processInstanceKey)
	follower, err := state.GetPartitionFollower(partitionId)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get follower node to get process instance: %w", err)
	}
	client, err := node.client.For(follower.Addr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get client to get process instance: %w", err)
	}
	resp, err := client.GetProcessInstance(ctx, &proto.GetProcessInstanceRequest{
		ProcessInstanceKey: &processInstanceKey,
	})
	if err != nil || resp.Error != nil {
		e := fmt.Errorf("failed to get process instance from partition %d", partitionId)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: %w", e, err)
		} else if resp.Error != nil {
			return nil, nil, fmt.Errorf("%w: %w", e, errors.New(resp.Error.GetMessage()))
		}
	}

	return resp.Processes, resp.ExecutionTokens, nil
}

// GetProcessInstance will contact follower node of partition that contains process instance
func (node *ZenNode) GetProcessInstanceJobs(ctx context.Context, processInstanceKey int64) ([]*proto.Job, error) {
	state := node.store.ClusterState()
	partitionId := zenflake.GetPartitionId(processInstanceKey)
	follower, err := state.GetPartitionFollower(partitionId)
	if err != nil {
		return nil, fmt.Errorf("failed to get follower node to get process instance: %w", err)
	}
	client, err := node.client.For(follower.Addr)
	if err != nil {
		return nil, fmt.Errorf("failed to get client to get process instance: %w", err)
	}
	resp, err := client.GetProcessInstanceJobs(ctx, &proto.GetProcessInstanceJobsRequest{
		ProcessInstanceKey: &processInstanceKey,
	})
	if err != nil || resp.Error != nil {
		e := fmt.Errorf("failed to get process instance jobs from partition %d", partitionId)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", e, err)
		} else if resp.Error != nil {
			return nil, fmt.Errorf("%w: %w", e, errors.New(resp.Error.GetMessage()))
		}
	}

	return resp.Jobs, nil
}

// GetFlowElementHistory will contact follower node of partition that contains process instance
func (node *ZenNode) GetFlowElementHistory(ctx context.Context, processInstanceKey int64) ([]*proto.FlowElement, error) {
	state := node.store.ClusterState()
	partitionId := zenflake.GetPartitionId(processInstanceKey)
	follower, err := state.GetPartitionFollower(partitionId)
	if err != nil {
		return nil, fmt.Errorf("failed to get follower node to get process instance: %w", err)
	}
	client, err := node.client.For(follower.Addr)
	if err != nil {
		return nil, fmt.Errorf("failed to get client to get process instance: %w", err)
	}
	resp, err := client.GetFlowElementHistory(ctx, &proto.GetFlowElementHistoryRequest{
		ProcessInstanceKey: &processInstanceKey,
	})
	if err != nil || resp.Error != nil {
		e := fmt.Errorf("failed to get process instance flow element history from partition %d", partitionId)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", e, err)
		} else if resp.Error != nil {
			return nil, fmt.Errorf("%w: %w", e, errors.New(resp.Error.GetMessage()))
		}
	}
	return resp.Flow, nil
}

// GetIncidents will contact follower node of partition that contains process instance
func (node *ZenNode) GetIncidents(ctx context.Context, processInstanceKey int64) ([]*proto.Incident, error) {
	state := node.store.ClusterState()
	partitionId := zenflake.GetPartitionId(processInstanceKey)
	follower, err := state.GetPartitionFollower(partitionId)
	if err != nil {
		return nil, fmt.Errorf("failed to get follower node to get process instance: %w", err)
	}
	client, err := node.client.For(follower.Addr)
	if err != nil {
		return nil, fmt.Errorf("failed to get client to get process instance: %w", err)
	}
	resp, err := client.GetIncidents(ctx, &proto.GetIncidentsRequest{
		ProcessInstanceKey: &processInstanceKey,
	})
	if err != nil || resp.Error != nil {
		e := fmt.Errorf("failed to get incidents from partition %d", partitionId)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", e, err)
		} else if resp.Error != nil {
			return nil, fmt.Errorf("%w: %w", e, errors.New(resp.Error.GetMessage()))
		}
	}
	return resp.Incidents, nil
}

func (node *ZenNode) GetStatus() state.Cluster {
	if node.store == nil {
		return state.Cluster{}
	}
	return node.store.ClusterState()
}

func (node *ZenNode) StartProcessInstanceOnElements(ctx context.Context, processDefinitionKey int64, startingElementIds []string, variables map[string]any) (*proto.ProcessInstance, error) {
	state := node.store.ClusterState()
	candidateNode, err := state.GetLeastStressedPartitionLeader()
	if err != nil {
		return nil, fmt.Errorf("failed to get node to start process instance: %w", err)
	}
	client, err := node.client.For(candidateNode.Addr)
	if err != nil {
		return nil, fmt.Errorf("failed to get client to start process instance: %w", err)
	}
	vars, err := json.Marshal(variables)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal variables to start process instance: %w", err)
	}
	resp, err := client.StartProcessInstanceOnElements(ctx, &proto.StartInstanceOnElementIdsRequest{
		DefinitionKey:      &processDefinitionKey,
		StartingElementIds: startingElementIds,
		Variables:          vars,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start process instance: %w", err)
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("failed to start process instance: %s", resp.Error.GetMessage())
	}
	return resp.Process, nil
}

func (node *ZenNode) LoadJobsToDistribute(jobTypes []string, idsToSkip []int64, count int64) ([]sql.Job, error) {
	// read jobs from all the partitions where this node is a leader
	databases := node.controller.AllPartitionLeaderDBs(node.ctx)
	if len(databases) == 0 {
		return nil, fmt.Errorf("no partitions where node is a leader were found")
	}
	jobsAcc := make([]sql.Job, 0)
	// hack to not send NULL into sqlite
	if len(idsToSkip) == 0 {
		idsToSkip = []int64{0}
	}
	for _, db := range databases {
		jobs, err := db.Queries.FindWaitingJobs(node.ctx, sql.FindWaitingJobsParams{
			KeySkip: idsToSkip,
			Type:    jobTypes,
			Limit:   count,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to load jobs from partition %d: %w", db.Partition, err)
		}
		jobsAcc = append(jobsAcc, jobs...)
	}
	return jobsAcc, nil
}

func (node *ZenNode) JobCompleteByKey(ctx context.Context, jobKey int64, variables map[string]any) error {
	partitionId := zenflake.GetPartitionId(jobKey)
	engine := node.controller.PartitionEngine(ctx, partitionId)
	if engine == nil {
		return fmt.Errorf("Engine to complete job was not found on the node")
	}
	err := engine.JobCompleteByKey(ctx, jobKey, variables)
	if err != nil {
		return err
	}
	return nil
}

func (node *ZenNode) JobFailByKey(ctx context.Context, jobKey int64, message string, errorCode *string, variables map[string]any) error {
	partitionId := zenflake.GetPartitionId(jobKey)
	engine := node.controller.PartitionEngine(ctx, partitionId)
	if engine == nil {
		return fmt.Errorf("Engine to fail job was not found on the node")
	}
	err := engine.JobFailByKey(ctx, jobKey, message, errorCode, variables)
	if err != nil {
		return err
	}
	return nil
}
