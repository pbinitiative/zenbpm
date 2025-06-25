package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/server"
	"github.com/pbinitiative/zenbpm/internal/cluster/store"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/internal/sql"
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
	store      *store.Store
	controller *controller
	server     *server.Server
	client     *client.ClientManager
	logger     hclog.Logger
	muxLn      net.Listener
	// TODO: add tracing to all the methods on ZenNode where it makes sense
}

// StartZenNode Starts a cluster node
func StartZenNode(mainCtx context.Context, conf config.Config) (*ZenNode, error) {
	node := ZenNode{
		logger: hclog.Default().Named(fmt.Sprintf("zen-node-%s", conf.Cluster.NodeId)),
	}

	mux, muxLn, err := network.NewNodeMux(conf.Cluster.Addr)
	if err != nil {
		return nil, fmt.Errorf("failed to create ZenNode mux on %s: %w", conf.Cluster.Addr, err)
	}

	node.muxLn = muxLn
	node.controller, err = NewController(mux, conf.Cluster)
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

	clusterSrvLn := network.NewZenBpmClusterListener(mux)
	clusterSrv := server.New(clusterSrvLn, node.store, node.controller)
	if err = clusterSrv.Open(); err != nil {
		return nil, fmt.Errorf("failed to open cluster GRPC server: %w", err)
	}
	node.server = clusterSrv

	node.client = client.NewClientManager(node.store)
	err = node.store.WaitForAllApplied(120 * time.Second) // TODO: pull out to config
	if err != nil {
		node.logger.Error("Failed to apply log until timeout was reached: %s", err)
	}
	err = node.controller.Start(node.store, node.client)
	if err != nil {
		return nil, fmt.Errorf("failed to start node controller: %w", err)
	}

	// bootstrapping logic
	nodes, err := node.store.Nodes()
	if err != nil {
		errInfo := fmt.Errorf("failed to get nodes %s", err.Error())
		node.logger.Error(errInfo.Error())
		return nil, errInfo
	}
	if len(nodes) > 0 {
		node.logger.Info("Preexisting configuration detected. Skipping bootstrap.")
		return &node, nil
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
		return &node, nil
	}

	if len(conf.Cluster.Raft.JoinAddresses) > 0 && conf.Cluster.Raft.BootstrapExpect > 0 {
		// Bootstrap with explicit join addresses requests.
		bs := NewBootstrapper(
			cluster.NewAddressProviderString(conf.Cluster.Raft.JoinAddresses),
			node.client,
		)
		err := bs.Boot(mainCtx, node.store.ID(), conf.Cluster.Adv, clusterSuf, bootDoneFn, conf.Cluster.Raft.BootstrapExpectTimeout)
		return &node, err

	}

	return &node, nil
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
	partitionNode, ok := node.controller.partitions[partition]
	if !ok {
		return nil, fmt.Errorf("partition %d storage not available in zen node", partition)
	}
	return partitionNode.rqliteDB, nil
}

// GetPartitionDB exposes DBTX interface for use in internal packages
func (node *ZenNode) GetPartitionDB(ctx context.Context, partition uint32) (sql.DBTX, error) {
	partitionNode, ok := node.controller.partitions[partition]
	if !ok {
		return nil, fmt.Errorf("partition %d storage not available in zen node", partition)
	}
	return partitionNode.rqliteDB, nil
}

// GetReadOnlyDB returns a database object preferably on partition where node is a follower
func (node *ZenNode) GetReadOnlyDB(ctx context.Context) (*RqLiteDB, error) {
	for _, node := range node.controller.partitions {
		if !node.IsLeader(ctx) {
			return node.rqliteDB, nil
		}
	}
	for _, node := range node.controller.partitions {
		return node.rqliteDB, nil
	}
	return nil, fmt.Errorf("no partition available to get read only database")
}

func (node *ZenNode) DeployDefinitionToAllPartitions(ctx context.Context, data []byte) (int64, error) {
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
		resp, err := client.DeployDefinition(ctx, &proto.DeployDefinitionRequest{
			Key:  definitionKey.Int64(),
			Data: data,
		})
		if err != nil || resp.Error != nil {
			e := fmt.Errorf("client call to deploy definition failed")
			if err != nil {
				errJoin = errors.Join(errJoin, fmt.Errorf("%w: %w", e, err))
			} else if resp.Error != nil {
				errJoin = errors.Join(errJoin, fmt.Errorf("%w: %w", e, errors.New(resp.Error.GetMessage())))
			}
		}
	}
	if errJoin != nil {
		return definitionKey.Int64(), errJoin
	}
	return definitionKey.Int64(), nil
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
		Key:       key,
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

// ActivateJob will call activate on all partition leaders, when it receives the first batch of jobs from the stream it closes all the active streams and returns them.
// TODO: we will need to have a locking logic on the server side + some round robin for active connections
// This client will then have to ack and lock on the jobs it will send to the API client
func (node *ZenNode) ActivateJob(ctx context.Context, jobType string) ([]*proto.InternalJob, error) {
	state := node.store.ClusterState()
	var errJoin error
	wg := sync.WaitGroup{}
	activateCtx, activateCancel := context.WithCancel(ctx)
	errChan := make(chan error, len(state.Partitions))
	jobsChan := make(chan *proto.InternalJob, len(state.Partitions))
	defer func() {
		activateCancel()
	}()
	for _, partition := range state.Partitions {
		pLeader := state.Nodes[partition.LeaderId]
		client, err := node.client.For(pLeader.Addr)
		if err != nil {
			errJoin = errors.Join(errJoin, fmt.Errorf("failed to get client: %w", err))
		}
		stream, err := client.ActivateJob(activateCtx, &proto.ActivateJobRequest{
			JobType: jobType,
		})
		if err != nil {
			errJoin = errors.Join(errJoin, fmt.Errorf("client call to create activate job stream failed: %w", err))
			continue
		}
		wg.Add(1)
		go func() {
			var errJoin error
			resp, err := stream.Recv()
			defer func() {
				activateCancel()
				wg.Done()
			}()
			if err != nil || resp.Error != nil {
				e := fmt.Errorf("client call to activate job failed")
				if err != nil {
					errJoin = errors.Join(errJoin, fmt.Errorf("%w: %w", e, err))
				} else if resp.Error != nil {
					errJoin = errors.Join(errJoin, fmt.Errorf("%w: %w", e, errors.New(resp.Error.GetMessage())))
				}
			}
			if errJoin == nil {
				variables := map[string]any{}
				err = json.Unmarshal(resp.Job.GetVariables(), &variables)
				if err != nil {
					errJoin = errors.Join(errJoin, fmt.Errorf("failed to deserialize job variables: %w", err))
				}
				jobsChan <- resp.Job
			}
			errChan <- errJoin
		}()
	}
	wg.Wait()
	close(errChan)
	close(jobsChan)
	for err := range errChan {
		if err != nil {
			errJoin = errors.Join(errJoin, err)
		}
	}
	jobs := make([]*proto.InternalJob, 0, len(state.Partitions))
	for job := range jobsChan {
		jobs = append(jobs, job)
	}
	if errJoin != nil {
		return nil, errJoin
	}
	return jobs, nil
}

func (node *ZenNode) PublishMessage(ctx context.Context, name string, instanceKey int64, variables map[string]any) error {
	partition := zenflake.GetPartitionId(instanceKey)
	client, err := node.client.PartitionLeader(partition)
	if err != nil {
		return fmt.Errorf("failed to get client: %w", err)
	}
	vars, err := json.Marshal(variables)
	if err != nil {
		return fmt.Errorf("failed marshal variables: %w", err)
	}
	resp, err := client.PublishMessage(ctx, &proto.PublishMessageRequest{
		Name:        name,
		InstanceKey: instanceKey,
		Variables:   vars,
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
func (node *ZenNode) GetProcessDefinitions(ctx context.Context) ([]proto.ProcessDefinition, error) {
	db, err := node.GetReadOnlyDB(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get process definitions: %w", err)
	}
	definitions, err := db.queries.FindAllProcessDefinitions(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read process definitions from database: %w", err)
	}
	resp := make([]proto.ProcessDefinition, 0, len(definitions))
	for _, def := range definitions {
		resp = append(resp, proto.ProcessDefinition{
			Key:        def.Key,
			Version:    def.Version,
			ProcessId:  def.BpmnProcessID,
			Definition: []byte(def.BpmnData),
		})
	}
	return resp, nil
}

// GetLatestProcessDefinition does not have to go through the grpc as all partitions should have the same definitions so it can just read it from any of its partitions
func (node *ZenNode) GetLatestProcessDefinition(ctx context.Context, processId string) (proto.ProcessDefinition, error) {
	db, err := node.GetReadOnlyDB(ctx)
	if err != nil {
		return proto.ProcessDefinition{}, fmt.Errorf("failed to get process definition: %w", err)
	}
	def, err := db.queries.FindLatestProcessDefinitionById(ctx, processId)
	if err != nil {
		return proto.ProcessDefinition{}, fmt.Errorf("failed to read process definition from database: %w", err)
	}
	return proto.ProcessDefinition{
		Key:        def.Key,
		Version:    def.Version,
		ProcessId:  def.BpmnProcessID,
		Definition: []byte(def.BpmnData),
	}, nil
}

// GetProcessDefinition does not have to go through the grpc as all partitions should have the same definitions so it can just read it from any of its partitions
func (node *ZenNode) GetProcessDefinition(ctx context.Context, key int64) (proto.ProcessDefinition, error) {
	db, err := node.GetReadOnlyDB(ctx)
	if err != nil {
		return proto.ProcessDefinition{}, fmt.Errorf("failed to get process definition: %w", err)
	}
	def, err := db.queries.FindProcessDefinitionByKey(ctx, key)
	if err != nil {
		return proto.ProcessDefinition{}, fmt.Errorf("failed to read process definition from database: %w", err)
	}
	return proto.ProcessDefinition{
		Key:        def.Key,
		Version:    def.Version,
		ProcessId:  def.BpmnProcessID,
		Definition: []byte(def.BpmnData),
	}, nil
}

func (node *ZenNode) CreateInstance(ctx context.Context, processDefinitionKey int64, variables map[string]any) (*proto.ProcessInstance, error) {
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
	resp, err := client.CreateInstance(ctx, &proto.CreateInstanceRequest{
		StartBy: &proto.CreateInstanceRequest_DefinitionKey{
			DefinitionKey: processDefinitionKey,
		},
		Variables: vars,
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

// GetProcessInstances will contact follower nodes and return instances in partitions they are following
func (node *ZenNode) GetProcessInstances(ctx context.Context, processDefinitionKey int64, page int32, size int32) ([]*proto.PartitionedProcessInstances, error) {
	state := node.store.ClusterState()
	result := make([]*proto.PartitionedProcessInstances, 0, len(state.Partitions))

	for partitionId := range state.Partitions {
		// TODO: we can smack these into goroutines
		follower, err := state.GetPartitionFollower(partitionId)
		if err != nil {
			return result, fmt.Errorf("failed to follower node to get process instances: %w", err)
		}
		client, err := node.client.For(follower.Addr)
		if err != nil {
			return result, fmt.Errorf("failed to get client to get process instances: %w", err)
		}
		resp, err := client.GetProcessInstances(ctx, &proto.GetProcessInstancesRequest{
			Page:          page,
			Size:          size,
			Partitions:    []uint32{partitionId},
			DefinitionKey: processDefinitionKey,
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
func (node *ZenNode) GetProcessInstance(ctx context.Context, processInstanceKey int64) (*proto.ProcessInstance, error) {
	state := node.store.ClusterState()
	partitionId := zenflake.GetPartitionId(processInstanceKey)
	follower, err := state.GetPartitionFollower(partitionId)
	if err != nil {
		return nil, fmt.Errorf("failed to follower node to get process instance: %w", err)
	}
	client, err := node.client.For(follower.Addr)
	if err != nil {
		return nil, fmt.Errorf("failed to get client to get process instance: %w", err)
	}
	resp, err := client.GetProcessInstance(ctx, &proto.GetProcessInstanceRequest{
		ProcessInstanceKey: processInstanceKey,
	})
	if err != nil || resp.Error != nil {
		e := fmt.Errorf("failed to get process instance from partition %d", partitionId)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", e, err)
		} else if resp.Error != nil {
			return nil, fmt.Errorf("%w: %w", e, errors.New(resp.Error.GetMessage()))
		}
	}

	return resp.Processes, nil
}

// GetProcessInstance will contact follower node of partition that contains process instance
func (node *ZenNode) GetProcessInstanceJobs(ctx context.Context, processInstanceKey int64) ([]*proto.Job, error) {
	state := node.store.ClusterState()
	partitionId := zenflake.GetPartitionId(processInstanceKey)
	follower, err := state.GetPartitionFollower(partitionId)
	if err != nil {
		return nil, fmt.Errorf("failed to follower node to get process instance: %w", err)
	}
	client, err := node.client.For(follower.Addr)
	if err != nil {
		return nil, fmt.Errorf("failed to get client to get process instance: %w", err)
	}
	resp, err := client.GetProcessInstanceJobs(ctx, &proto.GetProcessInstanceJobsRequest{
		ProcessInstanceKey: processInstanceKey,
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

func (node *ZenNode) GetStatus() store.ClusterState {
	if node.store == nil {
		return store.ClusterState{}
	}
	return node.store.ClusterState()
}
