package cluster

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"hash/fnv"
	"net"
	"slices"
	"time"

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
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"github.com/rqlite/rqlite/v8/cluster"
	"github.com/rqlite/rqlite/v8/tcp"
	"golang.org/x/sync/errgroup"
	gproto "google.golang.org/protobuf/proto"
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
	idGen      *snowflake.Node
	// TODO: add tracing to all the methods on ZenNode where it makes sense
}

// StartZenNode Starts a cluster node
func StartZenNode(mainCtx context.Context, conf config.Config) (*ZenNode, error) {
	idGen, err := snowflake.NewNode(0)
	if err != nil {
		return nil, fmt.Errorf("failed to create snowflake id generator: %w", err)
	}
	node := &ZenNode{
		logger: hclog.Default().Named(fmt.Sprintf("zen-node-%s", conf.Cluster.NodeId)),
		ctx:    mainCtx,
		idGen:  idGen,
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
func (node *ZenNode) GetDmnResourceDefinitions(ctx context.Context, request *proto.GetDmnResourceDefinitionsRequest) (proto.DmnResourceDefinitionsPage, error) {
	// TODO some methods like GetDmnResourceDefinitions and GetProcessInstances use GRPC request objects in their arguments
	// in order not to have up to 8 and more arguments line in getJobs. Unify the approach
	db, err := node.GetReadOnlyDB(ctx)
	if err != nil {
		return proto.DmnResourceDefinitionsPage{}, zenerr.TechnicalError(fmt.Errorf("failed to get node DB: %w", err))
	}
	page := *request.Page
	size := *request.Size
	latest := 0
	if request.OnlyLatest != nil && *request.OnlyLatest {
		latest = 1
	}
	definitions, err := db.Queries.FindAllDmnResourceDefinitions(ctx, sql.FindAllDmnResourceDefinitionsParams{
		OnlyLatest:              int64(latest),
		SortByOrder:             sql.ToNullString(request.SortByOrder),
		DmnResourceDefinitionID: sql.ToNullString(request.DmnResourceDefinitionId),
		DmnDefinitionName:       sql.ToNullString(request.DmnDefinitionName),
		Offset:                  int64((page - 1) * size),
		Size:                    int64(size),
	})
	if err != nil {
		return proto.DmnResourceDefinitionsPage{}, zenerr.TechnicalError(fmt.Errorf("failed to read dmn resource definitions from database: %w", err))
	}
	items := make([]*proto.DmnResourceDefinition, 0, len(definitions))
	totalCount := 0
	for i, def := range definitions {
		if i == 0 {
			totalCount = int(def.TotalCount)
		}
		items = append(items, &proto.DmnResourceDefinition{
			Key:                     &def.Key,
			Version:                 ptr.To(int32(def.Version)),
			DmnResourceDefinitionId: &def.DmnResourceDefinitionID,
			Definition:              []byte(def.DmnData),
			DmnDefinitionName:       &def.DmnDefinitionName,
		})
	}
	return proto.DmnResourceDefinitionsPage{
		Items:      items,
		TotalCount: ptr.To(int32(totalCount)),
	}, nil
}

// GetDmnResourceDefinition does not have to go through the grpc as all partitions should have the same definitions so it can just read it from any of its partitions
func (node *ZenNode) GetDmnResourceDefinition(ctx context.Context, key int64) (proto.DmnResourceDefinition, error) {
	db, err := node.GetReadOnlyDB(ctx)
	if err != nil {
		return proto.DmnResourceDefinition{}, zenerr.TechnicalError(fmt.Errorf("failed to get node DB: %w", err))
	}
	def, err := db.Queries.FindDmnResourceDefinitionByKey(ctx, key)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return proto.DmnResourceDefinition{}, zenerr.NotFound(fmt.Errorf("dmn resource definition by key %d not found", key))
		}
		return proto.DmnResourceDefinition{}, zenerr.TechnicalError(fmt.Errorf("failed to read dmn resource definition by key: %d from database: %w", key, err))
	}
	return proto.DmnResourceDefinition{
		Key:                     &def.Key,
		Version:                 ptr.To(int32(def.Version)),
		DmnResourceDefinitionId: &def.DmnResourceDefinitionID,
		Definition:              []byte(def.DmnData),
		DmnDefinitionName:       &def.DmnDefinitionName,
	}, nil
}

func (node *ZenNode) DeployDmnResourceDefinitionToAllPartitions(ctx context.Context, data []byte) (int64, error) {
	key, err := node.getDmnResourceDefinitionKeyByBytes(ctx, data)
	if err != nil {
		return key, zenerr.TechnicalError(fmt.Errorf("failed to get dmn resource definition key by bytes: %w", err))
	}
	if key != 0 {
		return key, zenerr.Conflict(fmt.Errorf("duplicate dmn resource definition %d exists", key))
	}
	definitionKey := node.idGen.Generate()
	clusterState := node.store.ClusterState()
	group, groupCtx := errgroup.WithContext(ctx)

	for _, currentPartition := range clusterState.Partitions {

		group.Go(func() error {
			return node.deployDmnResourceDefinitionToPartition(
				groupCtx,
				clusterState,
				currentPartition.LeaderId,
				definitionKey.Int64(),
				data,
			)
		})
	}

	if waitErr := group.Wait(); waitErr != nil {
		return definitionKey.Int64(), waitErr
	}

	return definitionKey.Int64(), nil
}

func (node *ZenNode) deployDmnResourceDefinitionToPartition(
	ctx context.Context,
	clusterState state.Cluster,
	partitionLeaderId string,
	definitionKey int64,
	data []byte,
) error {
	partitionLeader := clusterState.Nodes[partitionLeaderId]
	zenNodeClient, err := node.client.For(partitionLeader.Addr)
	if err != nil {
		return zenerr.TechnicalError(fmt.Errorf("failed to get client: %w", err))
	}

	resp, err := zenNodeClient.DeployDmnResourceDefinition(ctx, &proto.DeployDmnResourceDefinitionRequest{
		Key:  ptr.To(definitionKey),
		Data: data,
	})

	if err != nil {
		return zenerr.TechnicalError(fmt.Errorf("client call to deploy dmn resource definition failed: %w", err))
	}

	if resp == nil {
		return zenerr.TechnicalError(fmt.Errorf("client call to deploy dmn resource definition failed: response is nil"))
	}

	if resp.Error != nil {
		return zenerr.ToZenError(resp.Error, fmt.Errorf("client call to deploy dmn resource definition failed: %w", errors.New(resp.Error.GetMessage())))
	}

	return nil
}

func (node *ZenNode) getDmnResourceDefinitionKeyByBytes(ctx context.Context, data []byte) (int64, error) {
	db, err := node.GetReadOnlyDB(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get database for dmn resource definition key lookup: %w", err)
	}
	md5sum := md5.Sum(data)
	key, err := db.Queries.GetDmnResourceDefinitionKeyByChecksum(ctx, md5sum[:])
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("failed to find dmn resource definition by checksum: %w", err)
	}
	return key, nil
}

func (node *ZenNode) EvaluateDecision(ctx context.Context, bindingType string, decisionId string, versionTag string, variables map[string]any) (*proto.EvaluatedDRDResult, error) {
	candidateNode, err := node.GetStatus().GetLeastStressedPartitionLeader()
	if err != nil {
		return nil, zenerr.ClusterError(fmt.Errorf("failed to get node to evaluate decision: %w", err))
	}
	client, err := node.client.For(candidateNode.Addr)
	if err != nil {
		return nil, zenerr.ClusterError(fmt.Errorf("failed to get client to evaluate decision: %w", err))
	}
	vars, err := json.Marshal(variables)
	if err != nil {
		return nil, zenerr.TechnicalError(fmt.Errorf("failed to marshal variables to evaluate decision: %w", err))
	}
	resp, err := client.EvaluateDecision(ctx, &proto.EvaluateDecisionRequest{
		BindingType: &bindingType,
		DecisionId:  &decisionId,
		VersionTag:  &versionTag,
		Variables:   vars,
	})
	if err != nil {
		return nil, zenerr.ClusterError(fmt.Errorf("failed to evaluate decision %s: %w", decisionId, err))
	}
	if resp.Error != nil {
		return nil, zenerr.ToZenError(resp.Error, fmt.Errorf("failed to evaluate decision %s", decisionId))
	}
	return resp, nil
}

func (node *ZenNode) DeployProcessDefinitionToAllPartitions(ctx context.Context, data []byte, resourceName string) (int64, error) {
	key, err := node.GetDefinitionKeyByBytes(ctx, data)
	if err != nil {
		return key, zenerr.TechnicalError(fmt.Errorf("failed to get process definition key by bytes: %w", err))
	}
	if key != 0 {
		return key, zenerr.Conflict(fmt.Errorf("duplicate process definition %d exists", key))
	}
	definitionKey := node.idGen.Generate()
	clusterState := node.store.ClusterState()
	partitionIds := sortedPartitionIds(clusterState)
	group, groupCtx := errgroup.WithContext(ctx)

	// determine partitionIdx using incoming process definition id
	var definitions bpmn20.TDefinitions
	err = xml.Unmarshal(data, &definitions)
	if err != nil {
		return key, fmt.Errorf("failed to unmarshal xml data: %w", err)
	}
	h := fnv.New32a()
	_, err = h.Write([]byte(definitions.Process.Id))
	if err != nil {
		return key, fmt.Errorf("failed to hash process definition id: %w", err)
	}
	if len(partitionIds) == 0 {
		return key, fmt.Errorf("no partitions available in cluster state")
	}
	partitionIdx := int(h.Sum32() % uint32(len(partitionIds)))

	// use that partitionIdx to create potential process timer start events always only on that one partitionIdx
	timerStartEventPartitionId := partitionIds[partitionIdx]
	for _, partitionId := range partitionIds {
		leaderId := clusterState.Partitions[partitionId].LeaderId
		registerForPotentialTimerStartEvents := timerStartEventPartitionId == partitionId

		group.Go(func() error {
			return node.deployProcessDefinitionToPartition(
				groupCtx,
				clusterState,
				leaderId,
				definitionKey.Int64(),
				data,
				resourceName,
				registerForPotentialTimerStartEvents,
			)
		})
	}

	if waitErr := group.Wait(); waitErr != nil {
		return definitionKey.Int64(), waitErr
	}
	return definitionKey.Int64(), nil
}

func (node *ZenNode) deployProcessDefinitionToPartition(
	ctx context.Context,
	clusterState state.Cluster,
	partitionLeaderId string,
	definitionKey int64,
	data []byte,
	resourceName string,
	registerForPotentialTimerStartEvents bool,
) error {

	partitionLeader := clusterState.Nodes[partitionLeaderId]
	zenNodeClient, err := node.client.For(partitionLeader.Addr)
	if err != nil {
		return zenerr.TechnicalError(fmt.Errorf("failed to get client: %w", err))
	}

	resp, err := zenNodeClient.DeployProcessDefinition(ctx, &proto.DeployProcessDefinitionRequest{
		Key:                                  ptr.To(definitionKey),
		Data:                                 data,
		ResourceName:                         &resourceName,
		RegisterForPotentialTimerStartEvents: &registerForPotentialTimerStartEvents,
	})
	if err != nil {
		return zenerr.TechnicalError(fmt.Errorf("client call to deploy process definition failed: %w", err))
	}

	if resp == nil {
		return zenerr.TechnicalError(fmt.Errorf("failed to get response from partition, response is nil %s", partitionLeaderId))
	}

	if resp.Error != nil {
		return zenerr.ToZenError(resp.Error, fmt.Errorf("client call to deploy process definition %s failed", resourceName))
	}

	return nil
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
		return zenerr.ClusterError(fmt.Errorf("failed to get client: %w", err))
	}
	vars, err := json.Marshal(variables)
	if err != nil {
		return zenerr.BadRequest(fmt.Errorf("failed marshal variables: %w", err))
	}
	resp, err := client.CompleteJob(ctx, &proto.CompleteJobRequest{
		Key:       &key,
		Variables: vars,
	})
	if err != nil {
		e := fmt.Errorf("client call to complete job %d failed: %w", key, err)
		return zenerr.TechnicalError(e)
	}
	if resp.Error != nil {
		e := fmt.Errorf("client call to complete job %d failed", key)
		return zenerr.ToZenError(resp.Error, e)
	}
	return nil
}

func (node *ZenNode) AssignJob(ctx context.Context, key int64, assignee string) *zenerr.ZenError {
	partition := zenflake.GetPartitionId(key)
	client, err := node.client.PartitionLeader(partition)
	if err != nil {
		return zenerr.ClusterError(fmt.Errorf("failed to get client: %w", err))
	}
	resp, err := client.AssignJobToAssignee(ctx, &proto.AssignJobToAssigneeRequest{
		Key:      &key,
		Assignee: &assignee,
	})
	if err != nil {
		return zenerr.TechnicalError(fmt.Errorf("client call to assign job failed: %w", err))
	}
	if resp.Error != nil {
		return zenerr.ToZenError(resp.Error, fmt.Errorf("client call to assign job failed"))
	}
	return nil
}

func (node *ZenNode) FailJob(ctx context.Context, key int64, errorCode string, variables map[string]any) error {
	partition := zenflake.GetPartitionId(key)
	client, err := node.client.PartitionLeader(partition)

	if err != nil {
		return zenerr.ClusterError(fmt.Errorf("failed to get client: %w", err))
	}
	vars, err := json.Marshal(variables)

	if err != nil {
		return zenerr.BadRequest(fmt.Errorf("failed marshal variables: %w", err))
	}
	resp, err := client.FailJob(ctx, &proto.FailJobRequest{
		Key:       &key,
		ErrorCode: &errorCode,
		Variables: vars,
	})

	if err != nil {
		return zenerr.TechnicalError(fmt.Errorf("client call to fail job failed: %w", err))
	}
	if resp.Error != nil {
		return zenerr.ToZenError(resp.Error, fmt.Errorf("client call to fail job failed"))
	}

	return nil
}

func (node *ZenNode) ResolveIncident(ctx context.Context, key int64) error {
	partition := zenflake.GetPartitionId(key)
	client, err := node.client.PartitionLeader(partition)
	if err != nil {
		return zenerr.ClusterError(fmt.Errorf("failed to get client: %w", err))
	}
	resp, err := client.ResolveIncident(ctx, &proto.ResolveIncidentRequest{
		IncidentKey: &key,
	})
	if err != nil {
		return zenerr.TechnicalError(fmt.Errorf("client call to resolve incident failed: %w", err))
	}
	if resp.Error != nil {
		return zenerr.ToZenError(resp.Error, fmt.Errorf("client call to resolve incident failed"))
	}
	return nil
}

func (node *ZenNode) PublishMessage(ctx context.Context, name string, correlationKey string, variables map[string]any) error {
	partitionId := node.store.ClusterState().GetPartitionIdFromString(correlationKey)
	partitionNode := node.controller.GetPartition(ctx, partitionId)
	if partitionNode == nil {
		return zenerr.TechnicalError(fmt.Errorf("partition %d not found for correlation key %s", partitionId, correlationKey))
	}
	msPointer, err := partitionNode.DB.FindActiveMessageSubscriptionPointer(
		ctx,
		name,
		correlationKey,
	)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return zenerr.NotFound(fmt.Errorf("no active message subscription found for name %q and correlationKey %q", name, correlationKey))
		}
		return zenerr.TechnicalError(fmt.Errorf("failed to find message subscription for name %q and correlationKey %q: %w", name, correlationKey, err))
	}

	partitionId = zenflake.GetPartitionId(msPointer.MessageSubscriptionKey)
	client, err := node.client.PartitionLeader(partitionId)
	if err != nil {
		return zenerr.ClusterError(fmt.Errorf("failed to get partition leader: %w", err))
	}

	vars, err := json.Marshal(variables)
	if err != nil {
		return zenerr.TechnicalError(fmt.Errorf("failed to marshal variables: %w", err))
	}

	resp, err := client.PublishMessage(ctx, &proto.PublishMessageRequest{
		Key:       &msPointer.MessageSubscriptionKey,
		Variables: vars,
	})
	if err != nil {
		return zenerr.ClusterError(fmt.Errorf("client call to publish message failed: %w", err))
	}
	if resp.Error != nil {
		return zenerr.ToZenError(resp.Error, fmt.Errorf("client call to publish message failed"))
	}

	return nil
}

// GetProcessDefinitions does not have to go through the grpc as all partitions should have the same definitions so it can just read it from any of its partitions
func (node *ZenNode) GetProcessDefinitions(ctx context.Context, bpmnProcessId *string, onlyLatest *bool, sort *sql.Sort, page int32, size int32) (proto.ProcessDefinitionsPage, error) {
	// Get storage for the selected partition
	db, err := node.GetReadOnlyDB(ctx)
	if err != nil {
		return proto.ProcessDefinitionsPage{}, zenerr.TechnicalError(fmt.Errorf("failed to get partition store: %w", err))
	}

	latest := 0
	if onlyLatest != nil && *onlyLatest {
		latest = 1
	}

	dbDefinitions, err := db.Queries.FindProcessDefinitions(ctx, sql.FindProcessDefinitionsParams{
		BpmnProcessIDFilter: sql.ToNullString(bpmnProcessId),
		Sort:                sql.ToNullString(sort),
		OnlyLatest:          int64(latest),
		Offset:              int64((page - 1) * size),
		Limit:               int64(size),
	})
	if err != nil {
		return proto.ProcessDefinitionsPage{}, zenerr.TechnicalError(fmt.Errorf("failed to find process definitions: %w", err))
	}

	resp := make([]*proto.ProcessDefinition, 0, len(dbDefinitions))
	totalCount := 0
	for i, def := range dbDefinitions {
		if i == 0 {
			totalCount = int(def.TotalCount)
		}
		resp = append(resp, &proto.ProcessDefinition{
			Key:         &def.Key,
			Version:     ptr.To(int32(def.Version)),
			ProcessId:   &def.BpmnProcessID,
			ProcessName: &def.BpmnProcessName,
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
		return proto.ProcessDefinition{}, zenerr.TechnicalError(fmt.Errorf("failed to get node db to get process definition: %w", err))
	}
	def, err := db.Queries.FindLatestProcessDefinitionById(ctx, processId)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return proto.ProcessDefinition{}, zenerr.NotFound(fmt.Errorf("process definition with id %s not found", processId))
		}
		return proto.ProcessDefinition{}, zenerr.TechnicalError(fmt.Errorf("failed to read process definition from database: %w", err))
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
		return proto.ProcessDefinition{}, zenerr.TechnicalError(fmt.Errorf("failed to get node db to get process definition: %w", err))
	}
	def, err := db.Queries.FindProcessDefinitionByKey(ctx, key)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return proto.ProcessDefinition{}, zenerr.NotFound(fmt.Errorf("process definition with key %d not found", key))
		}
		return proto.ProcessDefinition{}, zenerr.TechnicalError(fmt.Errorf("failed to read process definition from database: %w", err))
	}
	return proto.ProcessDefinition{
		Key:        &def.Key,
		Version:    ptr.To(int32(def.Version)),
		ProcessId:  &def.BpmnProcessID,
		Definition: []byte(def.BpmnData),
	}, nil
}

// GetProcessDefinitionElementStatistics will contact follower nodes and return process definition element statistics from all partitions
func (node *ZenNode) GetProcessDefinitionElementStatistics(ctx context.Context, processDefinitionKey int64) ([]*proto.PartitionedElementStatistics, error) {
	clusterState := node.store.ClusterState()
	partitionIds := sortedPartitionIds(clusterState)

	partitionResults := make([][]*proto.PartitionedElementStatistics, len(partitionIds))
	group, groupCtx := errgroup.WithContext(ctx)

	for idx, partitionId := range partitionIds {

		group.Go(func() error {
			partitions, err := node.getProcessDefinitionElementStatisticsForPartition(
				groupCtx,
				clusterState,
				processDefinitionKey,
				partitionId,
			)
			if err != nil {
				return err
			}

			partitionResults[idx] = partitions
			return nil
		})
	}

	if errWait := group.Wait(); errWait != nil {
		return nil, errWait
	}

	return slices.Concat(partitionResults...), nil
}

// GetProcessInstanceElementStatistics will contact follower nodes and return process instance element statistics from all partitions
func (node *ZenNode) GetProcessInstanceElementStatistics(ctx context.Context, processInstanceKey int64) ([]*proto.PartitionedElementStatistics, error) {
	clusterState := node.store.ClusterState()
	partitionIds := sortedPartitionIds(clusterState)

	partitionResults := make([][]*proto.PartitionedElementStatistics, len(partitionIds))
	group, groupCtx := errgroup.WithContext(ctx)

	for idx, partitionId := range partitionIds {

		group.Go(func() error {
			partitions, err := node.getProcessInstanceElementStatisticsForPartition(
				groupCtx,
				clusterState,
				processInstanceKey,
				partitionId,
			)
			if err != nil {
				return err
			}

			partitionResults[idx] = partitions
			return nil
		})
	}

	if errWait := group.Wait(); errWait != nil {
		return nil, errWait
	}

	return slices.Concat(partitionResults...), nil
}

func (node *ZenNode) getProcessDefinitionElementStatisticsForPartition(
	ctx context.Context,
	clusterState state.Cluster,
	processDefinitionKey int64,
	partitionId uint32,
) ([]*proto.PartitionedElementStatistics, error) {
	follower, err := clusterState.GetPartitionFollower(partitionId)
	if err != nil {
		return nil, zenerr.ClusterError(fmt.Errorf("failed to read follower node to get element statistics from partition %d: %w", partitionId, err))
	}

	zenNodeClient, err := node.client.For(follower.Addr)
	if err != nil {
		return nil, zenerr.TechnicalError(fmt.Errorf("failed to get client to get element statistics from partition %d: %w", partitionId, err))
	}

	resp, err := zenNodeClient.GetProcessDefinitionElementStatistics(ctx, &proto.GetProcessDefinitionElementStatisticsRequest{
		ProcessDefinitionKey: &processDefinitionKey,
		Partitions:           []uint32{partitionId},
	})

	if err != nil {
		return nil, zenerr.TechnicalError(fmt.Errorf("%w: %w", fmt.Errorf("failed to get element statistics from partition %d", partitionId), err))
	}

	if resp == nil {
		return nil, zenerr.TechnicalError(fmt.Errorf("failed to get response from partition, response is nil %d", partitionId))
	}

	if resp.Error != nil {
		return nil, zenerr.ToZenError(resp.Error, fmt.Errorf("failed to get element statistics from partition %d", partitionId))
	}

	return resp.Partitions, nil
}

func (node *ZenNode) getProcessInstanceElementStatisticsForPartition(
	ctx context.Context,
	clusterState state.Cluster,
	processInstanceKey int64,
	partitionId uint32,
) ([]*proto.PartitionedElementStatistics, error) {
	follower, err := clusterState.GetPartitionFollower(partitionId)
	if err != nil {
		return nil, zenerr.ClusterError(fmt.Errorf("failed to read follower node to get element statistics from partition %d: %w", partitionId, err))
	}

	zenNodeClient, err := node.client.For(follower.Addr)
	if err != nil {
		return nil, zenerr.TechnicalError(fmt.Errorf("failed to get client to get element statistics from partition %d: %w", partitionId, err))
	}

	resp, err := zenNodeClient.GetProcessInstanceElementStatistics(ctx, &proto.GetProcessInstanceElementStatisticsRequest{
		ProcessInstanceKey: &processInstanceKey,
		Partitions:         []uint32{partitionId},
	})

	if err != nil {
		return nil, zenerr.TechnicalError(fmt.Errorf("failed to get element statistics from partition %d: %w", partitionId, err))
	}

	if resp == nil {
		return nil, zenerr.TechnicalError(fmt.Errorf("failed to get response from partition, response is nil %d", partitionId))
	}

	if resp.Error != nil {
		return nil, zenerr.ToZenError(resp.Error, fmt.Errorf("failed to get element statistics from partition %d", partitionId))
	}

	return resp.Partitions, nil
}

// GetProcessDefinitionStatistics will contact follower nodes and return process definition statistics from all partitions
func (node *ZenNode) GetProcessDefinitionStatistics(
	ctx context.Context,
	page int32,
	size int32,
	onlyLatest bool,
	bpmnProcessIdIn []string,
	bpmnProcessDefinitionKeyIn []int64,
	name *string,
	sort *sql.Sort,
) ([]*proto.PartitionedProcessDefinitionStatistics, error) {
	clusterState := node.store.ClusterState()
	partitionIds := sortedPartitionIds(clusterState)

	partitionResults := make([][]*proto.PartitionedProcessDefinitionStatistics, len(partitionIds))
	group, groupCtx := errgroup.WithContext(ctx)

	for idx, partitionId := range partitionIds {

		group.Go(func() error {
			partitions, err := node.getProcessDefinitionStatisticsForPartition(
				groupCtx,
				clusterState,
				page,
				size,
				onlyLatest,
				bpmnProcessIdIn,
				bpmnProcessDefinitionKeyIn,
				name,
				sort,
				partitionId,
			)
			if err != nil {
				return err
			}

			partitionResults[idx] = partitions
			return nil
		})
	}

	if errWait := group.Wait(); errWait != nil {
		return nil, errWait
	}

	return slices.Concat(partitionResults...), nil
}

func (node *ZenNode) getProcessDefinitionStatisticsForPartition(
	ctx context.Context,
	clusterState state.Cluster,
	page int32,
	size int32,
	onlyLatest bool,
	bpmnProcessIdIn []string,
	bpmnProcessDefinitionKeyIn []int64,
	name *string,
	sort *sql.Sort,
	partitionId uint32,
) ([]*proto.PartitionedProcessDefinitionStatistics, error) {
	follower, err := clusterState.GetPartitionFollower(partitionId)
	if err != nil {
		return nil, zenerr.ClusterError(
			fmt.Errorf("failed to read follower node to get process definition statistics: %w", err),
		)
	}

	zenNodeClient, err := node.client.For(follower.Addr)
	if err != nil {
		return nil, zenerr.TechnicalError(
			fmt.Errorf("failed to get client to get process definition statistics: %w", err),
		)
	}

	resp, err := zenNodeClient.GetProcessDefinitionStatistics(ctx, &proto.GetProcessDefinitionStatisticsRequest{
		Page:                       &page,
		Size:                       &size,
		Partitions:                 []uint32{partitionId},
		OnlyLatest:                 &onlyLatest,
		BpmnProcessIdIn:            bpmnProcessIdIn,
		BpmnProcessDefinitionKeyIn: bpmnProcessDefinitionKeyIn,
		Name:                       name,
		Sort:                       (*string)(sort),
	})

	if err != nil {
		return nil, zenerr.TechnicalError(fmt.Errorf("failed to get definition statistics from partition %d: %w", partitionId, err))
	}

	if resp == nil {
		return nil, zenerr.TechnicalError(fmt.Errorf("failed to get response from partition, response is nil %d", partitionId))
	}

	if resp.Error != nil {
		return nil, zenerr.ToZenError(resp.Error, fmt.Errorf("failed to get definition statistics from partition %d", partitionId))
	}

	return resp.Partitions, nil
}

func (node *ZenNode) StartPprofServer(ctx context.Context, nodeId string) error {
	state := node.store.ClusterState()
	targetNode, err2 := state.GetNode(nodeId)
	if err2 != nil {
		return err2
	}
	client, err := node.client.For(targetNode.Addr)
	if err != nil {
		return err
	}

	resp, err := client.StartPprofServer(ctx, &proto.PprofServerRequest{})
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

func (node *ZenNode) StopPprofServer(ctx context.Context, nodeId string) error {
	state := node.store.ClusterState()
	targetNode, err2 := state.GetNode(nodeId)
	if err2 != nil {
		return err2
	}
	client, err := node.client.For(targetNode.Addr)
	if err != nil {
		return err
	}

	resp, err := client.StopPprofServer(ctx, &proto.PprofServerRequest{})
	if err != nil || resp.Error != nil {
		e := fmt.Errorf("failed to stop pprof")
		if err != nil {
			return fmt.Errorf("%w: %w", e, err)
		} else if resp.Error != nil {
			return fmt.Errorf("%w: %w", e, errors.New(resp.Error.GetMessage()))
		}
	}

	return nil
}

func (node *ZenNode) CreateInstance(
	ctx context.Context,
	processDefinitionKey *int64,
	bpmnProcessId *string,
	businessKey *string,
	variables map[string]any,
	timeToLive *types.TTL,
) (*proto.ProcessInstance, error) {
	state := node.store.ClusterState()
	candidateNode, err := state.GetLeastStressedPartitionLeader()
	if err != nil {
		return nil, zenerr.ClusterError(fmt.Errorf("failed to get node to create process instance: %w", err))
	}
	client, err := node.client.For(candidateNode.Addr)
	if err != nil {
		return nil, zenerr.TechnicalError(fmt.Errorf("failed to get client to create process instance: %w", err))
	}
	vars, err := json.Marshal(variables)
	if err != nil {
		return nil, zenerr.BadRequest(fmt.Errorf("failed to marshal variables to create process instance: %w", err))
	}
	if timeToLive == nil {
		if node.controller.Config.Persistence.InstanceHistoryTTL != 0 {
			timeToLive = &node.controller.Config.Persistence.InstanceHistoryTTL
		}
	}

	var resp *proto.CreateInstanceResponse
	switch {
	case processDefinitionKey != nil:
		resp, err = client.CreateInstance(ctx, &proto.CreateInstanceRequest{
			StartBy: &proto.CreateInstanceRequest_DefinitionKey{
				DefinitionKey: *processDefinitionKey,
			},
			Variables:   vars,
			HistoryTTL:  (*int64)(timeToLive),
			BusinessKey: businessKey,
		})
	case bpmnProcessId != nil:
		resp, err = client.CreateInstance(ctx, &proto.CreateInstanceRequest{
			StartBy: &proto.CreateInstanceRequest_LatestProcessId{
				LatestProcessId: *bpmnProcessId,
			},
			Variables:   vars,
			HistoryTTL:  (*int64)(timeToLive),
			BusinessKey: businessKey,
		})
	default:
		return nil, zenerr.TechnicalError(fmt.Errorf("failed to create process instance: processDefinitionKey and bpmnProcessId are both null"))
	}
	if err != nil {
		return nil, zenerr.TechnicalError(fmt.Errorf("failed to create process instance %w", err))
	}
	if resp.Error != nil {
		return nil, zenerr.ToZenError(resp.Error, fmt.Errorf("failed to create process instance"))
	}
	return resp.Process, nil
}

func (node *ZenNode) UpdateProcessInstanceVariables(ctx context.Context, processInstanceKey int64, variables map[string]any) error {
	_, _, err := node.ModifyProcessInstance(ctx, processInstanceKey, []int64{}, []string{}, variables)
	return err
}

func (node *ZenNode) DeleteProcessInstanceVariable(ctx context.Context, processInstanceKey int64, variable string) error {
	partition := zenflake.GetPartitionId(processInstanceKey)
	client, err := node.client.PartitionLeader(partition)
	if err != nil {
		return zenerr.ClusterError(fmt.Errorf("failed to get client to delete process instance variable: %w", err))
	}

	resp, err := client.DeleteProcessInstanceVariable(ctx, &proto.DeleteProcessInstanceVariableRequest{
		ProcessInstanceKey: &processInstanceKey,
		Variable:           &variable,
	})
	if err != nil {
		return zenerr.TechnicalError(fmt.Errorf("failed to delete process instance variable %w", err))
	}
	if resp.Error != nil {
		return zenerr.ToZenError(resp.Error, fmt.Errorf("failed to delete process instance variable"))
	}
	return nil
}

func (node *ZenNode) CancelProcessInstance(ctx context.Context, processInstanceKey int64) error {
	partition := zenflake.GetPartitionId(processInstanceKey)
	client, err := node.client.PartitionLeader(partition)
	if err != nil {
		return zenerr.ClusterError(fmt.Errorf("failed to get client to cancel process instance: %d. %w", processInstanceKey, err))
	}

	resp, err := client.CancelProcessInstance(ctx, &proto.CancelProcessInstanceRequest{
		ProcessInstanceKey: &processInstanceKey,
	})
	if err != nil {
		return zenerr.TechnicalError(fmt.Errorf("failed to cancel process instance: %w", err))
	}
	if resp.Error != nil {
		return zenerr.ToZenError(resp.Error, fmt.Errorf("failed to cancel process instance: %s", resp.Error.GetMessage()))
	}
	return nil
}

func (node *ZenNode) ModifyProcessInstance(ctx context.Context, processInstanceKey int64, elementInstanceIdsToTerminate []int64, elementIdsToStartInstance []string, variables map[string]any) (*proto.ProcessInstance, []*proto.ExecutionToken, error) {
	partition := zenflake.GetPartitionId(processInstanceKey)
	client, err := node.client.PartitionLeader(partition)
	if err != nil {
		return nil, nil, zenerr.ClusterError(fmt.Errorf("failed to get client to modify process instance: %d. %w", processInstanceKey, err))
	}
	vars, err := json.Marshal(variables)
	if err != nil {
		return nil, nil, zenerr.BadRequest(fmt.Errorf("failed to marshal variables to modify process instance: %w", err))
	}

	resp, err := client.ModifyProcessInstance(ctx, &proto.ModifyProcessInstanceRequest{
		ProcessInstanceKey:            &processInstanceKey,
		ElementInstanceIdsToTerminate: elementInstanceIdsToTerminate,
		ElementIdsToStartInstance:     elementIdsToStartInstance,
		Variables:                     vars,
	})
	if err != nil {
		e := fmt.Errorf("failed to modify process instance for processInstanceKey %d %w", processInstanceKey, err)
		return nil, nil, zenerr.TechnicalError(e)
	}
	if resp.Error != nil {
		e := fmt.Errorf("failed to modify process instance for processInstanceKey %d", processInstanceKey)
		return nil, nil, zenerr.ToZenError(resp.Error, e)
	}
	return resp.Process, resp.ExecutionTokens, nil
}

// GetJobs will contact follower nodes and return jobs in partitions they are following
func (node *ZenNode) GetJobs(ctx context.Context, page int32, size int32, jobType *string, jobState *runtime.ActivityState, assignee *string, processInstanceKey *int64, sort *sql.Sort) ([]*proto.PartitionedJobs, error) {
	clusterState := node.store.ClusterState()
	partitionIds := sortedPartitionIds(clusterState)

	partitionResults := make([][]*proto.PartitionedJobs, len(partitionIds))
	group, groupCtx := errgroup.WithContext(ctx)

	for idx, partitionId := range partitionIds {

		group.Go(func() error {
			partitions, err := node.getJobsForPartition(groupCtx,
				clusterState,
				page,
				size,
				jobType,
				jobState,
				assignee,
				processInstanceKey,
				sort,
				partitionId)

			if err != nil {
				return err
			}

			partitionResults[idx] = partitions
			return nil
		})
	}

	if errWait := group.Wait(); errWait != nil {
		return nil, errWait
	}

	return slices.Concat(partitionResults...), nil
}

func (node *ZenNode) getJobsForPartition(
	ctx context.Context,
	clusterState state.Cluster,
	page int32,
	size int32,
	jobType *string,
	jobState *runtime.ActivityState,
	assignee *string,
	processInstanceKey *int64,
	sort *sql.Sort,
	partitionId uint32,
) ([]*proto.PartitionedJobs, error) {

	follower, err := clusterState.GetPartitionFollower(partitionId)
	if err != nil {
		return nil, zenerr.ClusterError(
			fmt.Errorf("failed to get follower node for partition %d: %w", partitionId, err),
		)
	}

	zenNodeClient, err := node.client.For(follower.Addr)
	if err != nil {
		return nil, zenerr.TechnicalError(
			fmt.Errorf("failed to get client to get jobs: %w", err),
		)
	}
	var reqState *int64
	if jobState != nil {
		reqState = ptr.To(int64(*jobState))
	}

	var sortString string
	if sort != nil {
		sortString = string(ptr.Deref(sort, ""))
	}
	resp, err := zenNodeClient.GetJobs(ctx, &proto.GetJobsRequest{
		Page:               &page,
		Size:               &size,
		Partitions:         []uint32{partitionId},
		JobType:            jobType,
		State:              reqState,
		Assignee:           assignee,
		ProcessInstanceKey: processInstanceKey,
		Sort:               &sortString,
	})

	if err != nil {
		return nil, zenerr.TechnicalError(fmt.Errorf("failed to get jobs from partition %d: %w", partitionId, err))
	}

	if resp == nil {
		return nil, zenerr.TechnicalError(fmt.Errorf("failed to get response from partition, response is nil %d", partitionId))
	}

	if resp.Error != nil {
		return nil, zenerr.ToZenError(resp.Error, fmt.Errorf("failed to get jobs from partition %d", partitionId))
	}

	return resp.Partitions, nil
}

func (node *ZenNode) GetJob(ctx context.Context, jobKey int64) (*proto.Job, error) {
	state := node.store.ClusterState()
	partitionId := zenflake.GetPartitionId(jobKey)
	follower, err := state.GetPartitionFollower(partitionId)
	if err != nil {
		return nil, zenerr.ClusterError(fmt.Errorf("failed to get follower node to get job: %w", err))
	}
	client, err := node.client.For(follower.Addr)
	if err != nil {
		return nil, zenerr.TechnicalError(fmt.Errorf("failed to get client to get job: %w", err))
	}
	resp, err := client.GetJob(ctx, &proto.GetJobRequest{
		JobKey: &jobKey,
	})
	if err != nil {
		e := fmt.Errorf("failed to get job from partition %d %w", partitionId, err)
		return nil, zenerr.TechnicalError(e)
	}
	if resp.Error != nil {
		e := fmt.Errorf("failed to get job from partition %d", partitionId)
		return nil, zenerr.ToZenError(resp.Error, e)
	}
	return resp.Job, nil
}

// GetProcessInstances will contact follower nodes and return instances in partitions they are following.
func (node *ZenNode) GetProcessInstances(
	ctx context.Context,
	getProcessInstancesRequest *proto.GetProcessInstancesRequest,
) ([]*proto.PartitionedProcessInstances, error) {
	clusterState := node.store.ClusterState()
	partitionIds := sortedPartitionIds(clusterState)

	partitionResults := make([][]*proto.PartitionedProcessInstances, len(partitionIds))
	group, groupCtx := errgroup.WithContext(ctx)

	for idx, partitionId := range partitionIds {

		group.Go(func() error {
			partitions, err := node.getProcessInstancesForPartition(
				groupCtx,
				clusterState,
				getProcessInstancesRequest,
				partitionId,
			)
			if err != nil {
				return err
			}

			partitionResults[idx] = partitions
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return nil, err
	}

	return slices.Concat(partitionResults...), nil
}

func (node *ZenNode) getProcessInstancesForPartition(
	ctx context.Context,
	clusterState state.Cluster,
	req *proto.GetProcessInstancesRequest,
	partitionId uint32,
) ([]*proto.PartitionedProcessInstances, error) {
	follower, err := clusterState.GetPartitionFollower(partitionId)
	if err != nil {
		return nil, zenerr.ClusterError(
			fmt.Errorf("failed to get follower node for partition %d: %w", partitionId, err),
		)
	}

	zenNodeClient, err := node.client.For(follower.Addr)
	if err != nil {
		return nil, zenerr.TechnicalError(
			fmt.Errorf("failed to get client for partition %d: %w", partitionId, err),
		)
	}

	requestClone := gproto.Clone(req).(*proto.GetProcessInstancesRequest)
	requestClone.Partitions = []uint32{partitionId}

	resp, err := zenNodeClient.GetProcessInstances(ctx, requestClone)
	if err != nil {
		return nil, zenerr.TechnicalError(
			fmt.Errorf("failed to get process instances from partition %d: %w", partitionId, err),
		)
	}

	if resp == nil {
		return nil, zenerr.TechnicalError(
			fmt.Errorf("failed to get process instances from partition %d: response is nil", partitionId),
		)
	}

	if resp.Error != nil {
		return nil, zenerr.ToZenError(
			resp.Error,
			fmt.Errorf("failed to get process instances from partition %d", partitionId),
		)
	}

	return resp.Partitions, nil
}

// GetProcessInstance will contact follower node of partition that contains process instance
func (node *ZenNode) GetProcessInstance(ctx context.Context, processInstanceKey int64) (*proto.ProcessInstance, []*proto.ExecutionToken, error) {
	state := node.store.ClusterState()
	partitionId := zenflake.GetPartitionId(processInstanceKey)
	follower, err := state.GetPartitionFollower(partitionId)
	if err != nil {
		return nil, nil, zenerr.ClusterError(fmt.Errorf("failed to get follower node to get process instance: %w", err))
	}
	client, err := node.client.For(follower.Addr)
	if err != nil {
		return nil, nil, zenerr.TechnicalError(fmt.Errorf("failed to get client to get process instance: %w", err))
	}
	resp, err := client.GetProcessInstance(ctx, &proto.GetProcessInstanceRequest{
		ProcessInstanceKey: &processInstanceKey,
	})
	if err != nil {
		e := fmt.Errorf("failed to get process instance from partition %d %w", partitionId, err)
		return nil, nil, zenerr.TechnicalError(e)
	}
	if resp.Error != nil {
		e := fmt.Errorf("failed to get process instance from partition %d", partitionId)
		return nil, nil, zenerr.ToZenError(resp.Error, e)
	}

	return resp.Processes, resp.ExecutionTokens, nil
}

// GetChildProcessInstances will contact follower nodes and return instances in partitions they are following
func (node *ZenNode) GetChildProcessInstances(
	ctx context.Context,
	request *proto.GetChildProcessInstancesRequest,
) ([]*proto.PartitionedProcessInstances, error) {
	state := node.store.ClusterState()
	result := make([]*proto.PartitionedProcessInstances, 0, len(state.Partitions))
	if request.ParentInstanceKey == nil {
		return nil, zenerr.ClusterError(fmt.Errorf("parentInstanceKey is null"))
	}
	partitionId := zenflake.GetPartitionId(*request.ParentInstanceKey)
	follower, err := state.GetPartitionFollower(partitionId)
	if err != nil {
		return nil, zenerr.ClusterError(fmt.Errorf("failed to get follower node to get child process instances: %w", err))
	}
	client, err := node.client.For(follower.Addr)
	if err != nil {
		return nil, zenerr.TechnicalError(fmt.Errorf("failed to get client to get child process instances: %w", err))
	}

	resp, err := client.GetChildProcessInstances(ctx, request)
	if err != nil || resp.Error != nil {
		e := fmt.Errorf("failed to get child process instances from partition %d", partitionId)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", e, err)
		} else if resp.Error != nil {
			return nil, fmt.Errorf("%w: %w", e, errors.New(resp.Error.GetMessage()))
		}
	}
	result = append(result, &proto.PartitionedProcessInstances{
		PartitionId: &partitionId,
		Instances:   resp.Instances,
		TotalCount:  resp.TotalCount,
	})

	return result, nil
}

func (node *ZenNode) GetDecisionInstance(ctx context.Context, decisionInstanceKey int64) (*proto.DecisionInstance, error) {
	state := node.store.ClusterState()
	partitionId := zenflake.GetPartitionId(decisionInstanceKey)
	follower, err := state.GetPartitionFollower(partitionId)
	if err != nil {
		return nil, zenerr.ClusterError(fmt.Errorf("failed to get follower node to get decision instance: %w", err))
	}
	client, err := node.client.For(follower.Addr)
	if err != nil {
		return nil, zenerr.ClusterError(fmt.Errorf("failed to get client to get decision instance: %w", err))
	}
	resp, err := client.GetDecisionInstance(ctx, &proto.GetDecisionInstanceRequest{
		DecisionInstanceKey: &decisionInstanceKey,
	})
	if err != nil {
		return nil, zenerr.TechnicalError(fmt.Errorf("failed to get decision instance from partition %d: %w", partitionId, err))
	}
	if resp.Error != nil {
		return nil, zenerr.ToZenError(resp.Error, fmt.Errorf("failed to get decision instance from partition %d", partitionId))
	}
	return resp.DecisionInstance, nil
}

// GetDecisionInstances will contact follower nodes and return decision instances in partitions they are following.
// Each partition is queried in parallel.
func (node *ZenNode) GetDecisionInstances(
	ctx context.Context,
	getDecisionInstancesRequest *proto.GetDecisionInstancesRequest,
) ([]*proto.PartitionedDecisionInstances, error) {
	clusterState := node.store.ClusterState()
	partitionIds := sortedPartitionIds(clusterState)

	partitionResults := make([][]*proto.PartitionedDecisionInstances, len(partitionIds))
	group, groupCtx := errgroup.WithContext(ctx)

	for idx, partitionId := range partitionIds {

		group.Go(func() error {
			partitions, err := node.getDecisionInstancesForPartition(
				groupCtx,
				clusterState,
				getDecisionInstancesRequest,
				partitionId,
			)
			if err != nil {
				return err
			}

			partitionResults[idx] = partitions
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return nil, err
	}

	return slices.Concat(partitionResults...), nil
}

func (node *ZenNode) getDecisionInstancesForPartition(
	ctx context.Context,
	clusterState state.Cluster,
	request *proto.GetDecisionInstancesRequest,
	partitionId uint32,
) ([]*proto.PartitionedDecisionInstances, error) {

	follower, err := clusterState.GetPartitionFollower(partitionId)
	if err != nil {
		return nil, zenerr.ClusterError(
			fmt.Errorf("failed to get follower node to get decision instances: %w", err),
		)
	}

	zenNodeClient, err := node.client.For(follower.Addr)
	if err != nil {
		return nil, zenerr.TechnicalError(
			fmt.Errorf("failed to get client to get decision instances: %w", err),
		)
	}

	req := gproto.Clone(request).(*proto.GetDecisionInstancesRequest)
	req.Partitions = []uint32{partitionId}

	resp, err := zenNodeClient.GetDecisionInstances(ctx, req)

	if err != nil {
		return nil, zenerr.TechnicalError(
			fmt.Errorf("failed to get decision instances from partition %d: %w", partitionId, err),
		)
	}

	if resp == nil {
		return nil, zenerr.TechnicalError(
			fmt.Errorf("failed to get decision instances from partition %d: response is nil", partitionId),
		)
	}

	if resp.Error != nil {
		return nil, zenerr.ToZenError(
			resp.Error,
			fmt.Errorf("failed to get decision instances from partition %d", partitionId),
		)
	}

	return resp.Partitions, nil
}

// GetProcessInstanceJobs will contact follower node of partition that contains process instance jobs
func (node *ZenNode) GetProcessInstanceJobs(ctx context.Context, page int32, size int32, processInstanceKey int64) (*proto.GetProcessInstanceJobsResponse, error) {
	state := node.store.ClusterState()
	partitionId := zenflake.GetPartitionId(processInstanceKey)
	follower, err := state.GetPartitionFollower(partitionId)
	if err != nil {
		return nil, zenerr.ClusterError(fmt.Errorf("failed to get follower node to get process instance jobs: %w", err))
	}
	client, err := node.client.For(follower.Addr)
	if err != nil {
		return nil, zenerr.TechnicalError(fmt.Errorf("failed to get client to get process instance jobs: %w", err))
	}
	resp, err := client.GetProcessInstanceJobs(ctx, &proto.GetProcessInstanceJobsRequest{
		ProcessInstanceKey: &processInstanceKey,
		Page:               &page,
		Size:               &size,
	})
	if err != nil {
		e := fmt.Errorf("failed to get process instance jobs from partition %d %w", partitionId, err)
		return nil, zenerr.TechnicalError(e)
	}
	if resp.Error != nil {
		e := fmt.Errorf("failed to get process instance jobs from partition %d", partitionId)
		return nil, zenerr.ToZenError(resp.Error, e)
	}
	return resp, nil
}

// GetFlowElementHistory will contact follower node of partition that contains process instance
func (node *ZenNode) GetFlowElementHistory(ctx context.Context, page int32, size int32, processInstanceKey int64) (*proto.GetFlowElementHistoryResponse, error) {
	state := node.store.ClusterState()
	partitionId := zenflake.GetPartitionId(processInstanceKey)
	follower, err := state.GetPartitionFollower(partitionId)
	if err != nil {
		return nil, zenerr.ClusterError(fmt.Errorf("failed to get follower node to get flow element history: %w", err))
	}
	client, err := node.client.For(follower.Addr)
	if err != nil {
		return nil, zenerr.TechnicalError(fmt.Errorf("failed to get client to get flow element history: %w", err))
	}
	resp, err := client.GetFlowElementHistory(ctx, &proto.GetFlowElementHistoryRequest{
		ProcessInstanceKey: &processInstanceKey,
		Page:               &page,
		Size:               &size,
	})
	if err != nil {
		e := fmt.Errorf("failed to get process instance flow element history from partition %d %w", partitionId, err)
		return nil, zenerr.TechnicalError(e)
	}
	if resp.Error != nil {
		e := fmt.Errorf("failed to get process instance flow element history from partition %d", partitionId)
		return nil, zenerr.ToZenError(resp.Error, e)
	}
	return resp, nil
}

// GetIncidents will contact follower node of partition that contains process instance
func (node *ZenNode) GetIncidents(ctx context.Context, page int32, size int32, processInstanceKey int64, state *string) (*proto.GetIncidentsResponse, error) {
	clusterState := node.store.ClusterState()
	partitionId := zenflake.GetPartitionId(processInstanceKey)
	follower, err := clusterState.GetPartitionFollower(partitionId)
	if err != nil {
		return nil, zenerr.ClusterError(fmt.Errorf("failed to get follower node to get incidents: %w", err))
	}
	client, err := node.client.For(follower.Addr)
	if err != nil {
		return nil, zenerr.TechnicalError(fmt.Errorf("failed to get client to get incidents: %w", err))
	}

	req := &proto.GetIncidentsRequest{
		ProcessInstanceKey: &processInstanceKey,
		Page:               &page,
		Size:               &size,
	}
	if state != nil {
		req.State = state
	}

	resp, err := client.GetIncidents(ctx, req)
	if err != nil {
		e := fmt.Errorf("failed to get incidents from partition %d %w", partitionId, err)
		return nil, zenerr.TechnicalError(e)
	}
	if resp.Error != nil {
		e := fmt.Errorf("failed to get incidents from partition %d", partitionId)
		return nil, zenerr.ToZenError(resp.Error, e)
	}
	return resp, nil
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
		return nil, zenerr.ClusterError(fmt.Errorf("failed to get node to start process instance on elements: %w", err))
	}
	client, err := node.client.For(candidateNode.Addr)
	if err != nil {
		return nil, zenerr.TechnicalError(fmt.Errorf("failed to get client to start process instance on elements: %w", err))
	}
	vars, err := json.Marshal(variables)
	if err != nil {
		return nil, zenerr.BadRequest(fmt.Errorf("failed to marshal variables to start process instance on elements: %w", err))
	}
	resp, err := client.StartProcessInstanceOnElements(ctx, &proto.StartInstanceOnElementIdsRequest{
		DefinitionKey:      &processDefinitionKey,
		StartingElementIds: startingElementIds,
		Variables:          vars,
	})
	if err != nil {
		return nil, zenerr.TechnicalError(fmt.Errorf("failed to start process instance on elements: %w", err))
	}
	if resp.Error != nil {
		return nil, zenerr.ToZenError(resp.Error, fmt.Errorf("failed to start process instance on elements"))
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
		jobs, err := db.Queries.GetWaitingJobs(node.ctx, sql.GetWaitingJobsParams{
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

func (node *ZenNode) JobAssignByKey(ctx context.Context, jobKey int64, assignee *string) error {
	partitionId := zenflake.GetPartitionId(jobKey)
	engine := node.controller.PartitionEngine(ctx, partitionId)
	if engine == nil {
		return fmt.Errorf("engine to assign job was not found on the node")
	}
	return engine.JobAssignByKey(ctx, jobKey, assignee)
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

func sortedPartitionIds(clusterState state.Cluster) []uint32 {
	partitionIds := make([]uint32, 0, len(clusterState.Partitions))
	for partitionId := range clusterState.Partitions {
		partitionIds = append(partitionIds, partitionId)
	}
	slices.Sort(partitionIds)
	return partitionIds
}
