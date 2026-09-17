package server

import (
	"context"
	"os"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/partition"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/pbinitiative/zenbpm/pkg/bpmn"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gproto "google.golang.org/protobuf/proto"
)

func TestDeploymentRPCRejectsObsoleteGenerationBeforeWriting(t *testing.T) {
	ctx := t.Context()
	storage := inmemory.NewStorage()
	engine := bpmn.NewEngine(bpmn.EngineWithStorage(storage))
	t.Cleanup(engine.Stop)
	guard := &partition.DB{}
	current := partition.RestoreToken{OperationID: "restored", Epoch: 2}
	guard.EnterRestoreFence(current)
	guard.LeaveRestoreFence(current)
	srv := &Server{controller: &deploymentController{
		engine:    &engine,
		partition: &partition.ZenPartitionNode{PartitionId: 1, DB: guard},
	}}
	data, err := os.ReadFile("../../../test/e2e/testdata/timer_start_event/timer_start_event.bpmn")
	require.NoError(t, err)
	req := &proto.DeployProcessDefinitionRequest{
		PartitionId: new(uint32(1)), Key: new(int64(501)), Version: new(int32(1)), Data: data,
		RegisterProcessDefinitionSubscriptions: new(true),
	}
	for _, old := range []partition.RestoreToken{{}, {OperationID: "previous", Epoch: 1}} {
		req.RestoreOperationId, req.RestoreEpoch = new(old.OperationID), new(old.Epoch)
		resp, err := srv.DeployProcessDefinition(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp.GetError())
		assert.Equal(t, uint32(zenerr.ConflictCode), resp.GetError().GetCode(), "the deployer must re-observe and allocate, not repeat the partition RPC")
		_, err = storage.FindProcessDefinitionByKey(ctx, req.GetKey())
		require.Error(t, err, "the old request must not persist a definition")
		timers, err := storage.FindProcessDefinitionTimers(ctx, req.GetKey(), runtime.TimerStateCreated)
		require.NoError(t, err)
		assert.Empty(t, timers)
	}

	// Marshal the request as the real RPC does: losing these fields would
	// reject fresh deployments after restore as well as obsolete ones.
	req.RestoreOperationId, req.RestoreEpoch = new(current.OperationID), new(current.Epoch)
	wire, err := gproto.Marshal(req)
	require.NoError(t, err)
	var received proto.DeployProcessDefinitionRequest
	require.NoError(t, gproto.Unmarshal(wire, &received))
	resp, err := srv.DeployProcessDefinition(ctx, &received)
	require.NoError(t, err)
	require.Nil(t, resp.GetError())
	definition, err := storage.FindProcessDefinitionByKey(ctx, req.GetKey())
	require.NoError(t, err)
	assert.Equal(t, int32(1), definition.Version)
	timers, err := storage.FindProcessDefinitionTimers(ctx, req.GetKey(), runtime.TimerStateCreated)
	require.NoError(t, err)
	assert.Len(t, timers, 1)

	// a partition this node does not lead is refused as a transient
	// condition: the deployer retries against the current leader
	req.PartitionId = new(uint32(2))
	resp, err = srv.DeployProcessDefinition(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp.GetError())
	assert.Equal(t, uint32(zenerr.UnavailableCode), resp.GetError().GetCode())

	// a sender that predates cluster-wide allocation carries neither a
	// version nor a generation: it deploys under the partition's current one
	resp, err = srv.DeployProcessDefinition(ctx, &proto.DeployProcessDefinitionRequest{Key: new(int64(502)), Data: data})
	require.NoError(t, err)
	require.Nil(t, resp.GetError(), "the legacy path is not fenced by a generation it cannot know")
}

// Only deployment's controller methods are needed; unexpected calls fail rather
// than silently returning data from an unrelated controller fixture.
type deploymentController struct {
	ControllerService
	engine    *bpmn.Engine
	partition *partition.ZenPartitionNode
}

func (c *deploymentController) Engines(context.Context) map[uint32]*bpmn.Engine {
	return map[uint32]*bpmn.Engine{1: c.engine}
}

func (c *deploymentController) GetPartition(context.Context, uint32) *partition.ZenPartitionNode {
	return c.partition
}
