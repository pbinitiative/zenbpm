package server

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/partition"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/pbinitiative/zenbpm/pkg/bpmn"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	rqstore "github.com/rqlite/rqlite/v10/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newSuspensionServer(t *testing.T) (*Server, *inmemory.Storage) {
	t.Helper()
	store := inmemory.NewStorage()
	engine := bpmn.NewEngine(bpmn.EngineWithStorage(store))
	t.Cleanup(engine.Stop)
	return &Server{controller: &deploymentController{engine: &engine}}, store
}

func TestSetProcessSuspensionTakesTheNextVersionWithoutOne(t *testing.T) {
	srv, store := newSuspensionServer(t)

	first, err := srv.SetProcessSuspension(t.Context(), &proto.SetProcessSuspensionRequest{
		PartitionId: new(uint32(1)), BpmnProcessId: new("process"), Suspended: new(true),
	})
	require.NoError(t, err)
	require.Nil(t, first.GetError())
	assert.Equal(t, int64(1), first.GetVersion())
	assert.Equal(t, int64(1), first.GetHeldVersion())
	assert.True(t, first.GetHeldSuspended())
	suspended, err := store.IsProcessSuspended(t.Context(), "process")
	require.NoError(t, err)
	assert.True(t, suspended)

	second, err := srv.SetProcessSuspension(t.Context(), &proto.SetProcessSuspensionRequest{
		PartitionId: new(uint32(1)), BpmnProcessId: new("process"), Suspended: new(false),
	})
	require.NoError(t, err)
	require.Nil(t, second.GetError())
	assert.Equal(t, int64(2), second.GetVersion())
	suspended, err = store.IsProcessSuspended(t.Context(), "process")
	require.NoError(t, err)
	assert.False(t, suspended)
}

func TestSetProcessSuspensionKeepsTheChangeWithTheHigherVersion(t *testing.T) {
	srv, store := newSuspensionServer(t)
	send := func(suspended bool, version int64) *proto.SetProcessSuspensionResponse {
		resp, err := srv.SetProcessSuspension(t.Context(), &proto.SetProcessSuspensionRequest{
			PartitionId: new(uint32(1)), BpmnProcessId: new("process"), Suspended: &suspended, Version: &version,
		})
		require.NoError(t, err)
		require.Nil(t, resp.GetError())
		assert.Equal(t, version, resp.GetVersion(), "the answer carries the version it was given")
		return resp
	}

	send(false, 6)
	late := send(true, 5)
	suspended, err := store.IsProcessSuspended(t.Context(), "process")
	require.NoError(t, err)
	assert.False(t, suspended, "a change must not undo a newer one")
	assert.Equal(t, int64(6), late.GetHeldVersion(), "the answer tells which change the partition holds")
	assert.False(t, late.GetHeldSuspended())
}

func TestSetProcessSuspensionTakesAVersionAboveTheMinimum(t *testing.T) {
	srv, _ := newSuspensionServer(t)

	resp, err := srv.SetProcessSuspension(t.Context(), &proto.SetProcessSuspensionRequest{
		PartitionId: new(uint32(1)), BpmnProcessId: new("process"), Suspended: new(true), MinVersion: new(int64(9)),
	})

	require.NoError(t, err)
	require.Nil(t, resp.GetError())
	assert.Equal(t, int64(10), resp.GetVersion())
	assert.Equal(t, int64(10), resp.GetHeldVersion())
}

// failingSuspensionStorage fails the changes of a suspension.
type failingSuspensionStorage struct {
	*inmemory.Storage
	err error
}

func (f *failingSuspensionStorage) ChangeProcessSuspension(context.Context, string, bool, ...storage.ChangeProcessSuspensionOption) (int64, error) {
	return 0, f.err
}

func (f *failingSuspensionStorage) ApplyProcessSuspension(context.Context, string, bool, int64) (storage.ProcessSuspension, error) {
	return storage.ProcessSuspension{}, f.err
}

func TestSetProcessSuspensionTellsAPartitionWhichCannotTakeTheChangeNowToTryAgain(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code zenerr.ZenErrorCode
	}{
		{"a store which is not open", fmt.Errorf("write: %w", rqstore.ErrNotOpen), zenerr.UnavailableCode},
		{"a partition fenced for a restore", fmt.Errorf("write: %w", partition.ErrPartitionFenced), zenerr.UnavailableCode},
		{"a node which lost the lead", fmt.Errorf("write: %w", rqstore.ErrNotLeader), zenerr.UnavailableCode},
		{"a node which has not caught up", fmt.Errorf("read: %w", rqstore.ErrNotReady), zenerr.UnavailableCode},
		{"a stale read", fmt.Errorf("read: %w", rqstore.ErrStaleRead), zenerr.UnavailableCode},
		{"anything else", errors.New("disk is gone"), zenerr.TechnicalErrorCode},
	}
	for _, tt := range tests {
		for _, version := range []int64{0, 3} {
			t.Run(fmt.Sprintf("%s, version %d", tt.name, version), func(t *testing.T) {
				engine := bpmn.NewEngine(bpmn.EngineWithStorage(&failingSuspensionStorage{Storage: inmemory.NewStorage(), err: tt.err}))
				t.Cleanup(engine.Stop)
				srv := &Server{controller: &deploymentController{engine: &engine}}

				resp, err := srv.SetProcessSuspension(t.Context(), &proto.SetProcessSuspensionRequest{
					PartitionId: new(uint32(1)), BpmnProcessId: new("process"), Suspended: new(true), Version: &version,
				})

				require.NoError(t, err)
				require.NotNil(t, resp.GetError())
				assert.Equal(t, uint32(tt.code), resp.GetError().GetCode())
			})
		}
	}
}

func TestSetProcessSuspensionRefusesWhatItCannotDo(t *testing.T) {
	srv, store := newSuspensionServer(t)

	tests := []struct {
		name string
		req  *proto.SetProcessSuspensionRequest
		code zenerr.ZenErrorCode
	}{
		{
			name: "a partition this node does not lead is a transient condition",
			req:  &proto.SetProcessSuspensionRequest{PartitionId: new(uint32(2)), BpmnProcessId: new("process"), Suspended: new(true)},
			code: zenerr.UnavailableCode,
		},
		{
			name: "no process id",
			req:  &proto.SetProcessSuspensionRequest{PartitionId: new(uint32(1)), Suspended: new(true)},
			code: zenerr.BadRequestCode,
		},
		{
			name: "no partition",
			req:  &proto.SetProcessSuspensionRequest{BpmnProcessId: new("process"), Suspended: new(true)},
			code: zenerr.BadRequestCode,
		},
		{
			name: "a negative minimum version",
			req:  &proto.SetProcessSuspensionRequest{PartitionId: new(uint32(1)), BpmnProcessId: new("process"), Suspended: new(true), MinVersion: new(int64(-1))},
			code: zenerr.BadRequestCode,
		},
		{
			name: "a negative version",
			req:  &proto.SetProcessSuspensionRequest{PartitionId: new(uint32(1)), BpmnProcessId: new("process"), Suspended: new(true), Version: new(int64(-1))},
			code: zenerr.BadRequestCode,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := srv.SetProcessSuspension(t.Context(), tt.req)
			require.NoError(t, err)
			require.NotNil(t, resp.GetError())
			assert.Equal(t, uint32(tt.code), resp.GetError().GetCode())
		})
	}
	suspended, err := store.IsProcessSuspended(t.Context(), "process")
	require.NoError(t, err)
	assert.False(t, suspended, "a refused request changes nothing")
}

// suspensionController serves one engine, for the calls which start an instance or publish a message.
type suspensionController struct {
	ControllerService
	engine *bpmn.Engine
}

func (c *suspensionController) Engines(context.Context) map[uint32]*bpmn.Engine {
	return map[uint32]*bpmn.Engine{1: c.engine}
}

func (c *suspensionController) PartitionEngine(context.Context, uint32) *bpmn.Engine {
	return c.engine
}

// TestAnInstanceOfASuspendedProcessIsRefusedAsAConflict shows that every call which starts an instance or
// publishes a message answers a conflict, which the REST api turns into a 409, when the process is suspended.
func TestAnInstanceOfASuspendedProcessIsRefusedAsAConflict(t *testing.T) {
	store := inmemory.NewStorage()
	engine := bpmn.NewEngine(bpmn.EngineWithStorage(store))
	t.Cleanup(engine.Stop)
	srv := &Server{controller: &suspensionController{engine: &engine}}

	task, err := engine.LoadFromFile(t.Context(), "../../../pkg/bpmn/test-cases/simple_task.bpmn")
	require.NoError(t, err)
	message, err := engine.LoadFromFile(t.Context(), "../../../pkg/bpmn/test-cases/process_definition_start_event/message-start-event-process.bpmn")
	require.NoError(t, err)
	require.NoError(t, engine.RegisterProcessDefinitionSubscriptions(t.Context(), message.Key))
	var subscriptionKey int64
	for key, subscription := range store.MessageSubscriptions {
		if subscription.MessageSubscription().ProcessDefinitionKey == message.Key {
			subscriptionKey = key
		}
	}
	require.NotZero(t, subscriptionKey)
	for _, processId := range []string{task.BpmnProcessId, message.BpmnProcessId} {
		_, err := engine.SetProcessSuspended(t.Context(), processId, true)
		require.NoError(t, err)
	}

	requireConflict := func(t *testing.T, resp *proto.ErrorResult) {
		t.Helper()
		require.NotNil(t, resp)
		assert.Equal(t, uint32(zenerr.ConflictCode), resp.GetCode())
		assert.Contains(t, resp.GetMessage(), "suspended")
	}

	t.Run("create an instance by key", func(t *testing.T) {
		resp, err := srv.CreateInstance(t.Context(), &proto.CreateInstanceRequest{
			StartBy:   &proto.CreateInstanceRequest_DefinitionKey{DefinitionKey: task.Key},
			Variables: []byte("{}"),
		})
		require.NoError(t, err)
		requireConflict(t, resp.GetError())
	})

	t.Run("create an instance by process id", func(t *testing.T) {
		resp, err := srv.CreateInstance(t.Context(), &proto.CreateInstanceRequest{
			StartBy:   &proto.CreateInstanceRequest_LatestProcessId{LatestProcessId: task.BpmnProcessId},
			Variables: []byte("{}"),
		})
		require.NoError(t, err)
		requireConflict(t, resp.GetError())
	})

	t.Run("start an instance on elements", func(t *testing.T) {
		resp, err := srv.StartProcessInstanceOnElements(t.Context(), &proto.StartInstanceOnElementIdsRequest{
			DefinitionKey:      new(task.Key),
			StartingElementIds: []string{"id"},
			Variables:          []byte("{}"),
		})
		require.NoError(t, err)
		requireConflict(t, resp.GetError())
	})

	t.Run("publish the message of a message start event by name", func(t *testing.T) {
		resp, err := srv.PublishMessageStartProcessInstance(t.Context(), &proto.PublishMessageStartProcessInstanceRequest{
			MessageName: new("messageStartEventProcessRef"),
		})
		require.NoError(t, err)
		requireConflict(t, resp.GetError())
	})

	t.Run("publish the message of a message start event by subscription key", func(t *testing.T) {
		resp, err := srv.PublishMessage(t.Context(), &proto.PublishMessageRequest{
			Key:       new(subscriptionKey),
			Variables: []byte("{}"),
		})
		require.NoError(t, err)
		requireConflict(t, resp.GetError())
	})

	assert.Empty(t, store.ProcessInstancesSnapshot(), "a refused request starts nothing")
}
