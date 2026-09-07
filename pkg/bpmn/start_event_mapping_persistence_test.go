package bpmn

import (
	"context"
	"errors"
	"maps"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/require"
)

func TestStartEventMappingPersistence(t *testing.T) {
	t.Run("Transition failure preserves variables and retry maps them once", func(t *testing.T) {
		engine, store, definition := setupStartMappingPersistence(t)
		store.failure = "transition"

		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"attempt": "initial"})
		require.ErrorIs(t, err, errStartMappingStorage)
		require.True(t, store.injected)
		incident := assertUncommittedStartMapping(t, store, instance)

		require.NoError(t, engine.ResolveIncident(t.Context(), incident.Key))
		assertCommittedStartMapping(t, store, instance.ProcessInstance().Key, incident.ElementInstanceKey)
	})

	t.Run("History failure preserves variables and retry maps them once", func(t *testing.T) {
		engine, store, definition := setupStartMappingPersistence(t)
		store.failure = "history"

		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"attempt": "initial"})
		require.ErrorIs(t, err, errStartMappingStorage)
		require.True(t, store.injected)
		incident := assertUncommittedStartMapping(t, store, instance)

		require.NoError(t, engine.ResolveIncident(t.Context(), incident.Key))
		assertCommittedStartMapping(t, store, instance.ProcessInstance().Key, incident.ElementInstanceKey)
	})

	t.Run("Process instance save failure preserves variables and retry maps them once", func(t *testing.T) {
		engine, store, definition := setupStartMappingPersistence(t)
		store.failure = "instance"

		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"attempt": "initial"})
		require.ErrorIs(t, err, errStartMappingStorage)
		require.True(t, store.injected)
		incident := assertUncommittedStartMapping(t, store, instance)

		require.NoError(t, engine.ResolveIncident(t.Context(), incident.Key))
		assertCommittedStartMapping(t, store, instance.ProcessInstance().Key, incident.ElementInstanceKey)
	})

	t.Run("Flush failure keeps the token at the start until incident resolution", func(t *testing.T) {
		engine, store, definition := setupStartMappingPersistence(t)
		store.failure = "flush"

		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"attempt": "initial"})
		require.ErrorIs(t, err, errStartMappingStorage)
		require.True(t, store.injected)
		incident := assertUncommittedStartMapping(t, store, instance)

		require.NoError(t, engine.ResolveIncident(t.Context(), incident.Key))
		assertCommittedStartMapping(t, store, instance.ProcessInstance().Key, incident.ElementInstanceKey)
	})

	t.Run("Token save failure leaves the original variables available for recovery", func(t *testing.T) {
		engine, store, definition := setupStartMappingPersistence(t)
		store.failure = "token"

		instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{"attempt": "initial"})
		require.ErrorIs(t, err, errStartMappingStorage)
		require.True(t, store.injected)
		require.Equal(t, "initial", instance.ProcessInstance().VariableHolder.GetLocalVariable("attempt"))
		persisted, err := store.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
		require.NoError(t, err)
		require.Equal(t, "initial", persisted.ProcessInstance().VariableHolder.GetLocalVariable("attempt"))
		incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key)
		require.NoError(t, err)
		require.Empty(t, incidents)
		tokens, err := store.GetActiveTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
		require.NoError(t, err)
		require.Len(t, tokens, 1)
		require.Equal(t, "StartEvent_1", tokens[0].ElementId)
		require.Equal(t, runtime.TokenStateRunning, tokens[0].State)

		require.NoError(t, engine.RunProcessInstance(t.Context(), persisted, tokens))
		assertCommittedStartMapping(t, store, instance.ProcessInstance().Key, tokens[0].ElementInstanceKey)
	})
}

func setupStartMappingPersistence(t *testing.T) (*Engine, *startMappingStorage, *runtime.ProcessDefinition) {
	t.Helper()
	store := &startMappingStorage{Storage: inmemory.NewStorage()}
	engine := NewEngine(EngineWithStorage(store))
	cleanupIncidentTestEngine(t, &engine)
	definition, err := engine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
	require.NoError(t, err)
	definition.Definitions.Process.StartEvents[0].Output = []extensions.TIoMapping{{Source: `=attempt + "-mapped"`, Target: "attempt"}}
	require.NoError(t, store.SaveProcessDefinition(t.Context(), *definition))
	return &engine, store, definition
}

func assertUncommittedStartMapping(t *testing.T, store *startMappingStorage, instance runtime.ProcessInstance) runtime.Incident {
	t.Helper()
	key := instance.ProcessInstance().Key
	require.Equal(t, "initial", instance.ProcessInstance().VariableHolder.GetLocalVariable("attempt"))
	persisted, err := store.FindProcessInstanceByKey(t.Context(), key)
	require.NoError(t, err)
	require.Equal(t, runtime.ActivityStateFailed, persisted.ProcessInstance().State)
	require.Equal(t, "initial", persisted.ProcessInstance().VariableHolder.GetLocalVariable("attempt"))
	incidents, err := store.FindIncidentsByProcessInstanceKey(t.Context(), key)
	require.NoError(t, err)
	require.Len(t, incidents, 1)
	token, err := store.GetTokenByKey(t.Context(), incidents[0].Token.Key)
	require.NoError(t, err)
	require.Equal(t, "StartEvent_1", token.ElementId)
	require.Equal(t, runtime.TokenStateFailed, token.State)
	_, err = store.GetFlowElementInstanceByKey(t.Context(), token.ElementInstanceKey)
	require.ErrorIs(t, err, storage.ErrNotFound, "the start history must be discarded with the transition")
	jobs, err := store.FindPendingProcessInstanceJobs(t.Context(), key)
	require.NoError(t, err)
	require.Empty(t, jobs, "a failed start must not run its outgoing task")
	return incidents[0]
}

func assertCommittedStartMapping(t *testing.T, store *startMappingStorage, key, startHistoryKey int64) {
	t.Helper()
	persisted, err := store.FindProcessInstanceByKey(t.Context(), key)
	require.NoError(t, err)
	require.Equal(t, runtime.ActivityStateActive, persisted.ProcessInstance().State)
	require.Equal(t, "initial-mapped", persisted.ProcessInstance().VariableHolder.GetLocalVariable("attempt"))
	history, err := store.GetFlowElementInstanceByKey(t.Context(), startHistoryKey)
	require.NoError(t, err)
	require.NotNil(t, history.CompletedAt)
	require.Equal(t, map[string]any{"attempt": "initial-mapped"}, history.OutputVariables)
	jobs, err := store.FindPendingProcessInstanceJobs(t.Context(), key)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.Equal(t, "id", jobs[0].ElementId)
	require.Equal(t, "initial-mapped", jobs[0].InputVariables["attempt"])
}

var errStartMappingStorage = errors.New("injected post-mapping storage failure")

type startMappingStorage struct {
	*inmemory.Storage
	failure  string
	injected bool
}

// Detach variable maps at storage boundaries so the in-memory backend cannot
// expose uncommitted mutations through shared pointers.
func cloneStartMappingInstance(instance runtime.ProcessInstance) runtime.ProcessInstance {
	clonedInstance := *instance.(*runtime.DefaultProcessInstance)
	clonedInstance.VariableHolder = runtime.NewVariableHolder(nil, maps.Clone(instance.ProcessInstance().VariableHolder.LocalVariables()))
	return &clonedInstance
}

func (s *startMappingStorage) SaveProcessInstance(ctx context.Context, instance runtime.ProcessInstance) error {
	return s.Storage.SaveProcessInstance(ctx, cloneStartMappingInstance(instance))
}

func (s *startMappingStorage) FindProcessInstanceByKey(ctx context.Context, key int64) (runtime.ProcessInstance, error) {
	instance, err := s.Storage.FindProcessInstanceByKey(ctx, key)
	if err != nil {
		return nil, err
	}
	return cloneStartMappingInstance(instance), nil
}

func (s *startMappingStorage) RefreshProcessInstance(ctx context.Context, instance runtime.ProcessInstance) error {
	if err := s.Storage.RefreshProcessInstance(ctx, instance); err != nil {
		return err
	}
	instance.ProcessInstance().VariableHolder = runtime.NewVariableHolder(nil, maps.Clone(instance.ProcessInstance().VariableHolder.LocalVariables()))
	return nil
}

func (s *startMappingStorage) NewBatch() storage.Batch {
	return &startMappingBatch{Batch: s.Storage.NewBatch(), store: s}
}

func (s *startMappingStorage) fail(operation string) bool {
	if s.failure != operation || s.injected {
		return false
	}
	s.injected = true
	return true
}

type startMappingBatch struct {
	storage.Batch
	store          *startMappingStorage
	completedStart bool
}

func (b *startMappingBatch) SaveProcessInstance(ctx context.Context, instance runtime.ProcessInstance) error {
	if b.completedStart && b.store.fail("instance") {
		return errStartMappingStorage
	}
	return b.Batch.SaveProcessInstance(ctx, cloneStartMappingInstance(instance))
}

func (b *startMappingBatch) SaveToken(ctx context.Context, token runtime.ExecutionToken) error {
	if b.completedStart && b.store.fail("token") {
		return errStartMappingStorage
	}
	return b.Batch.SaveToken(ctx, token)
}

func (b *startMappingBatch) SaveFlowElementInstance(ctx context.Context, history runtime.FlowElementInstance) error {
	if history.ElementId == "Flow_0xt1d7q" && b.store.fail("transition") {
		return errStartMappingStorage
	}
	return b.Batch.SaveFlowElementInstance(ctx, history)
}

func (b *startMappingBatch) UpdateOutputFlowElementInstance(ctx context.Context, history runtime.FlowElementInstance) error {
	if history.ElementId == "StartEvent_1" {
		b.completedStart = true
		if b.store.fail("history") {
			return errStartMappingStorage
		}
	}
	return b.Batch.UpdateOutputFlowElementInstance(ctx, history)
}

func (b *startMappingBatch) Flush(ctx context.Context) error {
	if b.completedStart && b.store.fail("flush") {
		return errStartMappingStorage
	}
	return b.Batch.Flush(ctx)
}
