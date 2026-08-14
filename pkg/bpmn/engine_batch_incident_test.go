package bpmn

import (
	"context"
	"errors"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteAndFlushTokenIncidentRejectsPartialBatch(t *testing.T) {
	injectedErr := errors.New("injected queue failure")
	tests := []struct {
		name          string
		failedMethod  string
		expectedCalls []string
	}{
		{name: "token", failedMethod: "SaveToken", expectedCalls: []string{"SaveToken"}},
		{name: "process instance", failedMethod: "SaveProcessInstance", expectedCalls: []string{"SaveToken", "SaveProcessInstance"}},
		{name: "incident", failedMethod: "SaveIncident", expectedCalls: []string{"SaveToken", "SaveProcessInstance", "SaveIncident"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controlledBatch := &incidentTestBatch{failedMethod: tt.failedMethod, err: injectedErr}
			persistence := &incidentTestStorage{Storage: inmemory.NewStorage(), batch: controlledBatch}
			engine := NewEngine(EngineWithStorage(persistence))
			cleanupIncidentTestEngine(t, &engine)
			batch, err := engine.NewEngineBatchClean()
			require.NoError(t, err)

			instance := incidentTestInstance()
			err = batch.writeAndFlushTokenIncident(t.Context(), incidentTestToken(), instance, errors.New("original failure"))

			require.ErrorIs(t, err, injectedErr)
			assert.False(t, controlledBatch.flushed, "a partially queued incident batch must never be flushed")
			assert.Equal(t, tt.expectedCalls, controlledBatch.calls)
			assert.Equal(t, runtime.ActivityStateActive, instance.ProcessInstance().State)
		})
	}
}

func TestWriteAndFlushTokenIncidentLeavesLiveStateUnchangedOnFlushFailure(t *testing.T) {
	injectedErr := errors.New("injected flush failure")
	controlledBatch := &incidentTestBatch{failedMethod: "Flush", err: injectedErr}
	persistence := &incidentTestStorage{Storage: inmemory.NewStorage(), batch: controlledBatch}
	engine := NewEngine(EngineWithStorage(persistence))
	cleanupIncidentTestEngine(t, &engine)
	batch, err := engine.NewEngineBatchClean()
	require.NoError(t, err)
	instance := incidentTestInstance()

	err = batch.writeAndFlushTokenIncident(t.Context(), incidentTestToken(), instance, errors.New("original failure"))

	require.ErrorIs(t, err, injectedErr)
	assert.True(t, controlledBatch.flushed)
	assert.Equal(t, runtime.ActivityStateActive, instance.ProcessInstance().State)
}

func TestWriteAndFlushTokenIncidentFlushesCompleteBatch(t *testing.T) {
	controlledBatch := &incidentTestBatch{}
	persistence := &incidentTestStorage{Storage: inmemory.NewStorage(), batch: controlledBatch}
	engine := NewEngine(EngineWithStorage(persistence))
	cleanupIncidentTestEngine(t, &engine)
	batch, err := engine.NewEngineBatchClean()
	require.NoError(t, err)
	instance := incidentTestInstance()

	err = batch.writeAndFlushTokenIncident(t.Context(), incidentTestToken(), instance, errors.New("original failure"))

	require.NoError(t, err)
	assert.True(t, controlledBatch.flushed)
	assert.Equal(t, []string{"SaveToken", "SaveProcessInstance", "SaveIncident"}, controlledBatch.calls)
	assert.Equal(t, runtime.ActivityStateFailed, instance.ProcessInstance().State)
}

func TestWriteAndFlushTokenIncidentPersistsCompleteFailureState(t *testing.T) {
	persistence := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(persistence))
	cleanupIncidentTestEngine(t, &engine)
	batch, err := engine.NewEngineBatchClean()
	require.NoError(t, err)
	token := incidentTestToken()
	instance := incidentTestInstance()

	require.NoError(t, batch.writeAndFlushTokenIncident(t.Context(), token, instance, errors.New("original failure")))

	persistedToken, err := persistence.GetTokenByKey(t.Context(), token.Key)
	require.NoError(t, err)
	assert.Equal(t, runtime.TokenStateFailed, persistedToken.State)
	persistedInstance, err := persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateFailed, persistedInstance.ProcessInstance().State)
	incidents, err := persistence.FindIncidentsByExecutionTokenKey(t.Context(), token.Key)
	require.NoError(t, err)
	require.Len(t, incidents, 1)
	assert.Equal(t, "original failure", incidents[0].Message)
}

func TestSaveTokensDiscardsPartialBatchOnFailure(t *testing.T) {
	injectedErr := errors.New("injected token queue failure")
	partialBatch := &tokenSaveTestBatch{failedKey: 22, err: injectedErr}
	replacementBatch := &tokenSaveTestBatch{}
	persistence := &tokenSaveTestStorage{
		Storage: inmemory.NewStorage(),
		batches: []storage.Batch{partialBatch, replacementBatch},
	}
	engine := NewEngine(EngineWithStorage(persistence))
	cleanupIncidentTestEngine(t, &engine)
	batch, err := engine.NewEngineBatchClean()
	require.NoError(t, err)

	err = batch.saveTokens(t.Context(), []runtime.ExecutionToken{{Key: 11}, {Key: 22}, {Key: 33}})

	require.ErrorIs(t, err, injectedErr)
	assert.Equal(t, []int64{11, 22}, partialBatch.savedKeys)
	assert.Same(t, replacementBatch, batch.b, "the partially populated storage batch must be discarded")
	assert.False(t, partialBatch.flushed)
}

type incidentTestStorage struct {
	*inmemory.Storage
	batch storage.Batch
}

func (s *incidentTestStorage) NewBatch() storage.Batch {
	return s.batch
}

type incidentTestBatch struct {
	storage.Batch
	failedMethod string
	err          error
	calls        []string
	flushed      bool
}

func (b *incidentTestBatch) SaveToken(_ context.Context, _ runtime.ExecutionToken) error {
	return b.record("SaveToken")
}

func (b *incidentTestBatch) SaveProcessInstance(_ context.Context, _ runtime.ProcessInstance) error {
	return b.record("SaveProcessInstance")
}

func (b *incidentTestBatch) SaveIncident(_ context.Context, _ runtime.Incident) error {
	return b.record("SaveIncident")
}

func (b *incidentTestBatch) Flush(_ context.Context) error {
	b.flushed = true
	if b.failedMethod == "Flush" {
		return b.err
	}
	return nil
}

func (b *incidentTestBatch) record(method string) error {
	b.calls = append(b.calls, method)
	if b.failedMethod == method {
		return b.err
	}
	return nil
}

type tokenSaveTestStorage struct {
	*inmemory.Storage
	batches []storage.Batch
	next    int
}

func (s *tokenSaveTestStorage) NewBatch() storage.Batch {
	if s.next >= len(s.batches) {
		return s.Storage.NewBatch()
	}
	batch := s.batches[s.next]
	s.next++
	return batch
}

type tokenSaveTestBatch struct {
	storage.Batch
	failedKey int64
	err       error
	savedKeys []int64
	flushed   bool
}

func (b *tokenSaveTestBatch) SaveToken(_ context.Context, token runtime.ExecutionToken) error {
	b.savedKeys = append(b.savedKeys, token.Key)
	if token.Key == b.failedKey {
		return b.err
	}
	return nil
}

func (b *tokenSaveTestBatch) Flush(context.Context) error {
	b.flushed = true
	return nil
}

func incidentTestToken() runtime.ExecutionToken {
	return runtime.ExecutionToken{
		Key:                11,
		ElementInstanceKey: 12,
		ElementId:          "task",
		ProcessInstanceKey: 13,
		State:              runtime.TokenStateRunning,
	}
}

func incidentTestInstance() runtime.ProcessInstance {
	return &runtime.DefaultProcessInstance{ProcessInstanceData: runtime.ProcessInstanceData{
		Key:   13,
		State: runtime.ActivityStateActive,
	}}
}

func cleanupIncidentTestEngine(t *testing.T, engine *Engine) {
	t.Helper()
	t.Cleanup(func() {
		engine.feelRuntime.Stop()
		engine.jsRuntime.Stop()
	})
}
