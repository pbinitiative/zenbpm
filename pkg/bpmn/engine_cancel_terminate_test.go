package bpmn

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTerminateExecutionTokensWithSingleKeyTerminatesOnlyMatchingToken(t *testing.T) {
	// setup: instance waiting with two active tokens on parallel branches
	engine, instance, tokens, tokenSaves := createParallelBranchesInstance(t)

	tokenB1, found := findTokenByElementID(tokens, "id-b-1")
	require.True(t, found, "expected an active token on id-b-1")
	tokenB2, found := findTokenByElementID(tokens, "id-b-2")
	require.True(t, found, "expected an active token on id-b-2")

	batch, err := engine.NewEngineBatch(t.Context(), instance)
	require.NoError(t, err)
	tokenSaves.Store(0)

	// when: terminate one of the two tokens (mixed matching and non-matching tokens)
	surviving, err := engine.terminateExecutionTokens(t.Context(), &batch, []int64{tokenB1.ElementInstanceKey}, instance.ProcessInstance().Key)
	require.NoError(t, err)
	require.NoError(t, batch.Flush(t.Context()))

	// then: the non-matching token survives exactly once
	require.Len(t, surviving, 1)
	assert.Equal(t, tokenB2.Key, surviving[0].Key)
	assert.Equal(t, int64(1), tokenSaves.Load(), "matching token should be terminated exactly once")

	// terminated token is never returned as active
	activeTokens, err := engine.persistence.GetActiveTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	require.Len(t, activeTokens, 1)
	assert.Equal(t, tokenB2.Key, activeTokens[0].Key)

	terminatedToken, err := engine.persistence.GetTokenByKey(t.Context(), tokenB1.Key)
	require.NoError(t, err)
	assert.Equal(t, runtime.TokenStateCanceled, terminatedToken.State)
}

func TestTerminateExecutionTokensWithMultipleKeysTerminatesEachTokenExactlyOnce(t *testing.T) {
	engine, instance, tokens, tokenSaves := createParallelBranchesInstance(t)

	tokenB1, found := findTokenByElementID(tokens, "id-b-1")
	require.True(t, found, "expected an active token on id-b-1")
	tokenB2, found := findTokenByElementID(tokens, "id-b-2")
	require.True(t, found, "expected an active token on id-b-2")

	batch, err := engine.NewEngineBatch(t.Context(), instance)
	require.NoError(t, err)
	tokenSaves.Store(0)

	// when: terminate both tokens
	surviving, err := engine.terminateExecutionTokens(t.Context(), &batch, []int64{tokenB1.ElementInstanceKey, tokenB2.ElementInstanceKey}, instance.ProcessInstance().Key)
	require.NoError(t, err)
	require.NoError(t, batch.Flush(t.Context()))

	// then: no surviving tokens, none returned as active
	assert.Empty(t, surviving)
	assert.Equal(t, int64(2), tokenSaves.Load(), "each matching token should be terminated exactly once")

	activeTokens, err := engine.persistence.GetActiveTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	assert.Empty(t, activeTokens)

	for _, key := range []int64{tokenB1.Key, tokenB2.Key} {
		token, err := engine.persistence.GetTokenByKey(t.Context(), key)
		require.NoError(t, err)
		assert.Equal(t, runtime.TokenStateCanceled, token.State)
	}
}

func TestTerminateExecutionTokensWithDuplicateKeysDoesNotDuplicateOperations(t *testing.T) {
	engine, instance, tokens, tokenSaves := createParallelBranchesInstance(t)

	tokenB1, found := findTokenByElementID(tokens, "id-b-1")
	require.True(t, found, "expected an active token on id-b-1")
	tokenB2, found := findTokenByElementID(tokens, "id-b-2")
	require.True(t, found, "expected an active token on id-b-2")

	batch, err := engine.NewEngineBatch(t.Context(), instance)
	require.NoError(t, err)
	tokenSaves.Store(0)

	// when: the same termination key is passed multiple times
	surviving, err := engine.terminateExecutionTokens(
		t.Context(),
		&batch,
		[]int64{tokenB1.ElementInstanceKey, tokenB1.ElementInstanceKey, tokenB1.ElementInstanceKey},
		instance.ProcessInstance().Key,
	)
	require.NoError(t, err)
	require.NoError(t, batch.Flush(t.Context()))

	// then: the surviving token appears in the result exactly once
	require.Len(t, surviving, 1)
	assert.Equal(t, tokenB2.Key, surviving[0].Key)

	// and the matching token was terminated exactly once
	terminatedToken, err := engine.persistence.GetTokenByKey(t.Context(), tokenB1.Key)
	require.NoError(t, err)
	assert.Equal(t, runtime.TokenStateCanceled, terminatedToken.State)
	assert.Equal(t, int64(1), tokenSaves.Load(), "duplicate keys must not terminate a token more than once")

	activeTokens, err := engine.persistence.GetActiveTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	require.Len(t, activeTokens, 1)
	assert.Equal(t, tokenB2.Key, activeTokens[0].Key)
}

func TestTerminateExecutionTokensWithNonMatchingKeysKeepsAllTokensActive(t *testing.T) {
	engine, instance, tokens, tokenSaves := createParallelBranchesInstance(t)

	batch, err := engine.NewEngineBatch(t.Context(), instance)
	require.NoError(t, err)
	tokenSaves.Store(0)

	// when: no active token matches the provided keys
	surviving, err := engine.terminateExecutionTokens(t.Context(), &batch, []int64{-1, -2}, instance.ProcessInstance().Key)
	require.NoError(t, err)
	require.NoError(t, batch.Flush(t.Context()))

	// then: every token survives exactly once
	require.Len(t, surviving, len(tokens))
	seen := make(map[int64]int, len(surviving))
	for _, token := range surviving {
		seen[token.Key]++
	}
	for _, token := range tokens {
		assert.Equal(t, 1, seen[token.Key], "token %d should appear in the result exactly once", token.Key)
	}
	assert.Equal(t, int64(0), tokenSaves.Load(), "non-matching tokens must not be terminated")
}

func TestCancelInstancePropagatesInnerCancelError(t *testing.T) {
	// setup: engine with a storage that can be switched to fail job lookups
	store := inmemory.NewStorage()
	failingStore := &failingJobReadStorage{Storage: store}
	engine := newCancelTerminateTestEngine(t, EngineWithStorage(failingStore))

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/simple-user-task.bpmn")
	require.NoError(t, err)
	instance, err := engine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)
	require.Equal(t, runtime.ActivityStateActive, instance.ProcessInstance().State)

	failingStore.failFindPendingJobs.Store(true)

	// when canceling the instance
	err = engine.CancelInstanceByKey(t.Context(), instance.ProcessInstance().Key)

	// then: the inner cancellation error is propagated to the caller
	require.Error(t, err)
	assert.ErrorIs(t, err, errFindPendingJobsFailure)
	assert.ErrorContains(t, err, "failed to cancel process instance")

	// and the unsuccessful cancellation is not reported as terminated
	failingStore.failFindPendingJobs.Store(false)
	storedInstance, err := engine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateActive, storedInstance.ProcessInstance().State,
		"process instance must not be marked terminated after a failed inner cancellation")
}

// createParallelBranchesInstance starts a parallel-gateway-flow instance on a dedicated
// engine where the first task is completed by a handler and the two parallel branch
// tokens stay active. A dedicated engine is used because the test process shares its
// process id with simple_task.bpmn used by other tests on the shared engine.
func createParallelBranchesInstance(t *testing.T) (Engine, runtime.ProcessInstance, []runtime.ExecutionToken, *atomic.Int64) {
	t.Helper()

	store := &countingTokenSaveStorage{Storage: inmemory.NewStorage()}
	engine := newCancelTerminateTestEngine(t, EngineWithStorage(store))

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/parallel-gateway-flow.bpmn")
	require.NoError(t, err)

	handler := engine.NewTaskHandler().Id("id-a-1").Handler(func(job ActivatedJob) {
		job.Complete()
	})
	defer engine.RemoveHandler(handler)

	instance, err := engine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)

	tokens, err := engine.persistence.GetActiveTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	require.Len(t, tokens, 2, "expected two active tokens on the parallel branches")

	return engine, instance, tokens, &store.tokenSaves
}

func newCancelTerminateTestEngine(t *testing.T, options ...EngineOption) Engine {
	t.Helper()

	engine := NewEngine(options...)
	t.Cleanup(engine.Stop)
	return engine
}

var errFindPendingJobsFailure = errors.New("storage failure: find pending jobs")

// failingJobReadStorage wraps a storage and fails to FindPendingProcessInstanceJobs on demand.
type failingJobReadStorage struct {
	storage.Storage
	failFindPendingJobs atomic.Bool
}

type countingTokenSaveStorage struct {
	storage.Storage
	tokenSaves atomic.Int64
}

type countingTokenSaveBatch struct {
	storage.Batch
	tokenSaves *atomic.Int64
}

func (s *failingJobReadStorage) FindPendingProcessInstanceJobs(ctx context.Context, processInstanceKey int64) ([]runtime.Job, error) {
	if s.failFindPendingJobs.Load() {
		return nil, errFindPendingJobsFailure
	}
	return s.Storage.FindPendingProcessInstanceJobs(ctx, processInstanceKey)
}

func (s *countingTokenSaveStorage) NewBatch() storage.Batch {
	return &countingTokenSaveBatch{Batch: s.Storage.NewBatch(), tokenSaves: &s.tokenSaves}
}

func (b *countingTokenSaveBatch) SaveToken(ctx context.Context, token runtime.ExecutionToken) error {
	b.tokenSaves.Add(1)
	return b.Batch.SaveToken(ctx, token)
}
