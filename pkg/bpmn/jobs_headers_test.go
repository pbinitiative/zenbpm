package bpmn

import (
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// waitForActiveJob polls the storage until exactly one active job of jobType exists.
func waitForActiveJob(t *testing.T, store *inmemory.Storage, jobType string) runtime.Job {
	t.Helper()
	var jobs []runtime.Job
	require.Eventually(t, func() bool {
		var err error
		jobs, err = store.FindActiveJobsByType(t.Context(), jobType)
		return err == nil && len(jobs) == 1
	}, time.Second, 10*time.Millisecond)
	return jobs[0]
}

// TestJobHeadersAreHandedToInlineTaskHandler covers the in-process handler path:
// headers declared on a service task (here in the Camunda/Zeebe namespace, as
// authored by Camunda Modeler) must reach ActivatedJob.Headers().
func TestJobHeadersAreHandedToInlineTaskHandler(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/task_headers/service-task-with-headers.bpmn")
	require.NoError(t, err)

	expected := map[string]string{
		"url":    "https://example.com/orders",
		"method": "POST",
	}
	var seen map[string]string
	h := engine.NewTaskHandler().Type("headers-job").Handler(func(aj ActivatedJob) {
		seen = aj.Headers()
		aj.Complete()
	})
	defer engine.RemoveHandler(h)

	instance, err := engine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)
	assert.Equal(t, expected, seen)
}

// TestJobHeadersSurviveStorageAndActivation covers the persisted worker path:
// the job is stored first (no handler registered) and later activated through
// engine.ActivateJobs, which must expose the headers after a storage round-trip.
func TestJobHeadersSurviveStorageAndActivation(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/task_headers/service-task-with-headers.bpmn")
	require.NoError(t, err)

	_, err = engine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)

	storedJob := waitForActiveJob(t, store, "headers-job")
	assert.Equal(t, map[string]string{
		"url":    "https://example.com/orders",
		"method": "POST",
	}, storedJob.Headers, "headers must be persisted with the job")

	activatedJobs, err := engine.ActivateJobs(t.Context(), "headers-job")
	require.NoError(t, err)
	require.Len(t, activatedJobs, 1)
	assert.Equal(t, map[string]string{
		"url":    "https://example.com/orders",
		"method": "POST",
	}, activatedJobs[0].Headers())

	require.NoError(t, engine.JobCompleteByKey(t.Context(), activatedJobs[0].Key(), nil))
}

// TestJobWithoutHeadersHasNilHeaders guards the default: elements without a
// taskHeaders extension expose nil (not an empty map) to workers.
func TestJobWithoutHeadersHasNilHeaders(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))

	process, err := engine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
	require.NoError(t, err)

	_, err = engine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)

	storedJob := waitForActiveJob(t, store, "TestType")
	assert.Nil(t, storedJob.Headers)

	activatedJobs, err := engine.ActivateJobs(t.Context(), "TestType")
	require.NoError(t, err)
	require.Len(t, activatedJobs, 1)
	assert.Nil(t, activatedJobs[0].Headers())
}
