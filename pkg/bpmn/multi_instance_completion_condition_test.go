package bpmn

import (
	"os"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMultiInstanceCompletionConditionIsADeploymentCheck shows a completion
// condition is refused only for a new deployment. A definition an older engine
// stored with one keeps loading and, as before, runs every iteration: the
// condition is never evaluated.
func TestMultiInstanceCompletionConditionIsADeploymentCheck(t *testing.T) {
	stored, err := os.ReadFile("./test-cases/multi_instance_completion_condition.bpmn")
	require.NoError(t, err)
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	t.Cleanup(engine.Stop)

	for name, deploy := range map[string]func() error{
		"LoadFromBytes": func() error {
			_, err := engine.LoadFromBytes(t.Context(), stored, engine.generateKey())
			return err
		},
		"DeployProcessDefinition": func() error {
			_, err := engine.DeployProcessDefinition(t.Context(), stored, engine.generateKey(), 1)
			return err
		},
		"ParseProcessDefinitionIdentity": func() error {
			_, err := ParseProcessDefinitionIdentity(stored)
			return err
		},
	} {
		assert.ErrorContains(t, deploy(), `serviceTask id="service_task" has a multi-instance completionCondition`, "%s: a new deployment is refused", name)
	}

	old, err := engine.ImportProcessDefinition(t.Context(), stored, engine.generateKey(), 1, false)
	require.NoError(t, err, "what is stored keeps loading")
	engine.NewTaskHandler().Type("completion-condition-task").Handler(func(job ActivatedJob) { job.Complete() })

	instance, err := engine.CreateInstanceByKey(t.Context(), old.Key, map[string]any{"items": []string{"first", "second", "third"}})
	require.NoError(t, err)

	// the stored instance is the object the engine mutates, so read it under the
	// same per-instance lock the engine holds while continuing the multi-instance
	instanceKey := instance.ProcessInstance().Key
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		engine.runningInstances.lockInstance(instanceKey)
		defer engine.runningInstances.unlockInstance(instanceKey)

		current, err := store.FindProcessInstanceByKey(t.Context(), instanceKey)
		if !assert.NoError(collect, err) {
			return
		}
		if !assert.Equal(collect, runtime.ActivityStateCompleted, current.ProcessInstance().State) {
			return
		}
		assert.ElementsMatch(collect, []any{"first", "second", "third"}, current.ProcessInstance().VariableHolder.GetLocalVariable("results"),
			"every iteration runs although the condition holds after the first one")
	}, 5*time.Second, 20*time.Millisecond)
}
