package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
)

func TestUserTasksCanBeHandled(t *testing.T) {

	// setup
	process, err := bpmnEngine.LoadFromFile("./test-cases/simple-user-task.bpmn")
	assert.NoError(t, err)
	cp := CallPath{}
	h := bpmnEngine.NewTaskHandler().Id("user-task").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(h)

	instance, _ := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)

	assert.Equal(t, runtime.ActivityStateCompleted, instance.State)
	assert.Equal(t, "user-task", cp.CallPath)
}

func TestUserTasksCanBeContinue(t *testing.T) {
	t.Skip("runtime modification of handlers is not supported yet")
	// setup
	process, err := bpmnEngine.LoadFromFile("./test-cases/simple-user-task.bpmn")
	assert.NoError(t, err)
	cp := CallPath{}

	// given

	instance, err := bpmnEngine.CreateInstance(t.Context(), process, nil)
	assert.NoError(t, err)

	userConfirm := false
	h := bpmnEngine.NewTaskHandler().Id("user-task").Handler(func(job ActivatedJob) {
		if userConfirm {
			cp.TaskHandler(job)
		}
	})
	defer bpmnEngine.RemoveHandler(h)

	tokens, err := bpmnEngine.persistence.GetActiveTokensForProcessInstance(t.Context(), instance.Key)
	assert.NoError(t, err)
	err = bpmnEngine.runProcessInstance(t.Context(), instance, tokens)
	assert.NoError(t, err)

	//when
	userConfirm = true
	tokens, err = bpmnEngine.persistence.GetActiveTokensForProcessInstance(t.Context(), instance.Key)
	assert.NoError(t, err)
	err = bpmnEngine.runProcessInstance(t.Context(), instance, tokens)
	assert.NoError(t, err)

	*instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.Key)
	assert.NoError(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, instance.State)
	assert.Equal(t, "user-task", cp.CallPath)
}
