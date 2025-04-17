package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
)

func Test_user_tasks_can_be_handled(t *testing.T) {

	// setup
	process, err := bpmnEngine.LoadFromFile("./test-cases/simple-user-task.bpmn")
	assert.Nil(t, err)
	cp := CallPath{}
	bpmnEngine.NewTaskHandler().Id("user-task").Handler(cp.TaskHandler)

	instance, _ := bpmnEngine.CreateAndRunInstance(process.Key, nil)

	assert.Equal(t, runtime.ActivityStateCompleted, instance.State)
	assert.Equal(t, "user-task", cp.CallPath)
}

func Test_user_tasks_can_be_continue(t *testing.T) {
	// setup
	process, err := bpmnEngine.LoadFromFile("./test-cases/simple-user-task.bpmn")
	assert.Nil(t, err)
	cp := CallPath{}
	bpmnEngine.clearTaskHandlers()

	// given

	instance, _ := bpmnEngine.CreateInstance(process, nil)

	userConfirm := false
	bpmnEngine.NewTaskHandler().Id("user-task").Handler(func(job ActivatedJob) {
		if userConfirm {
			cp.TaskHandler(job)
		}
	})
	_, err = bpmnEngine.RunOrContinueInstance(instance.Key)
	assert.Nil(t, err)

	//when
	userConfirm = true
	instance, err = bpmnEngine.RunOrContinueInstance(instance.Key)
	assert.Nil(t, err)

	// then
	assert.Equal(t, runtime.ActivityStateCompleted, instance.State)
	assert.Equal(t, "user-task", cp.CallPath)
}
