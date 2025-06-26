package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
)

func Test_callActivity_startsAndCompletes_rightAway(t *testing.T) {

	// setup
	process, err := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")
	assert.NoError(t, err)
	process, err = bpmnEngine.LoadFromFile("./test-cases/call-activity-simple.bpmn")
	assert.NoError(t, err)

	variableName := "variable_name"
	taskId := "id"
	variableContext := make(map[string]interface{}, 1)
	variableContext[variableName] = "oldVal"

	handler := func(job ActivatedJob) {
		v := job.Variable(variableName)
		assert.Equal(t, "oldVal", v, "one should be able to read variables")
		job.SetVariable(variableName, "newVal")
		job.Complete()
	}

	h := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(h)

	// given
	taskHandler := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(taskHandler)

	// when
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	v := engineStorage.ProcessInstances[instance.Key]
	// then
	assert.NotNil(t, v, "Process instance needs to be present")
	assert.Equal(t, "newVal", v.VariableHolder.GetVariable(variableName))
	assert.Equal(t, runtime.ActivityStateCompleted, instance.State)
}

func Test_callActivity_startsAndCompletes_afterFinishingtheJob(t *testing.T) {
	// setup
	process, err := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")
	assert.NoError(t, err)
	process, err = bpmnEngine.LoadFromFile("./test-cases/call-activity-simple.bpmn")
	assert.NoError(t, err)

	variableName := "variable_name"
	variableContext := make(map[string]interface{}, 1)
	variableContext[variableName] = "oldVal"

	// when
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	parentInstanceKey := instance.Key
	var foundInstance *runtime.ProcessInstance
	for _, pi := range engineStorage.ProcessInstances {
		if pi.ParentProcessExecutionToken != nil && *&pi.ParentProcessExecutionToken.ProcessInstanceKey == parentInstanceKey {
			foundInstance = &pi
			break
		}
	}

	var job runtime.Job
	for _, j := range engineStorage.Jobs {
		if j.ProcessInstanceKey == foundInstance.Key {
			job = j
			break
		}
	}

	assert.NoError(t, err)
	bpmnEngine.JobCompleteByKey(t.Context(), job.Key, map[string]interface{}{
		variableName: "newVal",
	})
	assert.NoError(t, err)

	v := engineStorage.ProcessInstances[instance.Key]

	// then
	assert.NotNil(t, v, "Process instance needs to be present")
	assert.Equal(t, "newVal", v.VariableHolder.GetVariable(variableName))
	assert.Equal(t, runtime.ActivityStateCompleted, v.State)

}
