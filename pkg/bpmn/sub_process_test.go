package bpmn

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
)

func TestCallActivityStartsAndCompletes(t *testing.T) {
	// setup
	_, err := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")
	assert.NoError(t, err)
	process, err := bpmnEngine.LoadFromFile("./test-cases/call-activity-simple.bpmn")
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

	v := engineStorage.ProcessInstances[instance.ProcessInstance().Key]
	time.Sleep(1 * time.Second)
	instance, err = bpmnEngine.FindProcessInstance(instance.ProcessInstance().Key)
	assert.NoError(t, err)
	// then
	assert.NotNil(t, v, "Process instance needs to be present")
	assert.Equal(t, "newVal", v.ProcessInstance().VariableHolder.GetLocalVariable(variableName))
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)
}

func TestSubProcessStartsAndCompletes(t *testing.T) {
	// setup
	process, err := bpmnEngine.LoadFromFile("./test-cases/simple_sub_process_task.bpmn")
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

	v := engineStorage.ProcessInstances[instance.ProcessInstance().Key]
	time.Sleep(2 * time.Second)
	instance, err = bpmnEngine.FindProcessInstance(instance.ProcessInstance().Key)
	assert.NoError(t, err)
	// then
	assert.NotNil(t, v, "Process instance needs to be present")
	assert.Equal(t, "newVal", v.ProcessInstance().VariableHolder.GetLocalVariable(variableName))
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)
}

func TestCallActivityStartsAndCompletesAfterFinishingTheJob(t *testing.T) {
	// setup
	_, err := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")
	assert.NoError(t, err)
	process, err := bpmnEngine.LoadFromFile("./test-cases/call-activity-simple.bpmn")
	assert.NoError(t, err)

	variableName := "variable_name"
	variableContext := make(map[string]interface{}, 1)
	variableContext[variableName] = "oldVal"

	// when
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	// wait for call activity process to be created
	time.Sleep(1 * time.Second)

	parentInstanceKey := instance.ProcessInstance().Key
	var foundInstance runtime.ProcessInstance
	for _, pi := range engineStorage.ProcessInstances {
		if pi.ParentProcessExecutionToken != nil && pi.ParentProcessExecutionToken.ProcessInstanceKey == parentInstanceKey {
			foundInstance = pi
			break
		}
	}

	var job runtime.Job
	jobs, err := bpmnEngine.persistence.FindPendingProcessInstanceJobs(t.Context(), foundInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(jobs), "There should be one job")
	job = jobs[0]

	assert.NoError(t, err)
	bpmnEngine.JobCompleteByKey(t.Context(), job.Key, map[string]interface{}{
		variableName: "newVal",
	})
	assert.NoError(t, err)

	// wait for parent process instance to continue
	time.Sleep(1 * time.Second)

	v, err := bpmnEngine.FindProcessInstance(foundInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.NotNil(t, v, "Process instance needs to be present")
	assert.Equal(t, runtime.ActivityStateCompleted.String(), v.ProcessInstance().State.String())
	assert.Equal(t, "newVal", v.ProcessInstance().VariableHolder.GetLocalVariable(variableName))
}

func TestCallActivityCancelsOnInterruptingBoundaryEvent(t *testing.T) {
	// setup
	_, err := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")
	assert.NoError(t, err)
	process, err := bpmnEngine.LoadFromFile("./test-cases/call-activity-with-boundary-simple.bpmn")
	assert.NoError(t, err)

	variableName := "variable_name"
	variableContext := make(map[string]interface{}, 2)
	variableContext[variableName] = "oldVal"

	randomCorellationKey := rand.Int63()

	variableContext["correlationKey"] = fmt.Sprint(randomCorellationKey)

	// when
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	// wait for call activity process to be created
	time.Sleep(1 * time.Second)

	parentInstanceKey := instance.ProcessInstance().Key
	var foundChildInstance runtime.ProcessInstance
	for _, pi := range engineStorage.ProcessInstances {
		if pi.ParentProcessExecutionToken != nil && pi.ParentProcessExecutionToken.ProcessInstanceKey == parentInstanceKey {
			foundChildInstance = pi
			break
		}
	}

	// when
	variables := map[string]interface{}{"payload": "message payload"}
	err = bpmnEngine.PublishMessageByName(t.Context(), "simple-boundary", fmt.Sprint(randomCorellationKey), variables)
	assert.NoError(t, err)

	// then
	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState(), "Parent instance should be completed")

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), foundChildInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateTerminated, instance.ProcessInstance().GetState(), "Child instance should be terminated")

	jobs := findActiveJobsForProcessInstance(instance.ProcessInstance().Key, "TestType")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(jobs))
}
