package bpmn

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCallActivityStartsAndCompletes(t *testing.T) {
	_, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
	assert.NoError(t, err)
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/call-activity-simple.bpmn")
	assert.NoError(t, err)

	variableName := "variable_name"
	taskId := "id"
	variableContext := make(map[string]interface{}, 1)
	variableContext[variableName] = "oldVal"

	handler := func(job ActivatedJob) {
		v := job.Variable(variableName)
		assert.Equal(t, "oldVal", v, "one should be able to read variables")
		job.SetOutputVariable(variableName, "newVal")
		job.Complete()
	}

	h := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(h)

	taskHandler := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(taskHandler)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	assert.NotNil(t, instance, "Process instance needs to be present")
	assert.Equal(t, "newVal", instance.ProcessInstance().VariableHolder.GetLocalVariable(variableName))
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))
}

func TestCallActivityStartsAndCompletesAfterFinishingTheJob(t *testing.T) {
	_, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
	assert.NoError(t, err)
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/call-activity-simple.bpmn")
	assert.NoError(t, err)

	variableName := "variable_name"
	variableContext := make(map[string]interface{}, 1)
	variableContext[variableName] = "oldVal"

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)

	parentInstanceKey := instance.ProcessInstance().Key
	var foundInstance runtime.CallActivityInstance
	for _, pi := range engineStorage.ProcessInstances {
		if pi.Type() != runtime.ProcessTypeCallActivity {
			continue
		}
		if pi.(*runtime.CallActivityInstance).ParentProcessExecutionToken.ProcessInstanceKey == parentInstanceKey {
			foundInstance = *pi.(*runtime.CallActivityInstance)
			break
		}
	}

	var job runtime.Job
	jobs, err := bpmnEngine.persistence.FindPendingProcessInstanceJobs(t.Context(), foundInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(jobs), "There should be one job")
	job = jobs[0]

	assert.NoError(t, err)
	err = bpmnEngine.JobCompleteByKey(t.Context(), job.Key, map[string]interface{}{
		variableName: "newVal",
	})
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	v, err := bpmnEngine.FindProcessInstance(t.Context(), foundInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.NotNil(t, v, "Process instance needs to be present")
	assert.Equal(t, runtime.ActivityStateCompleted.String(), v.ProcessInstance().State.String())
	assert.Equal(t, "newVal", v.ProcessInstance().VariableHolder.GetLocalVariable(variableName))

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))
}

func TestCallActivityCancelsOnInterruptingBoundaryEvent(t *testing.T) {
	_, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
	assert.NoError(t, err)
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/call-activity-with-boundary-simple.bpmn")
	assert.NoError(t, err)

	variableName := "variable_name"
	variableContext := make(map[string]interface{}, 2)
	variableContext[variableName] = "oldVal"

	randomCorellationKey := fmt.Sprint(rand.Int63())

	variableContext["correlationKey"] = randomCorellationKey

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)

	parentInstanceKey := instance.ProcessInstance().Key
	var foundChildInstance runtime.CallActivityInstance
	for _, pi := range engineStorage.ProcessInstances {
		if pi.Type() != runtime.ProcessTypeCallActivity {
			continue
		}
		if pi.(*runtime.CallActivityInstance).ParentProcessExecutionToken.ProcessInstanceKey == parentInstanceKey {
			foundChildInstance = *pi.(*runtime.CallActivityInstance)
			break
		}
	}

	variables := map[string]interface{}{"payload": "message payload"}
	err = bpmnEngine.PublishMessageByName(t.Context(), "simple-boundary", &randomCorellationKey, variables)
	assert.NoError(t, err)

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

func TestCallActivityCorrelateBoundaryEvent(t *testing.T) {
	_, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/message-boundary-event-interrupting.bpmn")
	assert.NoError(t, err)
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/call-activity-with-boundary-with-inner-boundary-event.bpmn")
	assert.NoError(t, err)

	variableName := "variable_name"
	variableContext := make(map[string]interface{}, 2)
	variableContext[variableName] = "oldVal"

	randomCorellationKey := rand.Int63()

	variableContext["correlationKey"] = fmt.Sprint(randomCorellationKey)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)

	parentInstanceKey := instance.ProcessInstance().Key
	var foundChildInstance runtime.CallActivityInstance
	for _, pi := range engineStorage.ProcessInstances {
		if pi.Type() != runtime.ProcessTypeCallActivity {
			continue
		}
		if pi.(*runtime.CallActivityInstance).ParentProcessExecutionToken.ProcessInstanceKey == parentInstanceKey {
			foundChildInstance = *pi.(*runtime.CallActivityInstance)
			break
		}
	}

	time.Sleep(1 * time.Second)

	variables := map[string]interface{}{"payload": "message payload"}
	correlationKey := "message-boundary-event-interruptingCorrelationKey"
	err = bpmnEngine.PublishMessageByName(t.Context(), "simple-boundary", &correlationKey, variables)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), foundChildInstance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))
	subscriptions, err = bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState(), "Parent instance should be completed")

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), foundChildInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState(), "Child instance should be completed")

	jobs := findActiveJobsForProcessInstance(instance.ProcessInstance().Key, "TestType")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(jobs))
}

func TestSubProcessStartsAndCompletes(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple_sub_process_task.bpmn")
	assert.NoError(t, err)

	variableName := "variable_name"
	taskId := "id"
	variableContext := make(map[string]interface{}, 1)
	variableContext[variableName] = "oldVal"

	handler := func(job ActivatedJob) {
		v := job.Variable("testInput")
		assert.Equal(t, "oldVal", v, "one should be able to read variables")
		job.SetOutputVariable("testJobOutput", "newVal")
		job.Complete()
	}

	h := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(h)

	taskHandler := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(taskHandler)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	assert.NotNil(t, instance, "Process instance needs to be present")
	assert.Equal(t, "newVal", instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutput"))
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))
}

func TestSubProcessStartsAndCompletesAfterFinishingTheJob(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple_sub_process_task.bpmn")
	assert.NoError(t, err)

	variableName := "variable_name"
	variableContext := make(map[string]interface{}, 1)
	variableContext[variableName] = "oldVal"

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)

	parentInstanceKey := instance.ProcessInstance().Key
	var foundInstance runtime.SubProcessInstance
	for _, pi := range engineStorage.ProcessInstances {
		if pi.Type() != runtime.ProcessTypeSubProcess {
			continue
		}
		if pi.(*runtime.SubProcessInstance).ParentProcessExecutionToken.ProcessInstanceKey == parentInstanceKey {
			foundInstance = *pi.(*runtime.SubProcessInstance)
			break
		}
	}

	var job runtime.Job
	jobs, err := bpmnEngine.persistence.FindPendingProcessInstanceJobs(t.Context(), foundInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(jobs), "There should be one job")
	job = jobs[0]

	assert.NoError(t, err)
	assert.Equal(t, variableContext[variableName], engineStorage.Jobs[job.Key].Variables["testInput"])
	bpmnEngine.JobCompleteByKey(t.Context(), job.Key, map[string]interface{}{
		"testJobOutput": "newJobVal",
	})
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	v, err := bpmnEngine.FindProcessInstance(t.Context(), foundInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.NotNil(t, v, "Process instance needs to be present")
	assert.Equal(t, runtime.ActivityStateCompleted.String(), v.ProcessInstance().State.String())
	assert.Equal(t, "oldVal", v.ProcessInstance().VariableHolder.GetLocalVariable("testInput"))
	assert.Equal(t, "newJobVal", v.ProcessInstance().VariableHolder.GetLocalVariable("testTaskOutput"))

	v, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.NotNil(t, v, "Process instance needs to be present")
	assert.Equal(t, runtime.ActivityStateCompleted.String(), v.ProcessInstance().State.String())
	assert.Equal(t, "newJobVal", v.ProcessInstance().VariableHolder.GetLocalVariable("testOutput"))

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))
}

func TestSubProcessCancelsOnInterruptingBoundaryEvent(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple_sub_process_task.bpmn")
	assert.NoError(t, err)

	variableName := "variable_name"
	variableContext := make(map[string]interface{}, 1)
	variableContext[variableName] = "oldVal"

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)

	parentInstanceKey := instance.ProcessInstance().Key
	var foundChildInstance runtime.SubProcessInstance
	for _, pi := range engineStorage.ProcessInstances {
		if pi.Type() != runtime.ProcessTypeSubProcess {
			continue
		}
		if pi.(*runtime.SubProcessInstance).ParentProcessExecutionToken.ProcessInstanceKey == parentInstanceKey {
			foundChildInstance = *pi.(*runtime.SubProcessInstance)
			break
		}
	}

	variables := map[string]interface{}{"payload": "message payload"}
	correlationKey := "testMessage"
	err = bpmnEngine.PublishMessageByName(t.Context(), "OuterTestMessage", &correlationKey, variables)
	assert.NoError(t, err)

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

func TestSubProcessCorrelateBoundaryEvent(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple_sub_process_task.bpmn")
	assert.NoError(t, err)

	variableName := "variable_name"
	variableContext := make(map[string]interface{}, 1)
	variableContext[variableName] = "oldVal"

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)

	parentInstanceKey := instance.ProcessInstance().Key
	var foundChildInstance runtime.SubProcessInstance
	for _, pi := range engineStorage.ProcessInstances {
		if pi.Type() != runtime.ProcessTypeSubProcess {
			continue
		}
		if pi.(*runtime.SubProcessInstance).ParentProcessExecutionToken.ProcessInstanceKey == parentInstanceKey {
			foundChildInstance = *pi.(*runtime.SubProcessInstance)
			break
		}
	}

	time.Sleep(1 * time.Second)

	variables := map[string]interface{}{"payload": "message payload"}
	correlationKey := "testMessage"
	err = bpmnEngine.PublishMessageByName(t.Context(), "InnerTestMessage", &correlationKey, variables)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), foundChildInstance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))
	subscriptions, err = bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState(), "Parent instance should be completed")

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), foundChildInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState(), "Child instance should be completed")

	jobs := findActiveJobsForProcessInstance(instance.ProcessInstance().Key, "TestType")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(jobs))
}

func TestMultiInstanceSubprocessCancelsOnInterruptingBoundaryEvent(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_sub_process_task.bpmn")
	assert.NoError(t, err)

	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)

	parentInstanceKey := instance.ProcessInstance().Key
	var multiInstance runtime.MultiInstanceInstance
	for _, pi := range engineStorage.ProcessInstances {
		if pi.Type() != runtime.ProcessTypeMultiInstance {
			continue
		}
		if pi.(*runtime.MultiInstanceInstance).ParentProcessExecutionToken.ProcessInstanceKey == parentInstanceKey {
			multiInstance = *pi.(*runtime.MultiInstanceInstance)
			break
		}
	}

	time.Sleep(1 * time.Second)

	var foundChildInstance runtime.SubProcessInstance
	for _, pi := range engineStorage.ProcessInstances {
		if pi.Type() != runtime.ProcessTypeSubProcess {
			continue
		}
		if pi.(*runtime.SubProcessInstance).ParentProcessExecutionToken.ProcessInstanceKey == multiInstance.Key {
			foundChildInstance = *pi.(*runtime.SubProcessInstance)
			break
		}
	}

	// when
	variables := map[string]interface{}{"payload": "message payload"}
	correlationKey := "1234"
	err = bpmnEngine.PublishMessageByName(t.Context(), "boundary message", &correlationKey, variables)
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))
	subscriptions, err = bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), foundChildInstance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState(), "Parent instance should be completed")

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), foundChildInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateTerminated, instance.ProcessInstance().GetState(), "Child instance should be terminated")

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), multiInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateTerminated, instance.ProcessInstance().GetState(), "Multi Instance should be terminated")

	jobs := findActiveJobsForProcessInstance(instance.ProcessInstance().Key, "TestType")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(jobs))
}

func TestMultiInstanceServiceTaskStartsAndCompletesLocalJob(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_service_task.bpmn")
	assert.NoError(t, err)

	variableName := "testJobOutput"
	taskId := "Activity_0rae016"

	handler := func(job ActivatedJob) {
		v := job.GetLocalVariables()["testElementInput"].(string)
		job.SetOutputVariable(variableName, v+"newVal")
		job.Complete()
	}

	h := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(h)

	taskHandler := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(taskHandler)

	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2", "test3"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	assert.NotNil(t, instance, "Process instance needs to be present")

	testOutput := make([]string, 0)
	for _, str := range instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").([]interface{}) {
		testOutput = append(testOutput, str.(string))
	}

	assert.ElementsMatch(t, []string{"test1newVal", "test2newVal", "test3newVal"}, testOutput)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))
	subProcesses, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))
	assert.Equal(t, runtime.ActivityStateCompleted, subProcesses[0].ProcessInstance().State)
}

func TestMultiInstanceServiceTaskStartsAndCompletesOnWorkerJob(t *testing.T) {
	//TODO: Some test in another file is leaving jobs in database
	engineStorage.Jobs = make(map[int64]runtime.Job)

	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_service_task.bpmn")
	assert.NoError(t, err)

	variableContext := make(map[string]interface{}, 1)
	testInputCollection := []string{"test1", "test2", "test3"}
	variableContext["testInputCollection"] = testInputCollection
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	time.Sleep(1000 * time.Millisecond)

	variableName := "testJobOutput"
	for _, str := range testInputCollection {
		var job runtime.Job
		jobs, err := bpmnEngine.persistence.FindActiveJobsByType(t.Context(), "TestType")
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, len(jobs), 1, "There should be one job")
		job = jobs[0]

		err = bpmnEngine.JobCompleteByKey(t.Context(), job.Key, map[string]interface{}{
			variableName: str + "newVal",
		})
		assert.NoError(t, err)
	}

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	testOutput := make([]string, 0)
	for _, str := range instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").([]interface{}) {
		testOutput = append(testOutput, str.(string))
	}

	assert.ElementsMatch(t, []string{"test1newVal", "test2newVal", "test3newVal"}, testOutput)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))
	subProcesses, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))
	assert.Equal(t, runtime.ActivityStateCompleted, subProcesses[0].ProcessInstance().State)
}

// completeMultiInstanceReceiveTaskByName completes the active "receive task message" subscriptions of a
// multi instance receive task by publishing a message (via PublishMessageByName) for each provided input
// item. Each iteration subscribes with correlationKey "=item", so the message is published with the item as
// the correlation key and "<item>newVal" as the testJobOutput. The publish is retried until the matching
// subscription is active, so it works for both sequential (one active subscription at a time) and parallel
// (all active at once) multi instance receive tasks.
func completeMultiInstanceReceiveTaskByName(t *testing.T, items []string) {
	t.Helper()
	for _, item := range items {
		correlationKey := item
		require.Eventually(t, func() bool {
			err := bpmnEngine.PublishMessageByName(t.Context(), "receive task message", &correlationKey, map[string]interface{}{
				"testJobOutput": item + "newVal",
			})
			return err == nil
		}, 2*time.Second, 50*time.Millisecond)
	}
}

// completeMultiInstanceReceiveTaskBySubscription completes the active "receive task message" subscriptions of a
// multi instance receive task by publishing directly on each subscription object (via PublishMessage). It waits
// for the subscription whose correlation key matches the input item before publishing "<item>newVal".
func completeMultiInstanceReceiveTaskBySubscription(t *testing.T, items []string) {
	t.Helper()
	for _, item := range items {
		var target runtime.MessageSubscription
		require.Eventually(t, func() bool {
			for _, message := range engineStorage.MessageSubscriptions {
				sub, ok := message.(*runtime.TokenMessageSubscription)
				if !ok {
					continue
				}
				if sub.Name == "receive task message" && sub.State == runtime.ActivityStateActive && sub.CorrelationKey == item {
					target = message
					return true
				}
			}
			return false
		}, 2*time.Second, 50*time.Millisecond)
		err := bpmnEngine.PublishMessage(t.Context(), target, map[string]interface{}{
			"testJobOutput": item + "newVal",
		})
		assert.NoError(t, err)
	}
}

func assertMultiInstanceReceiveTaskInputVariables(t *testing.T, multiInstanceProcessKey int64, expected []string) {
	t.Helper()
	flowElementInstances, err := bpmnEngine.persistence.GetFlowElementInstancesByProcessInstanceKey(t.Context(), multiInstanceProcessKey, false)
	require.NoError(t, err)
	actual := make([]string, 0, len(expected))
	for _, flowElementInstance := range flowElementInstances {
		if flowElementInstance.ElementId != "Activity_0rae016" {
			continue
		}
		value, ok := flowElementInstance.InputVariables["testElementInput"].(string)
		if ok {
			actual = append(actual, value)
		}
	}
	assert.ElementsMatch(t, expected, actual)
}

func expectedMultiInstanceReceiveTaskOutput(items []string) []string {
	output := make([]string, 0, len(items))
	for _, item := range items {
		output = append(output, item+":"+item+"newVal")
	}
	return output
}

func TestReceiveTaskBoundaryMessageWithSameNameUsesCorrelationKey(t *testing.T) {
	cleanUpMessageSubscriptions()
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/receive_task/receive-task-boundary-message-same-name-interrupting.bpmn")
	require.NoError(t, err)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, map[string]interface{}{})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		count := 0
		for _, message := range engineStorage.MessageSubscriptions {
			sub, ok := message.(*runtime.TokenMessageSubscription)
			if !ok {
				continue
			}
			if sub.ElementId == "ReceiveTask_same_name" && sub.Name == "shared receive boundary message" && sub.State == runtime.ActivityStateActive {
				count++
			}
		}
		return count == 2
	}, 2*time.Second, 50*time.Millisecond)

	boundaryCorrelationKey := "boundary-correlation-key"
	err = bpmnEngine.PublishMessageByName(t.Context(), "shared receive boundary message", &boundaryCorrelationKey, nil)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]
		return ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted
	}, 2*time.Second, 50*time.Millisecond)

	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	assert.Equal(t, true, instance.ProcessInstance().VariableHolder.GetLocalVariable("boundaryFired"))
	assert.Nil(t, instance.ProcessInstance().VariableHolder.GetLocalVariable("approved"))

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	require.NoError(t, err)
	assert.Empty(t, subscriptions)
}

func TestMultiInstanceReceiveTaskStartsAndCompletesOnPublishedMessage(t *testing.T) {
	cleanUpMessageSubscriptions()
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/receive_task/multi_instance_receive_task.bpmn")
	assert.NoError(t, err)

	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2", "test3"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	completeMultiInstanceReceiveTaskByName(t, []string{"test1", "test2", "test3"})

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 2*time.Second, 50*time.Millisecond)

	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.NotNil(t, instance, "Process instance needs to be present")

	testOutput := make([]string, 0)
	for _, str := range instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").([]interface{}) {
		testOutput = append(testOutput, str.(string))
	}

	assert.ElementsMatch(t, expectedMultiInstanceReceiveTaskOutput([]string{"test1", "test2", "test3"}), testOutput)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))
	subProcesses, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))
	assert.Equal(t, runtime.ActivityStateCompleted, subProcesses[0].ProcessInstance().State)
	assertMultiInstanceReceiveTaskInputVariables(t, subProcesses[0].ProcessInstance().Key, []string{"test1", "test2", "test3"})
}

func TestMultiInstanceReceiveTaskStartsAndCompletesOnSubscription(t *testing.T) {
	cleanUpMessageSubscriptions()
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/receive_task/multi_instance_receive_task.bpmn")
	assert.NoError(t, err)

	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2", "test3"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	completeMultiInstanceReceiveTaskBySubscription(t, []string{"test1", "test2", "test3"})

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 2*time.Second, 50*time.Millisecond)

	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.NotNil(t, instance, "Process instance needs to be present")

	testOutput := make([]string, 0)
	for _, str := range instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").([]interface{}) {
		testOutput = append(testOutput, str.(string))
	}

	assert.ElementsMatch(t, expectedMultiInstanceReceiveTaskOutput([]string{"test1", "test2", "test3"}), testOutput)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))
	subProcesses, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))
	assert.Equal(t, runtime.ActivityStateCompleted, subProcesses[0].ProcessInstance().State)
	assertMultiInstanceReceiveTaskInputVariables(t, subProcesses[0].ProcessInstance().Key, []string{"test1", "test2", "test3"})
}

func TestMultiInstanceReceiveTaskSkipsWhenInputIsEmptyButFillsOutputWithEmptyList(t *testing.T) {
	cleanUpMessageSubscriptions()
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/receive_task/multi_instance_receive_task.bpmn")
	assert.NoError(t, err)

	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 2*time.Second, 50*time.Millisecond)

	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.NotNil(t, instance, "Process instance needs to be present")

	assert.Equal(t, 0, len(instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").([]interface{})))
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))

	subProcesses, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subProcesses))
}

func TestMultiInstanceParallelReceiveTaskStartsAndCompletesOnPublishedMessage(t *testing.T) {
	cleanUpMessageSubscriptions()
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/receive_task/multi_instance_parallel_receive_task.bpmn")
	assert.NoError(t, err)

	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2", "test3"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	completeMultiInstanceReceiveTaskByName(t, []string{"test1", "test2", "test3"})

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 2*time.Second, 50*time.Millisecond)

	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.NotNil(t, instance, "Process instance needs to be present")

	testOutput := make([]string, 0)
	for _, str := range instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").([]interface{}) {
		testOutput = append(testOutput, str.(string))
	}

	assert.ElementsMatch(t, expectedMultiInstanceReceiveTaskOutput([]string{"test1", "test2", "test3"}), testOutput)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))
	subProcesses, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))
	assert.Equal(t, runtime.ActivityStateCompleted, subProcesses[0].ProcessInstance().State)
	assertMultiInstanceReceiveTaskInputVariables(t, subProcesses[0].ProcessInstance().Key, []string{"test1", "test2", "test3"})
}

func TestMultiInstanceParallelReceiveTaskStartsAndCompletesOnSubscription(t *testing.T) {
	cleanUpMessageSubscriptions()
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/receive_task/multi_instance_parallel_receive_task.bpmn")
	assert.NoError(t, err)

	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2", "test3"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	completeMultiInstanceReceiveTaskBySubscription(t, []string{"test1", "test2", "test3"})

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 2*time.Second, 50*time.Millisecond)

	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.NotNil(t, instance, "Process instance needs to be present")

	testOutput := make([]string, 0)
	for _, str := range instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").([]interface{}) {
		testOutput = append(testOutput, str.(string))
	}

	assert.ElementsMatch(t, expectedMultiInstanceReceiveTaskOutput([]string{"test1", "test2", "test3"}), testOutput)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))
	subProcesses, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))
	assert.Equal(t, runtime.ActivityStateCompleted, subProcesses[0].ProcessInstance().State)
	assertMultiInstanceReceiveTaskInputVariables(t, subProcesses[0].ProcessInstance().Key, []string{"test1", "test2", "test3"})
}

func TestMultiInstanceParallelReceiveTaskSkipsWhenInputIsEmptyButFillsOutputWithEmptyList(t *testing.T) {
	cleanUpMessageSubscriptions()
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/receive_task/multi_instance_parallel_receive_task.bpmn")
	assert.NoError(t, err)

	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 2*time.Second, 50*time.Millisecond)

	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.NotNil(t, instance, "Process instance needs to be present")

	assert.Equal(t, 0, len(instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").([]interface{})))
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))

	subProcesses, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subProcesses))
}

func TestMultiInstanceParallelServiceTaskStartsAndCompletesWorkerJob(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_parallel_service_task.bpmn")
	assert.NoError(t, err)

	variableContext := make(map[string]interface{}, 1)
	testInputCollection := []string{"test1", "test2", "test3"}
	variableContext["testInputCollection"] = testInputCollection
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	time.Sleep(1000 * time.Millisecond)

	variableName := "testJobOutput"
	for _, str := range testInputCollection {
		var job runtime.Job
		jobs, err := bpmnEngine.persistence.FindActiveJobsByType(t.Context(), "TestType")
		assert.NoError(t, err)
		assert.NotEmpty(t, jobs, "There should be at least one job")
		job = jobs[0]

		err = bpmnEngine.JobCompleteByKey(t.Context(), job.Key, map[string]interface{}{
			variableName: str + "newVal",
		})
		assert.NoError(t, err)
	}

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	testOutput := make([]string, 0)
	for _, str := range instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").([]interface{}) {
		testOutput = append(testOutput, str.(string))
	}

	assert.ElementsMatch(t, []string{"test1newVal", "test2newVal", "test3newVal"}, testOutput)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))
	subProcesses, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))
	assert.Equal(t, runtime.ActivityStateCompleted, subProcesses[0].ProcessInstance().State)
}

func TestMultiInstanceParallelServiceTaskStartsAndCompletesLocalJob(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_parallel_service_task.bpmn")
	assert.NoError(t, err)

	variableName := "testJobOutput"
	taskId := "Activity_0rae016"

	handler := func(job ActivatedJob) {
		v := job.GetLocalVariables()["testElementInput"].(string)
		job.SetOutputVariable(variableName, v+"newVal")
		job.Complete()
	}

	h := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(h)

	taskHandler := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(taskHandler)

	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2", "test3"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	// then
	assert.NotNil(t, instance, "Process instance needs to be present")

	testOutput := make([]string, 0)
	for _, str := range instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").([]interface{}) {
		testOutput = append(testOutput, str.(string))
	}

	assert.ElementsMatch(t, []string{"test1newVal", "test2newVal", "test3newVal"}, testOutput)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))
	subProcesses, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))
	assert.Equal(t, runtime.ActivityStateCompleted, subProcesses[0].ProcessInstance().State)
}

func TestMultiInstanceBusinessRuleTaskStartsAndCompletesLocalJob(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_business_rule.bpmn")
	assert.NoError(t, err)

	definition, xmldata, err := bpmnEngine.dmnEngine.ParseDmnFromFile(filepath.Join("..", "dmn", "test-data", "bulk-evaluation-test", "can-autoliquidate-rule.dmn"))
	assert.NoError(t, err)
	_, _, err = bpmnEngine.dmnEngine.SaveDmnResourceDefinition(
		t.Context(),
		definition,
		xmldata,
		bpmnEngine.generateKey(),
	)
	assert.NoError(t, err)

	variableContext := make(map[string]interface{}, 1)
	testInputCollection := []int{1000, 500000, 200}
	variableContext["testInputCollection"] = testInputCollection
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	assert.ElementsMatch(t, []map[string]interface{}{{"canAutoLiquidate": true}, {"canAutoLiquidate": false}, {"canAutoLiquidate": true}}, instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection"))
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))
	subProcesses, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))
	assert.Equal(t, runtime.ActivityStateCompleted, subProcesses[0].ProcessInstance().State)
}

func TestMultiInstanceParallelBusinessRuleTaskStartsAndCompletesLocalJob(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_parallel_business_rule.bpmn")
	assert.NoError(t, err)

	definition, xmldata, err := bpmnEngine.dmnEngine.ParseDmnFromFile(filepath.Join("..", "dmn", "test-data", "bulk-evaluation-test", "can-autoliquidate-rule.dmn"))
	assert.NoError(t, err)
	_, _, err = bpmnEngine.dmnEngine.SaveDmnResourceDefinition(
		t.Context(),
		definition,
		xmldata,
		bpmnEngine.generateKey(),
	)
	assert.NoError(t, err)

	variableContext := make(map[string]interface{}, 1)
	testInputCollection := []int{1000, 500000, 200}
	variableContext["testInputCollection"] = testInputCollection
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	assert.ElementsMatch(t, []map[string]interface{}{{"canAutoLiquidate": true}, {"canAutoLiquidate": false}, {"canAutoLiquidate": true}}, instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection"))
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))
	subProcesses, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))
	assert.Equal(t, runtime.ActivityStateCompleted, subProcesses[0].ProcessInstance().State)
}

func TestMultiInstanceCallActivityStartsAndCompletes(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_call_activity_process.bpmn")
	assert.NoError(t, err)

	process, err = bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_call_activity_task.bpmn")
	assert.NoError(t, err)

	variableName := "testJobOutput"
	taskId := "id"

	handler := func(job ActivatedJob) {
		v := job.GetLocalVariables()["testElementInput"].(string)
		job.SetOutputVariable(variableName, v+"newVal")
		job.Complete()
	}

	taskHandler := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(taskHandler)

	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2", "test3"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	assert.NotNil(t, instance, "Process instance needs to be present")

	testOutput := make([]string, 0)
	for _, str := range instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").([]interface{}) {
		testOutput = append(testOutput, str.(string))
	}

	assert.ElementsMatch(t, []string{"test1newVal", "test2newVal", "test3newVal"}, testOutput)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))
	subProcesses, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))
	assert.Equal(t, runtime.ActivityStateCompleted, subProcesses[0].ProcessInstance().State)
}

func TestMultiInstanceParallelCallActivityStartsAndCompletes(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_call_activity_process.bpmn")
	assert.NoError(t, err)

	process, err = bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_parallel_call_activity_task.bpmn")
	assert.NoError(t, err)

	variableName := "testJobOutput"
	taskId := "id"

	handler := func(job ActivatedJob) {
		v := job.GetLocalVariables()["testElementInput"].(string)
		job.SetOutputVariable(variableName, v+"newVal")
		job.Complete()
	}

	taskHandler := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(taskHandler)

	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2", "test3"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 5000*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	assert.NotNil(t, instance, "Process instance needs to be present")

	testOutput := make([]string, 0)
	for _, str := range instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").([]interface{}) {
		testOutput = append(testOutput, str.(string))
	}

	assert.ElementsMatch(t, []string{"test1newVal", "test2newVal", "test3newVal"}, testOutput)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	tokens, err := bpmnEngine.persistence.GetAllTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))
	subProcesses, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))

	counter := 0
	assert.Eventually(t, func() bool {
		subprocess, testErr := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), subProcesses[0].ProcessInstance().Key)
		assert.NoError(t, testErr)
		if subprocess.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		//Logging the flaky tests
		if counter > 5 {
			for _, s := range engineStorage.ProcessInstances {
				println(s.ProcessInstance().Key)
				println(s.ProcessInstance().State.String())
				println(s.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").(string))
				println(s.ProcessInstance())
			}
			for _, t := range engineStorage.ExecutionTokens {
				println(t.Key)
				println(t.State.String())
				println(t.ElementId)
			}
		}
		counter++
		return false
	}, 5000*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	assert.Equal(t, runtime.ActivityStateCompleted, subProcesses[0].ProcessInstance().State)
}

func TestMultiInstanceSubProcessStartsAndCompletes(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_sub_process_task.bpmn")
	assert.NoError(t, err)

	variableName := "testJobOutput"
	taskId := "id"

	handler := func(job ActivatedJob) {
		v := job.GetLocalVariables()["testElementInput"].(string)
		job.SetOutputVariable(variableName, v+"newVal")
		job.Complete()
	}

	taskHandler := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(taskHandler)

	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2", "test3"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	assert.NotNil(t, instance, "Process instance needs to be present")

	testOutput := make([]string, 0)
	for _, str := range instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").([]interface{}) {
		testOutput = append(testOutput, str.(string))
	}

	assert.ElementsMatch(t, []string{"test1newVal", "test2newVal", "test3newVal"}, testOutput)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))
	subProcesses, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))
	assert.Equal(t, runtime.ActivityStateCompleted, subProcesses[0].ProcessInstance().State)
}

func TestMultiInstanceSubProcessStartsAndCompletesJobByKey(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_sub_process_task.bpmn")
	assert.NoError(t, err)

	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)

	var foundMultiInstance runtime.MultiInstanceInstance
	assert.Eventually(t, func() bool {
		for _, pi := range engineStorage.ProcessInstances {
			if pi.Type() == runtime.ProcessTypeMultiInstance && *pi.(*runtime.MultiInstanceInstance).GetParentProcessInstanceKey() == instance.ProcessInstance().Key {
				foundMultiInstance = *pi.(*runtime.MultiInstanceInstance)
				return true
			}
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)

	var foundInstance runtime.SubProcessInstance
	assert.Eventually(t, func() bool {
		for _, pi := range engineStorage.ProcessInstances {
			if pi.Type() == runtime.ProcessTypeSubProcess && *pi.(*runtime.SubProcessInstance).GetParentProcessInstanceKey() == foundMultiInstance.Key {
				foundInstance = *pi.(*runtime.SubProcessInstance)
				return true
			}
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)

	var jobs []runtime.Job
	assert.Eventually(t, func() bool {
		jobs, _ = bpmnEngine.persistence.FindPendingProcessInstanceJobs(t.Context(), foundInstance.ProcessInstance().Key)
		if len(jobs) == 1 {
			return true
		}
		return false
	}, 10000*time.Millisecond, 100*time.Millisecond)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(jobs), "There should be one job")
	job := jobs[0]
	err = bpmnEngine.JobCompleteByKey(t.Context(), job.Key, map[string]interface{}{
		"testJobOutput": "newJobVal1",
	})
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)

	assert.Eventually(t, func() bool {
		for _, pi := range engineStorage.ProcessInstances {
			if pi.Type() == runtime.ProcessTypeSubProcess && foundInstance.ProcessInstance().Key != pi.ProcessInstance().Key && *pi.(*runtime.SubProcessInstance).GetParentProcessInstanceKey() == foundMultiInstance.Key {
				foundInstance = *pi.(*runtime.SubProcessInstance)
				return true
			}
		}
		return false
	}, 2000*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	assert.Eventually(t, func() bool {
		jobs, _ = bpmnEngine.persistence.FindPendingProcessInstanceJobs(t.Context(), foundInstance.ProcessInstance().Key)
		if len(jobs) == 1 {
			return true
		}
		return false
	}, 10000*time.Millisecond, 100*time.Millisecond)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(jobs), "There should be one job")
	job = jobs[0]
	err = bpmnEngine.JobCompleteByKey(t.Context(), job.Key, map[string]interface{}{
		"testJobOutput": "newJobVal2",
	})
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)

	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	assert.NotNil(t, instance, "Process instance needs to be present")

	testOutput := make([]string, 0)
	for _, str := range instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").([]interface{}) {
		testOutput = append(testOutput, str.(string))
	}

	assert.ElementsMatch(t, []string{"newJobVal1", "newJobVal2"}, testOutput)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))
	subProcesses, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))
	assert.Equal(t, runtime.ActivityStateCompleted, subProcesses[0].ProcessInstance().State)
}

func TestMultiInstanceSubProcessCorrelateBoundaryEvent(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_sub_process_task.bpmn")
	assert.NoError(t, err)

	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)

	variables := map[string]interface{}{"payload": "message payload"}
	correlationKey := "1234"
	err = bpmnEngine.PublishMessageByName(t.Context(), "Event_1r7iviyMessage", &correlationKey, variables)
	assert.NoError(t, err)

	var foundChildInstance runtime.ProcessInstance
	assert.Eventually(t, func() bool {
		for _, pi := range engineStorage.ProcessInstances {
			if pi.Type() == runtime.ProcessTypeSubProcess && pi.ProcessInstance().State == runtime.ActivityStateCompleted {
				foundChildInstance = pi
				return true
			}
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	correlationKey = "1234"
	err = bpmnEngine.PublishMessageByName(t.Context(), "Event_1r7iviyMessage", &correlationKey, variables)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		for _, pi := range engineStorage.ProcessInstances {
			if pi.Type() == runtime.ProcessTypeDefault && pi.ProcessInstance().State == runtime.ActivityStateCompleted {
				return true
			}
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), foundChildInstance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))
	subscriptions, err = bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState(), "Parent instance should be completed")

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), foundChildInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState(), "Child instance should be completed")

	jobs := findActiveJobsForProcessInstance(instance.ProcessInstance().Key, "TestType")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(jobs))
}

func TestMultiInstanceParallelSubProcessStartsAndCompletes(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_parallel_sub_process_task.bpmn")
	assert.NoError(t, err)

	variableName := "testJobOutput"
	taskId := "id"

	handler := func(job ActivatedJob) {
		v := job.GetLocalVariables()["testElementInput"].(string)
		job.SetOutputVariable(variableName, v+"newVal")
		job.Complete()
	}

	taskHandler := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(taskHandler)

	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2", "test3"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 5000*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	assert.NotNil(t, instance, "Process instance needs to be present")

	testOutput := make([]string, 0)
	for _, str := range instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").([]interface{}) {
		testOutput = append(testOutput, str.(string))
	}

	assert.ElementsMatch(t, []string{"test1newVal", "test2newVal", "test3newVal"}, testOutput)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	tokens, err := bpmnEngine.persistence.GetAllTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))
	subProcesses, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))
	multiInstanceSubprocess := subProcesses[0]
	assert.Equal(t, runtime.ProcessTypeMultiInstance, multiInstanceSubprocess.Type())
	assert.ElementsMatch(t, []string{"test1", "test2", "test3"}, multiInstanceSubprocess.ProcessInstance().VariableHolder.GetLocalVariable("testInputCollection"))
	assert.Nil(t, multiInstanceSubprocess.ProcessInstance().VariableHolder.GetLocalVariable("item"))
	assert.Nil(t, instance.ProcessInstance().VariableHolder.GetLocalVariable("item"))

	counter := 0
	assert.Eventually(t, func() bool {
		multiInstanceSubprocessLoaded, testErr := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), multiInstanceSubprocess.ProcessInstance().Key)
		assert.NoError(t, testErr)
		if multiInstanceSubprocessLoaded.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		//Logging the flaky tests
		if counter > 5 {
			for _, s := range engineStorage.ProcessInstances {
				println(s.ProcessInstance().Key)
				println(s.ProcessInstance().State.String())
				println(s.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").(string))
				println(s.ProcessInstance())
			}
			for _, t := range engineStorage.ExecutionTokens {
				println(t.Key)
				println(t.State.String())
				println(t.ElementId)
			}
		}
		counter++
		return false
	}, 5000*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	assert.Equal(t, runtime.ActivityStateCompleted, multiInstanceSubprocess.ProcessInstance().State)
}

func TestMultiInstanceParallelSubProcessCorrelateBoundaryEventFailsToCreateParallelInstancesWithSameMessage(t *testing.T) {
	engineStorage.Incidents = make(map[int64]runtime.Incident)

	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_parallel_sub_process_task.bpmn")
	assert.NoError(t, err)

	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2", "test3"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)

	variables := map[string]interface{}{"payload": "message payload"}
	correlationKey := "1324"
	err = bpmnEngine.PublishMessageByName(t.Context(), "Event_0g0g0nbMessage", &correlationKey, variables)
	assert.NoError(t, err)

	var foundChildInstance runtime.ProcessInstance
	assert.Eventually(t, func() bool {
		for _, pi := range engineStorage.ProcessInstances {
			if pi.Type() == runtime.ProcessTypeSubProcess && pi.ProcessInstance().State == runtime.ActivityStateCompleted {
				foundChildInstance = pi
				return true
			}
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), foundChildInstance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))
	subscriptions, err = bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subscriptions))

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateActive, instance.ProcessInstance().GetState(), "Parent instance should be completed")

	instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), foundChildInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().GetState(), "Child instance should be completed")

	countFailed := 0
	multiInstanceActive := 0
	for _, p := range engineStorage.ProcessInstances {
		if p.ProcessInstance().State == runtime.ActivityStateFailed && p.Type() == runtime.ProcessTypeSubProcess {
			countFailed++
		}
		if p.Type() == runtime.ProcessTypeMultiInstance && p.ProcessInstance().State == runtime.ActivityStateActive {
			multiInstanceActive++
		}
	}
	assert.Equal(t, 2, countFailed)
	assert.Equal(t, 1, multiInstanceActive)

	assert.Equal(t, 2, len(engineStorage.Incidents))
	assert.Equal(t, 2, len(engineStorage.Incidents))

	jobs := findActiveJobsForProcessInstance(instance.ProcessInstance().Key, "TestType")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(jobs))
}

func TestMultiInstanceSkipsWhenInputIsEmptyButFillsOutputWithEmptyList(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_service_task.bpmn")
	assert.NoError(t, err)

	variableName := "testJobOutput"
	taskId := "Activity_0rae016"

	handler := func(job ActivatedJob) {
		v := job.GetLocalVariables()["testElementInput"].(string)
		job.SetOutputVariable(variableName, v+"newVal")
		job.Complete()
	}

	h := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(h)

	taskHandler := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(taskHandler)

	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	assert.NotNil(t, instance, "Process instance needs to be present")

	assert.Equal(t, 0, len(instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").([]interface{})))
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))

	subProcesses, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subProcesses))
}

func TestMultiInstanceParallelSkipsWhenInputIsEmptyButFillsOutputWithEmptyList(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_parallel_service_task.bpmn")
	assert.NoError(t, err)

	variableName := "testJobOutput"
	taskId := "Activity_0rae016"

	handler := func(job ActivatedJob) {
		v := job.GetLocalVariables()["testElementInput"].(string)
		job.SetOutputVariable(variableName, v+"newVal")
		job.Complete()
	}

	h := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(h)

	taskHandler := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(taskHandler)

	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		if processInstance, ok := engineStorage.ProcessInstances[instance.ProcessInstance().Key]; ok && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted {
			return true
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	time.Sleep(1 * time.Second)

	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	assert.NotNil(t, instance, "Process instance needs to be present")

	assert.Equal(t, 0, len(instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").([]interface{})))
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))

	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))

	subProcesses, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subProcesses))
}
