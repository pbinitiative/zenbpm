package bpmn

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
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

	variables := map[string]interface{}{"payload": "message payload"}
	err = bpmnEngine.PublishMessageByName(t.Context(), "simple-boundary", fmt.Sprint(randomCorellationKey), variables)
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
	err = bpmnEngine.PublishMessageByName(t.Context(), "simple-boundary", "message-boundary-event-interruptingCorrelationKey", variables)
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
	err = bpmnEngine.PublishMessageByName(t.Context(), "OuterTestMessage", "testMessage", variables)
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
	err = bpmnEngine.PublishMessageByName(t.Context(), "InnerTestMessage", "testMessage", variables)
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
	err = bpmnEngine.PublishMessageByName(t.Context(), "boundary message", "1234", variables)
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
	err = bpmnEngine.PublishMessageByName(t.Context(), "Event_1r7iviyMessage", "1234", variables)
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

	err = bpmnEngine.PublishMessageByName(t.Context(), "Event_1r7iviyMessage", "1234", variables)
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
	err = bpmnEngine.PublishMessageByName(t.Context(), "Event_0g0g0nbMessage", "1324", variables)
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
