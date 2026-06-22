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
	taskHandler := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(taskHandler)
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)
	waitForProcessCompletion(t, instance.ProcessInstance().Key, 2*time.Second, 50*time.Millisecond)
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
	foundInstance := findChildCallActivityInstance(t, instance.ProcessInstance().Key)
	job := waitForPendingJob(t, foundInstance.ProcessInstance().Key)
	err = bpmnEngine.JobCompleteByKey(t.Context(), job.Key, map[string]interface{}{
		variableName: "newVal",
	})
	assert.NoError(t, err)
	waitForProcessCompletion(t, instance.ProcessInstance().Key, 2*time.Second, 50*time.Millisecond)
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
	foundChildInstance := findChildCallActivityInstance(t, instance.ProcessInstance().Key)
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
	foundChildInstance := findChildCallActivityInstance(t, instance.ProcessInstance().Key)
	// Wait for the inner boundary event subscription to be set up
	require.Eventually(t, func() bool {
		subs, subErr := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), foundChildInstance.ProcessInstance().Key, runtime.ActivityStateActive)
		return subErr == nil && len(subs) > 0
	}, 2*time.Second, 50*time.Millisecond)
	variables := map[string]interface{}{"payload": "message payload"}
	correlationKey := new(string)
	*correlationKey = "message-boundary-event-interruptingCorrelationKey"
	err = bpmnEngine.PublishMessageByName(t.Context(), "simple-boundary", correlationKey, variables)
	assert.NoError(t, err)
	waitForProcessCompletion(t, instance.ProcessInstance().Key, 2*time.Second, 50*time.Millisecond)
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
	taskHandler := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(taskHandler)
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)
	waitForProcessCompletion(t, instance.ProcessInstance().Key, 2*time.Second, 50*time.Millisecond)
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
	foundInstance := findChildSubProcessInstance(t, instance.ProcessInstance().Key)
	job := waitForPendingJob(t, foundInstance.ProcessInstance().Key)
	assert.Equal(t, variableContext[variableName], engineStorage.Jobs[job.Key].InputVariables["testInput"])
	err = bpmnEngine.JobCompleteByKey(t.Context(), job.Key, map[string]interface{}{
		"testJobOutput": "newJobVal",
	})
	assert.NoError(t, err)
	waitForProcessCompletion(t, instance.ProcessInstance().Key, 2*time.Second, 50*time.Millisecond)
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
	foundChildInstance := findChildSubProcessInstance(t, instance.ProcessInstance().Key)
	variables := map[string]interface{}{"payload": "message payload"}
	correlationKey := new(string)
	*correlationKey = "testMessage"
	err = bpmnEngine.PublishMessageByName(t.Context(), "OuterTestMessage", correlationKey, variables)
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
	foundChildInstance := findChildSubProcessInstance(t, instance.ProcessInstance().Key)
	// Wait for the inner boundary event subscription to be set up
	require.Eventually(t, func() bool {
		subs, subErr := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), foundChildInstance.ProcessInstance().Key, runtime.ActivityStateActive)
		return subErr == nil && len(subs) > 0
	}, 2*time.Second, 50*time.Millisecond)
	variables := map[string]interface{}{"payload": "message payload"}
	correlationKey := new(string)
	*correlationKey = "testMessage"
	err = bpmnEngine.PublishMessageByName(t.Context(), "InnerTestMessage", correlationKey, variables)
	assert.NoError(t, err)
	waitForProcessCompletion(t, instance.ProcessInstance().Key, 2*time.Second, 50*time.Millisecond)
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
	multiInstance := findChildMultiInstanceInstance(t, instance.ProcessInstance().Key)
	foundChildInstance := findChildSubProcessInstance(t, multiInstance.Key)
	// when
	variables := map[string]interface{}{"payload": "message payload"}
	correlationKey := new(string)
	*correlationKey = "1234"
	err = bpmnEngine.PublishMessageByName(t.Context(), "boundary message", correlationKey, variables)
	assert.NoError(t, err)
	require.Eventually(t, func() bool {
		pi, piErr := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
		return piErr == nil && pi.ProcessInstance().GetState() == runtime.ActivityStateCompleted
	}, 2*time.Second, 50*time.Millisecond)
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
	taskHandler := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(taskHandler)
	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2", "test3"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)
	waitForProcessCompletion(t, instance.ProcessInstance().Key, 500*time.Millisecond, 100*time.Millisecond)
	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.NotNil(t, instance, "Process instance needs to be present")
	assertOutputCollectionMatches(t, instance, []string{"test1newVal", "test2newVal", "test3newVal"})
	assertProcessCompletionWithSubProcess(t, instance)
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
	completeWorkerJobs(t, testInputCollection, "testJobOutput")
	waitForProcessCompletion(t, instance.ProcessInstance().Key, 500*time.Millisecond, 100*time.Millisecond)
	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assertOutputCollectionMatches(t, instance, []string{"test1newVal", "test2newVal", "test3newVal"})
	assertProcessCompletionWithSubProcess(t, instance)
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
	boundaryCorrelationKey := new(string)
	*boundaryCorrelationKey = "boundary-correlation-key"
	err = bpmnEngine.PublishMessageByName(t.Context(), "shared receive boundary message", boundaryCorrelationKey, nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		processInstance, findErr := engineStorage.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
		return findErr == nil && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted
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
	assertMultiInstanceReceiveTaskCompletion(t, instance, []string{"test1", "test2", "test3"})
}
func TestMultiInstanceReceiveTaskStartsAndCompletesOnSubscription(t *testing.T) {
	cleanUpMessageSubscriptions()
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/receive_task/multi_instance_receive_task.bpmn")
	assert.NoError(t, err)
	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2", "test3"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)
	assertActiveReceiveTaskSubscriptionsWaiting(t, []string{"test1"})
	completeMultiInstanceReceiveTaskBySubscription(t, []string{"test1", "test2", "test3"})
	assertMultiInstanceReceiveTaskCompletion(t, instance, []string{"test1", "test2", "test3"})
}
func TestMultiInstanceReceiveTaskSkipsWhenInputIsEmptyButFillsOutputWithEmptyList(t *testing.T) {
	cleanUpMessageSubscriptions()
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/receive_task/multi_instance_receive_task.bpmn")
	assert.NoError(t, err)
	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)
	assertMultiInstanceReceiveTaskSkipsWithEmptyOutput(t, instance)
}
func TestMultiInstanceParallelReceiveTaskStartsAndCompletesOnPublishedMessage(t *testing.T) {
	cleanUpMessageSubscriptions()
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/receive_task/multi_instance_parallel_receive_task.bpmn")
	assert.NoError(t, err)
	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2", "test3"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)
	assertActiveReceiveTaskSubscriptionsWaiting(t, []string{"test1", "test2", "test3"})
	completeMultiInstanceReceiveTaskByName(t, []string{"test1", "test2", "test3"})
	assertMultiInstanceReceiveTaskCompletion(t, instance, []string{"test1", "test2", "test3"})
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
	assertMultiInstanceReceiveTaskCompletion(t, instance, []string{"test1", "test2", "test3"})
}
func TestMultiInstanceParallelReceiveTaskSkipsWhenInputIsEmptyButFillsOutputWithEmptyList(t *testing.T) {
	cleanUpMessageSubscriptions()
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/receive_task/multi_instance_parallel_receive_task.bpmn")
	assert.NoError(t, err)
	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)
	assertMultiInstanceReceiveTaskSkipsWithEmptyOutput(t, instance)
}
func TestMultiInstanceParallelServiceTaskStartsAndCompletesWorkerJob(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_parallel_service_task.bpmn")
	assert.NoError(t, err)
	variableContext := make(map[string]interface{}, 1)
	testInputCollection := []string{"test1", "test2", "test3"}
	variableContext["testInputCollection"] = testInputCollection
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)
	completeWorkerJobs(t, testInputCollection, "testJobOutput")
	waitForProcessCompletion(t, instance.ProcessInstance().Key, 500*time.Millisecond, 100*time.Millisecond)
	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assertOutputCollectionMatches(t, instance, []string{"test1newVal", "test2newVal", "test3newVal"})
	assertProcessCompletionWithSubProcess(t, instance)
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
	taskHandler := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(taskHandler)
	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2", "test3"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)
	waitForProcessCompletion(t, instance.ProcessInstance().Key, 500*time.Millisecond, 100*time.Millisecond)
	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	// then
	assert.NotNil(t, instance, "Process instance needs to be present")
	assertOutputCollectionMatches(t, instance, []string{"test1newVal", "test2newVal", "test3newVal"})
	assertProcessCompletionWithSubProcess(t, instance)
}
func TestMultiInstanceBusinessRuleTaskStartsAndCompletesLocalJob(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_business_rule.bpmn")
	assert.NoError(t, err)
	loadCanAutoLiquidateDMN(t)
	variableContext := make(map[string]interface{}, 1)
	testInputCollection := []int{1000, 500000, 200}
	variableContext["testInputCollection"] = testInputCollection
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)
	waitForProcessCompletion(t, instance.ProcessInstance().Key, 500*time.Millisecond, 100*time.Millisecond)
	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []map[string]interface{}{{"canAutoLiquidate": true}, {"canAutoLiquidate": false}, {"canAutoLiquidate": true}}, instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection"))
	assertProcessCompletionWithSubProcess(t, instance)
}
func TestMultiInstanceParallelBusinessRuleTaskStartsAndCompletesLocalJob(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_parallel_business_rule.bpmn")
	assert.NoError(t, err)
	loadCanAutoLiquidateDMN(t)
	variableContext := make(map[string]interface{}, 1)
	testInputCollection := []int{1000, 500000, 200}
	variableContext["testInputCollection"] = testInputCollection
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)
	waitForProcessCompletion(t, instance.ProcessInstance().Key, 500*time.Millisecond, 100*time.Millisecond)
	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []map[string]interface{}{{"canAutoLiquidate": true}, {"canAutoLiquidate": false}, {"canAutoLiquidate": true}}, instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection"))
	assertProcessCompletionWithSubProcess(t, instance)
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
	waitForProcessCompletion(t, instance.ProcessInstance().Key, 500*time.Millisecond, 100*time.Millisecond)
	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.NotNil(t, instance, "Process instance needs to be present")
	assertOutputCollectionMatches(t, instance, []string{"test1newVal", "test2newVal", "test3newVal"})
	assertProcessCompletionWithSubProcess(t, instance)
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
	waitForProcessCompletion(t, instance.ProcessInstance().Key, 5000*time.Millisecond, 100*time.Millisecond)
	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.NotNil(t, instance, "Process instance needs to be present")
	assertOutputCollectionMatches(t, instance, []string{"test1newVal", "test2newVal", "test3newVal"})
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
		// Logging the flaky tests
		if counter > 5 {
			logFlakyTestDiagnostics()
		}
		counter++
		return false
	}, 5000*time.Millisecond, 100*time.Millisecond)
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
	waitForProcessCompletion(t, instance.ProcessInstance().Key, 500*time.Millisecond, 100*time.Millisecond)
	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.NotNil(t, instance, "Process instance needs to be present")
	assertOutputCollectionMatches(t, instance, []string{"test1newVal", "test2newVal", "test3newVal"})
	assertProcessCompletionWithSubProcess(t, instance)
}
func TestMultiInstanceSubProcessStartsAndCompletesJobByKey(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_sub_process_task.bpmn")
	assert.NoError(t, err)
	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)
	var foundMultiInstance runtime.MultiInstanceInstance
	assert.Eventually(t, func() bool {
		for _, pi := range engineStorage.ProcessInstancesSnapshot() {
			if pi.Type() == runtime.ProcessTypeMultiInstance && *pi.(*runtime.MultiInstanceInstance).GetParentProcessInstanceKey() == instance.ProcessInstance().Key {
				foundMultiInstance = *pi.(*runtime.MultiInstanceInstance)
				return true
			}
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	var foundInstance runtime.SubProcessInstance
	assert.Eventually(t, func() bool {
		for _, pi := range engineStorage.ProcessInstancesSnapshot() {
			if pi.Type() == runtime.ProcessTypeSubProcess && *pi.(*runtime.SubProcessInstance).GetParentProcessInstanceKey() == foundMultiInstance.Key {
				foundInstance = *pi.(*runtime.SubProcessInstance)
				return true
			}
		}
		return false
	}, 500*time.Millisecond, 100*time.Millisecond)
	findAndCompleteSubProcessJob(t, foundInstance.ProcessInstance().Key, 10*time.Second, map[string]interface{}{"testJobOutput": "newJobVal1"})
	assert.Eventually(t, func() bool {
		for _, pi := range engineStorage.ProcessInstancesSnapshot() {
			if pi.Type() == runtime.ProcessTypeSubProcess && foundInstance.ProcessInstance().Key != pi.ProcessInstance().Key && *pi.(*runtime.SubProcessInstance).GetParentProcessInstanceKey() == foundMultiInstance.Key {
				foundInstance = *pi.(*runtime.SubProcessInstance)
				return true
			}
		}
		return false
	}, 2000*time.Millisecond, 100*time.Millisecond)
	findAndCompleteSubProcessJob(t, foundInstance.ProcessInstance().Key, 10*time.Second, map[string]interface{}{"testJobOutput": "newJobVal2"})
	waitForProcessCompletion(t, instance.ProcessInstance().Key, 2*time.Second, 50*time.Millisecond)
	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.NotNil(t, instance, "Process instance needs to be present")
	assertOutputCollectionMatches(t, instance, []string{"newJobVal1", "newJobVal2"})
	assertProcessCompletionWithSubProcess(t, instance)
}
func TestMultiInstanceSubProcessCorrelateBoundaryEvent(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_sub_process_task.bpmn")
	assert.NoError(t, err)
	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)
	variables := map[string]interface{}{"payload": "message payload"}
	correlationKey := "1234"
	// Retry until the subscription is persisted and the publish succeeds
	require.Eventually(t, func() bool {
		return bpmnEngine.PublishMessageByName(t.Context(), "Event_1r7iviyMessage", &correlationKey, variables) == nil
	}, 2*time.Second, 50*time.Millisecond)
	// Find the multi-instance wrapper so we can scope sub-process searches to this test
	var foundMultiInstance runtime.MultiInstanceInstance
	require.Eventually(t, func() bool {
		for _, pi := range engineStorage.ProcessInstancesSnapshot() {
			if pi.Type() == runtime.ProcessTypeMultiInstance && *pi.(*runtime.MultiInstanceInstance).GetParentProcessInstanceKey() == instance.ProcessInstance().Key {
				foundMultiInstance = *pi.(*runtime.MultiInstanceInstance)
				return true
			}
		}
		return false
	}, 2*time.Second, 50*time.Millisecond)
	var foundChildInstance runtime.ProcessInstance
	require.Eventually(t, func() bool {
		for _, pi := range engineStorage.ProcessInstancesSnapshot() {
			if pi.Type() == runtime.ProcessTypeSubProcess &&
				pi.ProcessInstance().State == runtime.ActivityStateCompleted &&
				*pi.(*runtime.SubProcessInstance).GetParentProcessInstanceKey() == foundMultiInstance.ProcessInstance().Key {
				foundChildInstance = pi
				return true
			}
		}
		return false
	}, 2*time.Second, 50*time.Millisecond)
	correlationKey = "1234"
	// Retry until the next subscription is persisted and the publish succeeds
	require.Eventually(t, func() bool {
		return bpmnEngine.PublishMessageByName(t.Context(), "Event_1r7iviyMessage", &correlationKey, variables) == nil
	}, 2*time.Second, 50*time.Millisecond)
	assert.Eventually(t, func() bool {
		pi, piErr := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
		return piErr == nil && pi.ProcessInstance().GetState() == runtime.ActivityStateCompleted
	}, 2*time.Second, 50*time.Millisecond)
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
	waitForProcessCompletion(t, instance.ProcessInstance().Key, 5000*time.Millisecond, 100*time.Millisecond)
	instance, err = bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.NotNil(t, instance, "Process instance needs to be present")
	assertOutputCollectionMatches(t, instance, []string{"test1newVal", "test2newVal", "test3newVal"})
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
		// Logging the flaky tests
		if counter > 5 {
			logFlakyTestDiagnostics()
		}
		counter++
		return false
	}, 5000*time.Millisecond, 100*time.Millisecond)
}
func TestMultiInstanceParallelSubProcessCorrelateBoundaryEventFailsToCreateParallelInstancesWithSameMessage(t *testing.T) {
	engineStorage.Incidents = make(map[int64]runtime.Incident)
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/multi_instance_parallel_sub_process_task.bpmn")
	assert.NoError(t, err)
	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{"test1", "test2", "test3"}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)
	variables := map[string]interface{}{"payload": "message payload"}
	correlationKey := "1324"
	// Retry until the subscription is persisted and the publish succeeds
	require.Eventually(t, func() bool {
		return bpmnEngine.PublishMessageByName(t.Context(), "Event_0g0g0nbMessage", &correlationKey, variables) == nil
	}, 2*time.Second, 50*time.Millisecond)
	var foundChildInstance runtime.ProcessInstance
	assert.Eventually(t, func() bool {
		for _, pi := range engineStorage.ProcessInstancesSnapshot() {
			if pi.Type() == runtime.ProcessTypeSubProcess && pi.ProcessInstance().State == runtime.ActivityStateCompleted {
				foundChildInstance = pi
				return true
			}
		}
		return false
	}, 2*time.Second, 50*time.Millisecond)
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
	// Iterate over a locked snapshot: engine goroutines mutate the storage map concurrently,
	// so ranging over the raw map here causes "concurrent map iteration and map write".
	for _, p := range engineStorage.Copy().ProcessInstances {
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
	runMultiInstanceSkipEmptyInputTest(t, "./test-cases/multi_instance_service_task.bpmn")
}
func TestMultiInstanceParallelSkipsWhenInputIsEmptyButFillsOutputWithEmptyList(t *testing.T) {
	runMultiInstanceSkipEmptyInputTest(t, "./test-cases/multi_instance_parallel_service_task.bpmn")
}

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
func assertActiveReceiveTaskSubscriptionsWaiting(t *testing.T, expectedCorrelationKeys []string) {
	t.Helper()
	expected := map[string]struct{}{}
	for _, key := range expectedCorrelationKeys {
		expected[key] = struct{}{}
	}
	require.Eventually(t, func() bool {
		seen := map[string]struct{}{}
		for _, message := range engineStorage.MessageSubscriptions {
			sub, ok := message.(*runtime.TokenMessageSubscription)
			if !ok || sub.Name != "receive task message" || sub.State != runtime.ActivityStateActive {
				continue
			}
			if _, ok := expected[sub.CorrelationKey]; !ok {
				continue
			}
			persistedToken, ok := engineStorage.ExecutionTokens[sub.Token.Key]
			if !ok || persistedToken.State != runtime.TokenStateWaiting || sub.Token.State != runtime.TokenStateWaiting {
				return false
			}
			seen[sub.CorrelationKey] = struct{}{}
		}
		return len(seen) == len(expected)
	}, 2*time.Second, 50*time.Millisecond)
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
func assertMultiInstanceReceiveTaskCompletion(t *testing.T, instance runtime.ProcessInstance, expectedItems []string) {
	t.Helper()
	assert.Eventually(t, func() bool {
		processInstance, findErr := engineStorage.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
		return findErr == nil && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted
	}, 2*time.Second, 50*time.Millisecond)
	updatedInstance, err := bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.NotNil(t, updatedInstance, "Process instance needs to be present")
	assertOutputCollectionMatches(t, updatedInstance, expectedMultiInstanceReceiveTaskOutput(expectedItems))
	assert.Equal(t, runtime.ActivityStateCompleted, updatedInstance.ProcessInstance().State)
	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), updatedInstance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))
	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), updatedInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))
	subProcesses, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(subProcesses))
	assert.Equal(t, runtime.ActivityStateCompleted, subProcesses[0].ProcessInstance().State)
	assertMultiInstanceReceiveTaskInputVariables(t, subProcesses[0].ProcessInstance().Key, expectedItems)
}
func assertMultiInstanceReceiveTaskSkipsWithEmptyOutput(t *testing.T, instance runtime.ProcessInstance) {
	t.Helper()
	assert.Eventually(t, func() bool {
		processInstance, findErr := engineStorage.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
		return findErr == nil && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted
	}, 2*time.Second, 50*time.Millisecond)
	updatedInstance, err := bpmnEngine.FindProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.NotNil(t, updatedInstance, "Process instance needs to be present")
	assert.Equal(t, 0, len(updatedInstance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").([]interface{})))
	assert.Equal(t, runtime.ActivityStateCompleted, updatedInstance.ProcessInstance().State)
	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), updatedInstance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions))
	tokens, err := bpmnEngine.persistence.GetCompletedTokensForProcessInstance(t.Context(), updatedInstance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tokens))
	subProcesses, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), tokens[0].Key)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subProcesses))
}
func assertProcessCompletionWithSubProcess(t *testing.T, instance runtime.ProcessInstance) {
	t.Helper()
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

// findChildCallActivityInstance waits for and returns the child CallActivity instance under the given parent.
func findChildCallActivityInstance(t *testing.T, parentInstanceKey int64) runtime.CallActivityInstance {
	t.Helper()
	var found runtime.CallActivityInstance
	require.Eventually(t, func() bool {
		for _, pi := range engineStorage.ProcessInstancesSnapshot() {
			if pi.Type() != runtime.ProcessTypeCallActivity {
				continue
			}
			if pi.(*runtime.CallActivityInstance).ParentProcessExecutionToken.ProcessInstanceKey == parentInstanceKey {
				found = *pi.(*runtime.CallActivityInstance)
				return true
			}
		}
		return false
	}, 2*time.Second, 50*time.Millisecond)
	return found
}

// findChildSubProcessInstance waits for and returns the child SubProcess instance under the given parent.
func findChildSubProcessInstance(t *testing.T, parentInstanceKey int64) runtime.SubProcessInstance {
	t.Helper()
	var found runtime.SubProcessInstance
	require.Eventually(t, func() bool {
		for _, pi := range engineStorage.ProcessInstancesSnapshot() {
			if pi.Type() != runtime.ProcessTypeSubProcess {
				continue
			}
			if pi.(*runtime.SubProcessInstance).ParentProcessExecutionToken.ProcessInstanceKey == parentInstanceKey {
				found = *pi.(*runtime.SubProcessInstance)
				return true
			}
		}
		return false
	}, 2*time.Second, 50*time.Millisecond)
	return found
}

// findChildMultiInstanceInstance waits for and returns the child MultiInstance under the given parent.
func findChildMultiInstanceInstance(t *testing.T, parentInstanceKey int64) runtime.MultiInstanceInstance {
	t.Helper()
	var found runtime.MultiInstanceInstance
	require.Eventually(t, func() bool {
		for _, pi := range engineStorage.ProcessInstancesSnapshot() {
			if pi.Type() != runtime.ProcessTypeMultiInstance {
				continue
			}
			if pi.(*runtime.MultiInstanceInstance).ParentProcessExecutionToken.ProcessInstanceKey == parentInstanceKey {
				found = *pi.(*runtime.MultiInstanceInstance)
				return true
			}
		}
		return false
	}, 2*time.Second, 50*time.Millisecond)
	return found
}

// waitForProcessCompletion asserts that a process instance reaches Completed state within the given timeout.
func waitForProcessCompletion(t *testing.T, instanceKey int64, timeout, interval time.Duration) {
	t.Helper()
	assert.Eventually(t, func() bool {
		processInstance, findErr := engineStorage.FindProcessInstanceByKey(t.Context(), instanceKey)
		return findErr == nil && processInstance.ProcessInstance().State == runtime.ActivityStateCompleted
	}, timeout, interval)
}

// waitForPendingJob waits for and returns the first pending job for the given process instance.
func waitForPendingJob(t *testing.T, instanceKey int64) runtime.Job {
	t.Helper()
	var job runtime.Job
	require.Eventually(t, func() bool {
		jobs, jobErr := bpmnEngine.persistence.FindPendingProcessInstanceJobs(t.Context(), instanceKey)
		if jobErr != nil || len(jobs) == 0 {
			return false
		}
		job = jobs[0]
		return true
	}, 2*time.Second, 50*time.Millisecond)
	return job
}

// assertOutputCollectionMatches verifies the testOutputCollection variable against expected string values.
func assertOutputCollectionMatches(t *testing.T, instance runtime.ProcessInstance, expected []string) {
	t.Helper()
	testOutput := make([]string, 0)
	for _, str := range instance.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection").([]interface{}) {
		testOutput = append(testOutput, str.(string))
	}
	assert.ElementsMatch(t, expected, testOutput)
}

// completeWorkerJobs completes one worker job per item in inputCollection, setting outputVarName = item+"newVal".
func completeWorkerJobs(t *testing.T, inputCollection []string, outputVarName string) {
	t.Helper()
	for _, str := range inputCollection {
		var job runtime.Job
		require.Eventually(t, func() bool {
			jobs, jobErr := bpmnEngine.persistence.FindActiveJobsByType(t.Context(), "TestType")
			if jobErr != nil || len(jobs) == 0 {
				return false
			}
			job = jobs[0]
			return true
		}, 2*time.Second, 50*time.Millisecond)
		err := bpmnEngine.JobCompleteByKey(t.Context(), job.Key, map[string]interface{}{
			outputVarName: str + "newVal",
		})
		assert.NoError(t, err)
	}
}

// loadCanAutoLiquidateDMN loads and saves the can-autoliquidate-rule DMN definition.
func loadCanAutoLiquidateDMN(t *testing.T) {
	t.Helper()
	definition, xmldata, err := bpmnEngine.dmnEngine.ParseDmnFromFile(filepath.Join("..", "dmn", "test-data", "bulk-evaluation-test", "can-autoliquidate-rule.dmn"))
	assert.NoError(t, err)
	_, _, err = bpmnEngine.dmnEngine.SaveDmnResourceDefinition(t.Context(), definition, xmldata, bpmnEngine.generateKey())
	assert.NoError(t, err)
}

// logFlakyTestDiagnostics prints engine state for debugging flaky parallel tests.
func logFlakyTestDiagnostics() {
	for _, s := range engineStorage.ProcessInstancesSnapshot() {
		println(s.ProcessInstance().Key)
		println(s.ProcessInstance().State.String())
		println(fmt.Sprint(s.ProcessInstance().VariableHolder.GetLocalVariable("testOutputCollection")))
	}
	for _, token := range engineStorage.ExecutionTokens {
		println(token.Key)
		println(token.State.String())
		println(token.ElementId)
	}
}

// runMultiInstanceSkipEmptyInputTest is the shared body for empty-collection skip tests.
func runMultiInstanceSkipEmptyInputTest(t *testing.T, bpmnFile string) {
	t.Helper()
	process, err := bpmnEngine.LoadFromFile(t.Context(), bpmnFile)
	assert.NoError(t, err)
	variableName := "testJobOutput"
	taskId := "Activity_0rae016"
	handler := func(job ActivatedJob) {
		v := job.GetLocalVariables()["testElementInput"].(string)
		job.SetOutputVariable(variableName, v+"newVal")
		job.Complete()
	}
	taskHandler := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(taskHandler)
	variableContext := make(map[string]interface{}, 1)
	variableContext["testInputCollection"] = []string{}
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)
	assertMultiInstanceReceiveTaskSkipsWithEmptyOutput(t, instance)
}

// findAndCompleteSubProcessJob waits for a pending job on the given instance and completes it with the provided outputs.
func findAndCompleteSubProcessJob(t *testing.T, instanceKey int64, timeout time.Duration, outputs map[string]interface{}) {
	t.Helper()
	var jobs []runtime.Job
	assert.Eventually(t, func() bool {
		jobs, _ = bpmnEngine.persistence.FindPendingProcessInstanceJobs(t.Context(), instanceKey)
		return len(jobs) == 1
	}, timeout, 100*time.Millisecond)
	assert.Equal(t, 1, len(jobs), "There should be one job")
	err := bpmnEngine.JobCompleteByKey(t.Context(), jobs[0].Key, outputs)
	assert.NoError(t, err)
}
