package bpmn

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"

	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type CallPath struct {
	CallPath string
}

func (callPath *CallPath) TaskHandler(job ActivatedJob) {
	if len(callPath.CallPath) > 0 {
		callPath.CallPath += ","
	}
	callPath.CallPath += job.ElementId()
	job.Complete()
}

var bpmnEngine Engine
var engineStorage *inmemory.Storage

func TestMain(m *testing.M) {
	engineStorage = inmemory.NewStorage()

	var exitCode int

	defer func() {
		os.Exit(exitCode)
	}()

	bpmnEngine = NewEngine(EngineWithStorage(engineStorage))
	if err := bpmnEngine.Start(context.Background()); err != nil {
		fmt.Printf("failed to start bpmn engine: %s\n", err)
		exitCode = 1
		return
	}

	// Run the tests
	exitCode = m.Run()
}

func TestRegisterHandlerByTaskIdGetsCalled(t *testing.T) {
	// setup
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
	assert.NoError(t, err)
	wasCalled := false
	handler := func(job ActivatedJob) {
		wasCalled = true
		job.Complete()
	}

	// given
	idH := bpmnEngine.NewTaskHandler().Id("id").Handler(handler)
	defer bpmnEngine.RemoveHandler(idH)

	// when
	_, err = bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	// then
	assert.True(t, wasCalled)
}

func TestRegisterHandlerByTaskIdGetsCalledAfterLateRegister(t *testing.T) {
	t.Skip("runtime modification of handlers is not supported yet")
	// setup
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
	assert.NoError(t, err)
	wasCalled := false
	handler := func(job ActivatedJob) {
		wasCalled = true
		job.Complete()
	}
	// // given
	pi, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	if err != nil {
		t.Fatal(err)
	}
	idH := bpmnEngine.NewTaskHandler().Id("id").Handler(handler)
	defer bpmnEngine.RemoveHandler(idH)

	tokens, err := bpmnEngine.persistence.GetActiveTokensForProcessInstance(t.Context(), pi.ProcessInstance().Key)
	assert.NoError(t, err)
	err = bpmnEngine.RunProcessInstance(t.Context(), pi, tokens)
	assert.NoError(t, err)

	// when
	assert.True(t, wasCalled)
}

func TestRegisteredHandlerCanMutateVariableContext(t *testing.T) {
	// setup
	variableName := "variable_name"
	taskId := "id"
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
	assert.NoError(t, err)
	variableContext := make(map[string]interface{}, 1)
	variableContext[variableName] = "oldVal"

	handler := func(job ActivatedJob) {
		v := job.Variable(variableName)
		assert.Equal(t, "oldVal", v, "one should be able to read variables")
		job.SetOutputVariable(variableName, "newVal")
		job.Complete()
	}

	// given
	taskHandler := bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)
	defer bpmnEngine.RemoveHandler(taskHandler)

	// when
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	v := engineStorage.ProcessInstances[instance.ProcessInstance().Key]
	// then
	assert.NotNil(t, v, "Process instance needs to be present")
	assert.Equal(t, "newVal", v.ProcessInstance().VariableHolder.GetLocalVariable(variableName))
}

func TestMetadataIsGivenFromLoadedXmlFile(t *testing.T) {
	// setup
	metadata, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
	assert.NoError(t, err)

	assert.Equal(t, int32(1), metadata.Version)
	assert.Greater(t, metadata.Key, int64(1))
	assert.Equal(t, "Simple_Task_Process", metadata.BpmnProcessId)
}

func TestLoadingTheSameFileWillNotIncreaseTheVersionNorChangeTheProcessKey(t *testing.T) {
	// setup
	metadata, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
	assert.NoError(t, err)
	keyOne := metadata.Key
	assert.Equal(t, int32(1), metadata.Version)

	metadata, err = bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
	assert.NoError(t, err)
	keyTwo := metadata.Key
	assert.Equal(t, int32(1), metadata.Version)
	assert.Equal(t, keyTwo, keyOne)
}

func TestLoadingTheSameProcessWithModificationWillCreateNewVersion(t *testing.T) {
	// setup
	process1, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
	assert.NoError(t, err)
	process2, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple_task_modified_taskId.bpmn")
	assert.NoError(t, err)
	process3, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
	assert.NoError(t, err)

	assert.Equal(t, process1.BpmnProcessId, process2.BpmnProcessId, "both prepared files should have equal IDs")
	assert.Equal(t, int32(1), process1.Version)
	assert.Equal(t, int32(2), process2.Version)
	assert.Equal(t, int32(3), process3.Version)

	assert.NotEqual(t, process2.Key, process1.Key)
}

func TestInstanceCanStartAtChosenFlowNode(t *testing.T) {
	cp := CallPath{}

	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/forked-flow.bpmn")
	assert.NoError(t, err)
	a1H := bpmnEngine.NewTaskHandler().Id("id-a-1").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(a1H)
	b1H := bpmnEngine.NewTaskHandler().Id("id-b-1").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(b1H)
	b2H := bpmnEngine.NewTaskHandler().Id("id-b-2").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(b2H)

	startingElementIds := []string{"id-b-1", "id-b-2"}
	_, err = bpmnEngine.CreateInstanceWithStartingElements(t.Context(), process.Key, startingElementIds, nil, nil)
	assert.NoError(t, err)

	assert.Equal(t, "id-b-1,id-b-2", cp.CallPath)
}

func TestMultipleInstancesCanBeCreated(t *testing.T) {
	// setup
	beforeCreation := time.Now()

	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
	assert.NoError(t, err)

	// when
	instance1, err := bpmnEngine.CreateInstance(t.Context(), process, nil)
	assert.NoError(t, err)
	instance2, err := bpmnEngine.CreateInstance(t.Context(), process, nil)
	assert.NoError(t, err)

	// then
	assert.GreaterOrEqual(t, instance1.ProcessInstance().CreatedAt.UnixNano(), beforeCreation.UnixNano(), "make sure we have creation time set")
	assert.Equal(t, instance2.ProcessInstance().Definition.Key, instance1.ProcessInstance().Definition.Key)
}

func TestSimpleAndUncontrolledForkingTwoTasks(t *testing.T) {
	// setup
	cp := CallPath{}

	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/forked-flow.bpmn")
	assert.NoError(t, err)
	a1H := bpmnEngine.NewTaskHandler().Id("id-a-1").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(a1H)
	b1H := bpmnEngine.NewTaskHandler().Id("id-b-1").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(b1H)
	b2H := bpmnEngine.NewTaskHandler().Id("id-b-2").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(b2H)

	// when
	_, err = bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	// then
	assert.Equal(t, "id-a-1,id-b-1,id-b-2", cp.CallPath)
}

func TestParallelGateWayTwoTasks(t *testing.T) {
	// setup
	cp := CallPath{}

	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/parallel-gateway-flow.bpmn")
	assert.NoError(t, err)
	a1H := bpmnEngine.NewTaskHandler().Id("id-a-1").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(a1H)
	b1H := bpmnEngine.NewTaskHandler().Id("id-b-1").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(b1H)
	b2H := bpmnEngine.NewTaskHandler().Id("id-b-2").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(b2H)

	// when
	_, err = bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	// then
	assert.Equal(t, "id-a-1,id-b-1,id-b-2", cp.CallPath)
}

func TestHasActiveSubProcessInstanceIncludesReadyChild(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	parentToken := runtime.ExecutionToken{
		Key:                engine.generateKey(),
		ElementInstanceKey: engine.generateKey(),
		ElementId:          "parent-element",
		ProcessInstanceKey: engine.generateKey(),
		State:              runtime.TokenStateWaiting,
	}
	assert.NoError(t, store.SaveToken(t.Context(), parentToken))
	assert.NoError(t, store.SaveProcessInstance(t.Context(), &runtime.SubProcessInstance{
		ParentProcessExecutionToken: parentToken,
		ProcessInstanceData: runtime.ProcessInstanceData{
			Key:   engine.generateKey(),
			State: runtime.ActivityStateReady,
		},
	}))

	hasActiveChild, err := engine.hasActiveSubProcessInstance(t.Context(), parentToken.ProcessInstanceKey)
	assert.NoError(t, err)
	assert.True(t, hasActiveChild, "a persisted READY child must keep its parent alive until execution starts")
}

func TestMultipleEnginesCreateUniqueIds(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine1 := NewEngine(EngineWithStorage(store))
	store2 := inmemory.NewStorage()
	bpmnEngine2 := NewEngine(EngineWithStorage(store2))

	// when
	process1, err := bpmnEngine1.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
	assert.NoError(t, err)
	process2, err := bpmnEngine2.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
	assert.NoError(t, err)

	// then
	assert.NotEqual(t, process2.Key, process1.Key)
}

func TestCreateInstanceByIdUsesLatestProcessVersion(t *testing.T) {
	// when
	v1, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
	assert.NoError(t, err)
	assert.Equal(t, "aName", v1.Definitions.Process.Name)
	// when
	v2, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple_task_v2.bpmn")
	assert.NoError(t, err)
	assert.Equal(t, "aName", v2.Definitions.Process.Name)

	instance, err := bpmnEngine.CreateInstanceById(t.Context(), "Simple_Task_Process", nil)
	assert.NoError(t, err)
	assert.NotNil(t, instance)
	assert.Equal(t, v2.Version, instance.ProcessInstance().Definition.Version)
}

func TestCreateAndRunInstanceByIdUsesLatestProcessVersion(t *testing.T) {
	// when
	v1, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
	assert.NoError(t, err)
	assert.Equal(t, "aName", v1.Definitions.Process.Name)
	// when
	v2, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple_task_v2.bpmn")
	assert.NoError(t, err)
	assert.Equal(t, "aName", v2.Definitions.Process.Name)

	instance, err := bpmnEngine.CreateInstanceById(t.Context(), "Simple_Task_Process", nil)
	assert.NoError(t, err)
	assert.NotNil(t, instance)

	// then
	assert.Equal(t, v2.Version, instance.ProcessInstance().Definition.Version)
}

func TestCreateInstanceByIdReturnErrorWhenNoIDFound(t *testing.T) {
	// when
	instance, err := bpmnEngine.CreateInstanceById(t.Context(), "Simple_Task_Process_not_existing", nil)

	// then
	assert.Nil(t, instance)
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "no process definition with id=Simple_Task_Process_not_existing was found (prior loaded into the engine)"))
}

func TestCancelInstanceShouldCancelInstance(t *testing.T) {
	// setup
	_, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
	assert.NoError(t, err)
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/call-activity-with-multiple-boundary.bpmn")
	assert.NoError(t, err)

	variableContext := make(map[string]interface{}, 1)
	randomCorrelationKey := rand.Int63()
	variableContext["correlationKey"] = fmt.Sprint(randomCorrelationKey)

	// when
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, variableContext)
	assert.NoError(t, err)

	time.Sleep(2 * time.Second)

	err = bpmnEngine.CancelInstanceByKey(t.Context(), instance.ProcessInstance().GetInstanceKey())
	assert.NoError(t, err)

	// then

	// All message subscriptions should be canceled
	subscriptions, err := bpmnEngine.persistence.FindProcessInstanceMessageSubscriptions(t.Context(), instance.ProcessInstance().Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions), "expected 0 message subscriptions, but found %d", len(subscriptions))

	// All timers should be canceled
	timers, err := bpmnEngine.persistence.FindProcessInstanceTimers(t.Context(), instance.ProcessInstance().Key, runtime.TimerStateCreated)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(timers), "expected 0 timers, but found %d", len(timers))

	// All jobs should be canceled
	jobs, err := bpmnEngine.persistence.FindPendingProcessInstanceJobs(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(jobs), "expected 0 jobs, but found %d", len(jobs))

	// All incidents should be resolved
	// TODO: would need different test

	// All called processes should be terminated
	tokens, err := bpmnEngine.persistence.GetActiveTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)

	for _, token := range tokens {
		cps, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), token.Key)
		assert.NoError(t, err)

		for _, cp := range cps {
			assert.Equal(t, runtime.ActivityStateTerminated, cp.ProcessInstance().State, "expected cancelled state for terminated process, but found %s", cp.ProcessInstance().State)
		}
	}

	// Cancel process instance
	pi, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateTerminated, pi.ProcessInstance().State, "expected canceled state for process instance, but found %s", pi.ProcessInstance().State)

}

func TestProcessInstanceMustBeInActiveStateForCreateInstanceByKey(t *testing.T) {
	// setup
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/parallel_flow_with_terminate_end_task.bpmn")
	assert.NoError(t, err)

	// when
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, make(map[string]interface{}))
	assert.NoError(t, err)

	err = bpmnEngine.CancelInstanceByKey(t.Context(), instance.ProcessInstance().GetInstanceKey())
	assert.ErrorContains(t, err, "cannot cancel process instance")
	assert.ErrorContains(t, err, "it is not in correct state, expected=ActivityStateActive, actual=ActivityStateCompleted")
}

func TestModifyProcessInstance(t *testing.T) {
	// setup
	_, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
	assert.NoError(t, err)
	definition, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/call-activity-with-multiple-boundary-user-task-end.bpmn")
	assert.NoError(t, err)

	variableContext := make(map[string]interface{}, 1)
	randomCorrelationKey := rand.Int63()
	variableContext["correlationKey"] = fmt.Sprint(randomCorrelationKey)

	// when
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), definition.Key, variableContext)
	assert.NoError(t, err)

	// wait for activity instance to be created (TODO: the fact that this needs to be here is an issue)
	assert.Eventually(t, func() bool {
		inMem := bpmnEngine.persistence.(*inmemory.Storage)
		for _, inst := range inMem.ProcessInstances {

			if activityInstance, ok := inst.(*runtime.CallActivityInstance); ok {
				// wait till instance is already created
				if activityInstance.ParentProcessExecutionToken.ProcessInstanceKey == instance.ProcessInstance().Key {
					return true
				}
			}
		}
		return false
	}, 5000*time.Millisecond, 200*time.Millisecond)

	var executionTokens []runtime.ExecutionToken
	assert.Eventually(t, func() bool {
		executionTokens, err = bpmnEngine.persistence.GetActiveTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
		assert.NoError(t, err)
		if executionTokens != nil && len(executionTokens) == 1 {
			return true
		}
		return false
	}, 5*time.Second, 200*time.Millisecond)

	time.Sleep(2 * time.Second)

	var mainToken runtime.ExecutionToken
	for _, token := range executionTokens {
		if token.ElementId == "callActivity" {
			mainToken = token
		}
	}
	assert.NotEqual(t, "", mainToken.Key)

	elementInstancesToTerminate := make([]int64, 0, 1)
	elementInstancesToTerminate = append(elementInstancesToTerminate, mainToken.ElementInstanceKey)
	elementIdsToStartInstance := make([]string, 0, 1)
	elementIdsToStartInstance = append(elementIdsToStartInstance, "userTask")

	modifiedInstance, runningTokens, err := bpmnEngine.ModifyInstance(t.Context(), instance.ProcessInstance().GetInstanceKey(), elementInstancesToTerminate, elementIdsToStartInstance, map[string]any{
		"order": map[string]any{"name": "test-order-name"}})

	time.Sleep(2 * time.Second)

	assert.NoError(t, err)
	assert.Equal(t, definition.Key, modifiedInstance.ProcessInstance().Definition.Key)
	assert.Equal(t, map[string]any{"name": "test-order-name"}, instance.ProcessInstance().VariableHolder.LocalVariables()["order"])
	assert.NotEmpty(t, runningTokens)
	assert.Equal(t, 1, len(runningTokens))
	assert.NotEmpty(t, runningTokens[0].Key)
	assert.Equal(t, runningTokens[0].ElementId, "userTask")
	assert.Equal(t, runningTokens[0].ProcessInstanceKey, instance.ProcessInstance().Key)

	instanceCheck, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, definition.Key, instanceCheck.ProcessInstance().Definition.Key)
	assert.Equal(t, map[string]any{"name": "test-order-name"}, instanceCheck.ProcessInstance().VariableHolder.LocalVariables()["order"])

	// All message subscriptions should be canceled
	subscriptions, err := bpmnEngine.persistence.FindTokenMessageSubscriptions(t.Context(), mainToken.Key, runtime.ActivityStateActive)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(subscriptions), "expected 0 message subscriptions, but found %d", len(subscriptions))

	// All timers should be canceled
	timers, err := bpmnEngine.persistence.FindTokenActiveTimerSubscriptions(t.Context(), mainToken.Key)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(timers), "expected 0 timers, but found %d", len(timers))

	// All jobs should be canceled
	jobs, err := bpmnEngine.persistence.GetJobsInStateByTokenKey(t.Context(), mainToken.Key, []runtime.ActivityState{runtime.ActivityStateActive, runtime.ActivityStateCompleting, runtime.ActivityStateFailed})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(jobs), "expected 0 jobs, but found %d", len(jobs))

	// All incidents should be resolved
	// TODO: would need different test

	time.Sleep(2 * time.Second)

	// All called processes should be terminated
	cps, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), mainToken.Key)
	assert.NoError(t, err)

	for _, cp := range cps {
		assert.Equal(t, runtime.ActivityStateTerminated, cp.ProcessInstance().State, "expected cancelled state for terminated definition, but found %s", cp.ProcessInstance().State)
	}
}

func TestEventBasedGatewaySelectsMessagePath(t *testing.T) {
	// setup
	cp := CallPath{}

	// given
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/message-intermediate-timer-event.bpmn")
	assert.NoError(t, err)
	mH := bpmnEngine.NewTaskHandler().Id("task-for-message").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(mH)
	tH := bpmnEngine.NewTaskHandler().Id("task-for-timer").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(tH)
	_, err = bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	// when
	for _, message := range engineStorage.MessageSubscriptions {
		if message.MessageSubscription().Name == "message" {
			err = bpmnEngine.PublishMessage(t.Context(), message, nil)
			assert.NoError(t, err)
		}
	}

	// then
	assert.Equal(t, "task-for-message", cp.CallPath)
}

// Also tests Binding Type - VersionTag and Latest
// TODO: Fix this test after implementing support for nested variables
func TestBusinessRuleTaskInternalInputOutputExecutionCompleted(t *testing.T) {
	//setup
	process, err := bpmnEngine.LoadFromFile(t.Context(), filepath.Join(".", "test-cases", "business_rule/simple-business-rule-task-local.bpmn"))
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

	//run
	instance, _ := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)

	assert.NotEmpty(t, instance.ProcessInstance().VariableHolder.LocalVariables())
	assert.Equal(t, true, instance.ProcessInstance().VariableHolder.LocalVariables()["OutputTestResultVariable"])
	assert.NotContains(t, instance.ProcessInstance().VariableHolder.LocalVariables(), "testResultVariable")
	assert.Nil(t, instance.ProcessInstance().VariableHolder.LocalVariables()["testResultVariable2"])
	assert.NotContains(t, instance.ProcessInstance().VariableHolder.LocalVariables(), "testResultVariable3")

	flowElementInstances, err := bpmnEngine.persistence.GetFlowElementInstancesByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key, false)
	assert.NoError(t, err)
	expectedFlowElementOutputs := map[string]map[string]any{
		"BusinessRuleTask1": {},
		"BusinessRuleTask2": {"OutputTestResultVariable": true},
		"BusinessRuleTask3": {},
	}
	seenFlowElementOutputs := map[string]bool{}
	for _, flowElementInstance := range flowElementInstances {
		expectedOutput, ok := expectedFlowElementOutputs[flowElementInstance.ElementId]
		if !ok {
			continue
		}
		seenFlowElementOutputs[flowElementInstance.ElementId] = true
		assert.Equal(t, expectedOutput, flowElementInstance.OutputVariables)
	}
	for elementId := range expectedFlowElementOutputs {
		assert.True(t, seenFlowElementOutputs[elementId], "expected flow element output snapshot for %s", elementId)
	}

	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)
}

func TestExclusiveGatewaySequenceFlowSavedInHistory(t *testing.T) {
	// This test verifies that sequence flows after exclusive gateways are saved in flow element history.
	// See: https://github.com/pbinitiative/zenbpm/issues/370
	//
	// The bug: Gateway handlers (exclusive, inclusive, parallel) selected the outgoing flow and moved
	// the token, but never called SaveFlowElementInstance() for the sequence flow itself.
	// This caused the flow to be missing from the process instance history.

	// setup - load a process with exclusive gateway
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/exclusive-gateway-with-condition-and-default.bpmn")
	assert.NoError(t, err)

	// Register handlers for the tasks
	taskACalled := false
	handlerA := bpmnEngine.NewTaskHandler().Type("task-a").Handler(func(job ActivatedJob) {
		taskACalled = true
		job.Complete()
	})
	defer bpmnEngine.RemoveHandler(handlerA)

	taskBCalled := false
	handlerB := bpmnEngine.NewTaskHandler().Type("task-b").Handler(func(job ActivatedJob) {
		taskBCalled = true
		job.Complete()
	})
	defer bpmnEngine.RemoveHandler(handlerB)

	// when - create instance with price > 0 (should take price-gt-zero flow to task-a)
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, map[string]interface{}{
		"price": 100,
	})
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)
	assert.True(t, taskACalled, "task-a should have been called")
	assert.False(t, taskBCalled, "task-b should NOT have been called")

	// then - check that the sequence flow from gateway to task-a is in history
	flowElements, err := bpmnEngine.persistence.GetFlowElementInstancesByProcessInstanceKey(
		t.Context(),
		instance.ProcessInstance().Key,
		true, // order by time created
	)
	assert.NoError(t, err)

	// Find all element IDs in history
	elementIds := make([]string, len(flowElements))
	flowElementsByID := make(map[string]runtime.FlowElementInstance, len(flowElements))
	for i, fe := range flowElements {
		elementIds[i] = fe.ElementId
		flowElementsByID[fe.ElementId] = fe
	}

	// The sequence flow "price-gt-zero" (from gateway to task-a) MUST be in history
	assert.Contains(t, elementIds, "price-gt-zero",
		"Sequence flow 'price-gt-zero' after exclusive gateway should be saved in history. Got: %v", elementIds)

	// Also verify the flow from start to gateway is there
	assert.Contains(t, elementIds, "Flow_1y8jegt",
		"Sequence flow 'Flow_1y8jegt' (start to gateway) should be in history. Got: %v", elementIds)

	for _, elementID := range []string{"Gateway_01wr5g0", "Flow_1y8jegt", "price-gt-zero"} {
		flowElement, ok := flowElementsByID[elementID]
		assert.True(t, ok, "element %s should be in history", elementID)
		assert.NotNil(t, flowElement.CompletedAt, "element %s should have CompletedAt after the gateway transition", elementID)
	}
}

// TestFlowElementInstanceCompletedAt verifies that CompletedAt is populated for every
// flow element once execution has passed it, including synchronous elements.
func TestFlowElementInstanceCompletedAt(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple_task.bpmn")
	assert.NoError(t, err)

	handler := bpmnEngine.NewTaskHandler().Type("TestType").Handler(func(job ActivatedJob) {
		job.SetOutputVariable("variable_name", "done")
		job.Complete()
	})
	defer bpmnEngine.RemoveHandler(handler)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	flowElements, err := bpmnEngine.persistence.GetFlowElementInstancesByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key, true)
	assert.NoError(t, err)

	byID := make(map[string]runtime.FlowElementInstance, len(flowElements))
	for _, fe := range flowElements {
		byID[fe.ElementId] = fe
	}

	// service task completes via UpdateOutputFlowElementInstance -> CompletedAt must be set
	task, ok := byID["id"]
	assert.True(t, ok, "service task 'id' should be in history")
	assert.NotNil(t, task.CompletedAt, "service task should have CompletedAt set after completion")
	assert.Equal(t, map[string]any{"variable_name": "done"}, task.OutputVariables,
		"service task should have its mapped output variables")
	assert.NotNil(t, task.InputVariables, "service task should carry input variables")

	for _, elementID := range []string{"StartEvent_1", "Flow_0xt1d7q", "Flow_1vz4oo2"} {
		fe, found := byID[elementID]
		assert.True(t, found, "element %s should be in history", elementID)
		assert.NotNil(t, fe.CompletedAt, "element %s should have CompletedAt after execution passes it", elementID)
	}

	endEvent, ok := byID["Event_1j4mcqg"]
	assert.True(t, ok, "end event 'Event_1j4mcqg' should be in history")
	assert.NotNil(t, endEvent.CompletedAt, "plain end event should have CompletedAt set when the process completes")
}

func TestTerminateEndEventCompletedAt(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/parallel_flow_with_terminate_end_task.bpmn")
	assert.NoError(t, err)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	flowElements, err := bpmnEngine.persistence.GetFlowElementInstancesByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key, true)
	assert.NoError(t, err)

	var terminateEndEvent *runtime.FlowElementInstance
	for i := range flowElements {
		if flowElements[i].ElementId == "TerminateEndEvent_id" {
			terminateEndEvent = &flowElements[i]
			break
		}
	}
	assert.NotNil(t, terminateEndEvent, "terminate end event should be in history")
	if terminateEndEvent != nil {
		assert.NotNil(t, terminateEndEvent.CompletedAt, "terminate end event should have CompletedAt after terminating its scope")
	}
}

func TestIntermediateTimerCatchEventCompletedAt(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/simple-timer-catch-event.bpmn")
	assert.NoError(t, err)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	// The bpmn defines a PT1S timer; wait for it to fire and the process to complete.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		updated, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
		assert.NoError(t, err)
		if updated.ProcessInstance().State == runtime.ActivityStateCompleted {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	updated, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, updated.ProcessInstance().State,
		"process should have completed after the intermediate timer fired")

	flowElements, err := bpmnEngine.persistence.GetFlowElementInstancesByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key, true)
	assert.NoError(t, err)

	var catchEvent *runtime.FlowElementInstance
	for i := range flowElements {
		if flowElements[i].ElementId == "Event_14lg07w" {
			catchEvent = &flowElements[i]
			break
		}
	}
	assert.NotNil(t, catchEvent, "intermediate timer catch event 'Event_14lg07w' should be in history")
	if catchEvent == nil {
		return
	}
	assert.NotNil(t, catchEvent.CompletedAt,
		"intermediate timer catch event should have CompletedAt set after the timer fires")
}

func TestPlainEndEventCompletedAtWithMultipleTokens(t *testing.T) {
	process, err := bpmnEngine.LoadFromFile(t.Context(), "./test-cases/parallel-gateway-flow.bpmn")
	assert.NoError(t, err)

	a1H := bpmnEngine.NewTaskHandler().Id("id-a-1").Handler(func(job ActivatedJob) { job.Complete() })
	defer bpmnEngine.RemoveHandler(a1H)
	b1H := bpmnEngine.NewTaskHandler().Id("id-b-1").Handler(func(job ActivatedJob) { job.Complete() })
	defer bpmnEngine.RemoveHandler(b1H)
	b2H := bpmnEngine.NewTaskHandler().Id("id-b-2").Handler(func(job ActivatedJob) { job.Complete() })
	defer bpmnEngine.RemoveHandler(b2H)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)

	flowElements, err := bpmnEngine.persistence.GetFlowElementInstancesByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key, true)
	assert.NoError(t, err)

	endEventRows := make([]runtime.FlowElementInstance, 0, 2)
	for _, fe := range flowElements {
		if fe.ElementId == "Event_1qu1nt8" {
			endEventRows = append(endEventRows, fe)
		}
	}
	assert.Equal(t, 2, len(endEventRows),
		"expected two FlowElementInstance rows for the end event (one per parallel token)")

	for i, fe := range endEventRows {
		assert.NotNil(t, fe.CompletedAt,
			"end event row %d (key %d) should have CompletedAt set", i, fe.Key)
	}
}

// ---------------------------------------------------------------------------
// Tests for issue #725 — Indexed BPMN element lookup during token execution.
//
// These tests pin down the externally observable contract of
// Engine.getExecutionTokenActivity. They cover the behavior the engine
// hot path depends on:
//   - top-level flow node resolution (DefaultProcessInstance),
//   - nested-subprocess token resolution (SubProcessInstance),
//   - call-activity token resolution (CallActivityInstance),
//   - multi-instance token resolution (MultiInstanceInstance),
//   - error paths for unknown IDs, IDs that resolve to a non-flow element,
//     and IDs that resolve to a boundary event,
//   - instance-type validation,
//   - pointer identity of the returned flow node (locks in the indexed
//     object rather than a range-loop copy).
//
// End-to-end coverage for call-activity and multi-instance execution is
// already provided by TestCallActivityStartsAndCompletes and the
// TestMultiInstance* tests in sub_process_test.go; this section adds
// focused tests that drive the lookup hot path directly with those
// instance types so the lookup contract is pinned even if a future
// change introduces a separate instance-type-specific lookup branch.
// ---------------------------------------------------------------------------

// lookupTestProcess loads and returns a BPMN process definition used by
// unsaved instances in direct getExecutionTokenActivity tests.
func lookupTestProcess(t *testing.T, path string) *runtime.ProcessDefinition {
	t.Helper()
	def, err := bpmnEngine.LoadFromFile(t.Context(), path)
	require.NoError(t, err, "failed to load BPMN fixture %s", path)
	return def
}

func lookupTestInstance(def *runtime.ProcessDefinition) runtime.ProcessInstance {
	return &runtime.DefaultProcessInstance{
		ProcessInstanceData: runtime.ProcessInstanceData{
			Definition: def,
			Key:        1,
			State:      runtime.ActivityStateReady,
		},
	}
}

func lookupTestToken(elementID string) runtime.ExecutionToken {
	return runtime.ExecutionToken{
		Key:                100,
		ElementInstanceKey: 200,
		ElementId:          elementID,
		ProcessInstanceKey: 1,
		State:              runtime.TokenStateRunning,
	}
}

// TestGetExecutionTokenActivity_TopLevelFlowNode ensures a token whose
// ElementId points to a top-level service task resolves to that element.
func TestGetExecutionTokenActivity_TopLevelFlowNode(t *testing.T) {
	def := lookupTestProcess(t, "./test-cases/simple_task.bpmn")

	activity, err := bpmnEngine.getExecutionTokenActivity(
		t.Context(),
		lookupTestInstance(def),
		lookupTestToken("id"),
	)
	require.NoError(t, err)
	require.NotNil(t, activity)
	assert.Equal(t, "id", activity.Element().GetId())
	assert.Equal(t, bpmn20.ElementTypeServiceTask, activity.Element().GetType())
}

// TestGetExecutionTokenActivity_NestedSubprocess exercises a token whose
// ElementId lives two subprocess levels deep and verifies that indexed
// lookup preserves the previous recursive lookup behavior.
func TestGetExecutionTokenActivity_NestedSubprocess(t *testing.T) {
	def := lookupTestProcess(t, "./test-cases/nested_sub_process_lookup.bpmn")

	activity, err := bpmnEngine.getExecutionTokenActivity(
		t.Context(),
		lookupTestInstance(def),
		lookupTestToken("DeepTask"),
	)
	require.NoError(t, err)
	require.NotNil(t, activity)
	assert.Equal(t, "DeepTask", activity.Element().GetId())
	assert.Equal(t, bpmn20.ElementTypeServiceTask, activity.Element().GetType())
}

// TestGetExecutionTokenActivity_SubProcessInstance drives the hot path
// with a SubProcessInstance whose token targets a deeply nested element.
// Indexed lookup resolves it with one definition-wide map probe.
func TestGetExecutionTokenActivity_SubProcessInstance(t *testing.T) {
	def := lookupTestProcess(t, "./test-cases/nested_sub_process_lookup.bpmn")

	parentToken := lookupTestToken("OuterSub")
	instance := &runtime.SubProcessInstance{
		ParentProcessExecutionToken:  parentToken,
		ParentProcessTargetElementId: "OuterSub",
		ProcessInstanceData: runtime.ProcessInstanceData{
			Definition: def,
			Key:        2,
			State:      runtime.ActivityStateReady,
		},
	}

	activity, err := bpmnEngine.getExecutionTokenActivity(
		t.Context(),
		instance,
		lookupTestToken("DeepTask"),
	)
	require.NoError(t, err)
	require.NotNil(t, activity)
	assert.Equal(t, "DeepTask", activity.Element().GetId())
}

// TestGetExecutionTokenActivity_CallActivityInstance drives the hot path
// with a CallActivityInstance to confirm the lookup path is the same
// regardless of which supported instance type the token belongs to.
// End-to-end call-activity execution is covered by
// TestCallActivityStartsAndCompletes in sub_process_test.go; this test
// focuses on the lookup contract itself.
func TestGetExecutionTokenActivity_CallActivityInstance(t *testing.T) {
	def := lookupTestProcess(t, "./test-cases/simple_task.bpmn")

	instance := &runtime.CallActivityInstance{
		ProcessInstanceData: runtime.ProcessInstanceData{
			Definition: def,
			Key:        4,
			State:      runtime.ActivityStateReady,
		},
	}

	activity, err := bpmnEngine.getExecutionTokenActivity(
		t.Context(),
		instance,
		lookupTestToken("id"),
	)
	require.NoError(t, err)
	require.NotNil(t, activity)
	assert.Equal(t, "id", activity.Element().GetId())
	assert.Equal(t, bpmn20.ElementTypeServiceTask, activity.Element().GetType())
}

// TestGetExecutionTokenActivity_MultiInstanceInstance drives the hot path
// with a MultiInstanceInstance to confirm the lookup path is the same
// regardless of which supported instance type the token belongs to.
// End-to-end multi-instance execution is covered by the
// TestMultiInstance* tests in sub_process_test.go; this test focuses on
// the lookup contract itself.
func TestGetExecutionTokenActivity_MultiInstanceInstance(t *testing.T) {
	def := lookupTestProcess(t, "./test-cases/simple_task.bpmn")

	instance := &runtime.MultiInstanceInstance{
		ParentProcessExecutionToken: lookupTestToken("id"),
		ProcessInstanceData: runtime.ProcessInstanceData{
			Definition: def,
			Key:        5,
			State:      runtime.ActivityStateReady,
		},
	}

	activity, err := bpmnEngine.getExecutionTokenActivity(
		t.Context(),
		instance,
		lookupTestToken("id"),
	)
	require.NoError(t, err)
	require.NotNil(t, activity)
	assert.Equal(t, "id", activity.Element().GetId())
	assert.Equal(t, bpmn20.ElementTypeServiceTask, activity.Element().GetType())
}

// TestGetExecutionTokenActivity_UnknownElementId confirms the engine
// returns a controlled error (not a panic) when the token references an
// id that the index does not contain.
func TestGetExecutionTokenActivity_UnknownElementId(t *testing.T) {
	def := lookupTestProcess(t, "./test-cases/simple_task.bpmn")

	activity, err := bpmnEngine.getExecutionTokenActivity(
		t.Context(),
		lookupTestInstance(def),
		lookupTestToken("does-not-exist"),
	)
	assert.Nil(t, activity)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does-not-exist")
}

// TestGetExecutionTokenActivity_NonFlowNode covers the case where the
// id resolves to an element that is registered in the index but is not
// a flow node. Sequence flows are good examples: they are BaseElements
// (and therefore indexed), but they do not implement FlowNode because
// they connect flow nodes rather than being driven themselves.
// Boundary events, by contrast, embed TFlowNode through TEvent and so
// they ARE flow nodes — see TestGetExecutionTokenActivity_BoundaryEventRejected.
func TestGetExecutionTokenActivity_NonFlowNode(t *testing.T) {
	def := lookupTestProcess(t, "./test-cases/simple_sub_process_task.bpmn")

	// Flow_0xt1d7q is a sequence flow. It is in the index, but it is
	// not a flow node, so the engine must refuse to process it.
	activity, err := bpmnEngine.getExecutionTokenActivity(
		t.Context(),
		lookupTestInstance(def),
		lookupTestToken("Flow_0xt1d7q"),
	)
	assert.Nil(t, activity)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Flow_0xt1d7q")
	assert.Contains(t, err.Error(), "not a flow node")
}

// TestGetExecutionTokenActivity_BoundaryEventRejected pins down the
// contract that boundary events must never be accepted as token targets.
// A TBoundaryEvent technically satisfies the FlowNode interface through
// method promotion (TEvent -> TFlowNode), but boundary events are
// consumed via boundary-event subscriptions rather than token execution,
// so the engine must reject them at the lookup step. This test would
// fail if the explicit boundary-event rejection were ever removed.
func TestGetExecutionTokenActivity_BoundaryEventRejected(t *testing.T) {
	def := lookupTestProcess(t, "./test-cases/simple_sub_process_task.bpmn")

	activity, err := bpmnEngine.getExecutionTokenActivity(
		t.Context(),
		lookupTestInstance(def),
		lookupTestToken("Event_07bcheq"),
	)
	assert.Nil(t, activity)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Event_07bcheq")
	assert.Contains(t, err.Error(), "boundary event")
}

// bogusProcessInstance satisfies runtime.ProcessInstance but does not
// match any of the case arms in the getExecutionTokenActivity switch.
// It is used by TestGetExecutionTokenActivity_InvalidInstanceType.
type bogusProcessInstance struct {
	runtime.ProcessInstanceData
}

func (b *bogusProcessInstance) Type() runtime.ProcessType { return runtime.ProcessType(99) }
func (b *bogusProcessInstance) ProcessInstance() *runtime.ProcessInstanceData {
	return &b.ProcessInstanceData
}
func (b *bogusProcessInstance) GetParentProcessInstanceKey() *int64 { return nil }

// TestGetExecutionTokenActivity_InvalidInstanceType asserts that the
// hot path validates the process-instance category before attempting to
// resolve the token element.
func TestGetExecutionTokenActivity_InvalidInstanceType(t *testing.T) {
	def := lookupTestProcess(t, "./test-cases/simple_task.bpmn")

	bogus := &bogusProcessInstance{
		ProcessInstanceData: runtime.ProcessInstanceData{
			Definition: def,
			Key:        3,
			State:      runtime.ActivityStateReady,
		},
	}

	activity, err := bpmnEngine.getExecutionTokenActivity(
		t.Context(),
		bogus,
		lookupTestToken("id"),
	)
	assert.Nil(t, activity)
	require.Error(t, err)
}

// TestGetExecutionTokenActivity_PointerIdentity locks in the hot path's
// use of the indexed object rather than a pointer to a range-loop copy.
// On the recursive implementation this test fails: the returned flow node
// is a copy of the slice element that lives at a different address than
// &definitions.Process.ServiceTasks[i]. Once the lookup switches to the
// index, the returned flow node IS that slice element, and the assertion
// holds.
func TestGetExecutionTokenActivity_PointerIdentity(t *testing.T) {
	def := lookupTestProcess(t, "./test-cases/simple_task.bpmn")

	activity, err := bpmnEngine.getExecutionTokenActivity(
		t.Context(),
		lookupTestInstance(def),
		lookupTestToken("id"),
	)
	require.NoError(t, err)
	require.NotNil(t, activity)

	// Find the service task index in the parsed definition.
	expected := (*bpmn20.TServiceTask)(nil)
	for i := range def.Definitions.Process.ServiceTasks {
		if def.Definitions.Process.ServiceTasks[i].Id == "id" {
			expected = &def.Definitions.Process.ServiceTasks[i]
			break
		}
	}
	require.NotNil(t, expected, "test fixture must contain a service task with id 'id'")

	// The hot path must hand back the very same slice element, not a copy.
	got, ok := activity.Element().(*bpmn20.TServiceTask)
	require.True(t, ok, "activity element should be a *TServiceTask")
	assert.Same(t, expected, got)
}
