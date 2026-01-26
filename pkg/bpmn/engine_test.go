package bpmn

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"

	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
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
	bpmnEngine.Start()

	// Run the tests
	exitCode = m.Run()
}

func TestRegisterHandlerByTaskIdGetsCalled(t *testing.T) {
	// setup
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")
	wasCalled := false
	handler := func(job ActivatedJob) {
		wasCalled = true
		job.Complete()
	}

	// given
	idH := bpmnEngine.NewTaskHandler().Id("id").Handler(handler)
	defer bpmnEngine.RemoveHandler(idH)

	// when
	_, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	// then
	assert.True(t, wasCalled)
}

func TestRegisterHandlerByTaskIdGetsCalledAfterLateRegister(t *testing.T) {
	t.Skip("runtime modification of handlers is not supported yet")
	// setup
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")
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
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")
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
	assert.NotNil(t, v, "Process isntance needs to be present")
	assert.Equal(t, "newVal", v.ProcessInstance().VariableHolder.GetLocalVariable(variableName))
}

func TestMetadataIsGivenFromLoadedXmlFile(t *testing.T) {
	// setup
	metadata, _ := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")

	assert.Equal(t, int32(1), metadata.Version)
	assert.Greater(t, metadata.Key, int64(1))
	assert.Equal(t, "Simple_Task_Process", metadata.BpmnProcessId)
}

func TestLoadingTheSameFileWillNotIncreaseTheVersionNorChangeTheProcessKey(t *testing.T) {
	// setup
	metadata, _ := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")
	keyOne := metadata.Key
	assert.Equal(t, int32(1), metadata.Version)

	metadata, _ = bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")
	keyTwo := metadata.Key
	assert.Equal(t, int32(1), metadata.Version)
	assert.Equal(t, keyTwo, keyOne)
}

func TestLoadingTheSameProcessWithModificationWillCreateNewVersion(t *testing.T) {
	// setup
	process1, _ := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")
	process2, _ := bpmnEngine.LoadFromFile("./test-cases/simple_task_modified_taskId.bpmn")
	process3, _ := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")

	assert.Equal(t, process1.BpmnProcessId, process2.BpmnProcessId, "both prepared files should have equal IDs")
	assert.Equal(t, int32(1), process1.Version)
	assert.Equal(t, int32(2), process2.Version)
	assert.Equal(t, int32(3), process3.Version)

	assert.NotEqual(t, process2.Key, process1.Key)
}

func TestInstanceCanStartAtChosenFlowNode(t *testing.T) {
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/forked-flow.bpmn")
	a1H := bpmnEngine.NewTaskHandler().Id("id-a-1").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(a1H)
	b1H := bpmnEngine.NewTaskHandler().Id("id-b-1").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(b1H)
	b2H := bpmnEngine.NewTaskHandler().Id("id-b-2").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(b2H)

	startingElementIds := []string{"id-b-1", "id-b-2"}
	_, err := bpmnEngine.CreateInstanceWithStartingElements(t.Context(), process.Key, startingElementIds, nil, nil)
	assert.NoError(t, err)

	assert.Equal(t, "id-b-1,id-b-2", cp.CallPath)
}

func TestMultipleInstancesCanBeCreated(t *testing.T) {
	// setup
	beforeCreation := time.Now()

	// given
	process, err := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")
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
	process, _ := bpmnEngine.LoadFromFile("./test-cases/forked-flow.bpmn")
	a1H := bpmnEngine.NewTaskHandler().Id("id-a-1").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(a1H)
	b1H := bpmnEngine.NewTaskHandler().Id("id-b-1").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(b1H)
	b2H := bpmnEngine.NewTaskHandler().Id("id-b-2").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(b2H)

	// when
	_, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	// then
	assert.Equal(t, "id-a-1,id-b-1,id-b-2", cp.CallPath)
}

func TestParallelGateWayTwoTasks(t *testing.T) {
	// setup
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/parallel-gateway-flow.bpmn")
	a1H := bpmnEngine.NewTaskHandler().Id("id-a-1").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(a1H)
	b1H := bpmnEngine.NewTaskHandler().Id("id-b-1").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(b1H)
	b2H := bpmnEngine.NewTaskHandler().Id("id-b-2").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(b2H)

	// when
	_, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	// then
	assert.Equal(t, "id-a-1,id-b-1,id-b-2", cp.CallPath)
}

func TestMultipleEnginesCreateUniqueIds(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine1 := NewEngine(EngineWithStorage(store))
	store2 := inmemory.NewStorage()
	bpmnEngine2 := NewEngine(EngineWithStorage(store2))

	// when
	process1, err := bpmnEngine1.LoadFromFile("./test-cases/simple_task.bpmn")
	assert.NoError(t, err)
	process2, err := bpmnEngine2.LoadFromFile("./test-cases/simple_task.bpmn")
	assert.NoError(t, err)

	// then
	assert.NotEqual(t, process2.Key, process1.Key)
}

func TestCreateInstanceByIdUsesLatestProcessVersion(t *testing.T) {
	// when
	v1, err := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")
	assert.NoError(t, err)
	assert.Equal(t, "aName", v1.Definitions.Process.Name)
	// when
	v2, err := bpmnEngine.LoadFromFile("./test-cases/simple_task_v2.bpmn")
	assert.NoError(t, err)
	assert.Equal(t, "aName", v2.Definitions.Process.Name)

	instance, err := bpmnEngine.CreateInstanceById(t.Context(), "Simple_Task_Process", nil)
	assert.NoError(t, err)
	assert.NotNil(t, instance)
	assert.Equal(t, v2.Version, instance.ProcessInstance().Definition.Version)
}

func TestCreateAndRunInstanceByIdUsesLatestProcessVersion(t *testing.T) {
	// when
	v1, err := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")
	assert.NoError(t, err)
	assert.Equal(t, "aName", v1.Definitions.Process.Name)
	// when
	v2, err := bpmnEngine.LoadFromFile("./test-cases/simple_task_v2.bpmn")
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
	assert.True(t, strings.Contains(err.Error(), "no process with id=Simple_Task_Process_not_existing was found (prior loaded into the engine)"))
}

func TestCancelInstanceShouldCancelInstance(t *testing.T) {
	// setup
	_, err := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")
	assert.NoError(t, err)
	process, err := bpmnEngine.LoadFromFile("./test-cases/call-activity-with-multiple-boundary.bpmn")
	assert.NoError(t, err)

	variableContext := make(map[string]interface{}, 1)
	randomCorellationKey := rand.Int63()
	variableContext["correlationKey"] = fmt.Sprint(randomCorellationKey)

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
		cps, err := bpmnEngine.persistence.FindProcessInstanceByParentExecutionTokenKey(t.Context(), token.Key)
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

func TestModifyProcessInstance(t *testing.T) {
	// setup
	_, err := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")
	assert.NoError(t, err)
	definition, err := bpmnEngine.LoadFromFile("./test-cases/call-activity-with-multiple-boundary-user-task-end.bpmn")
	assert.NoError(t, err)

	variableContext := make(map[string]interface{}, 1)
	randomCorellationKey := rand.Int63()
	variableContext["correlationKey"] = fmt.Sprint(randomCorellationKey)

	// when
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), definition.Key, variableContext)
	assert.NoError(t, err)

	// wait for activity instance to be created (TODO: the fact that this needs to be here is an issue)
	assert.Eventually(t, func() bool {
		inMem := bpmnEngine.persistence.(*inmemory.Storage)
		for _, inst := range inMem.ProcessInstances {
			if inst.ParentProcessExecutionToken != nil {
				// wait till instance is already created
				if inst.ParentProcessExecutionToken.ProcessInstanceKey == instance.Key {
					return true
				}
			}
		}
		return false
	}, 500*time.Millisecond, 10*time.Millisecond)

	var executionTokens []runtime.ExecutionToken
	assert.Eventually(t, func() bool {
		executionTokens, err = bpmnEngine.persistence.GetActiveTokensForProcessInstance(t.Context(), instance.ProcessInstance().Key)
		assert.NoError(t, err)
		if executionTokens != nil && len(executionTokens) == 1 {
			return true
		}
		return false
	}, 5*time.Second, 200*time.Millisecond)

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
	jobs, err := bpmnEngine.persistence.FindTokenJobsInState(t.Context(), mainToken.Key, []runtime.ActivityState{runtime.ActivityStateActive, runtime.ActivityStateCompleting, runtime.ActivityStateFailed})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(jobs), "expected 0 jobs, but found %d", len(jobs))

	// All incidents should be resolved
	// TODO: would need different test

	// All called processes should be terminated
	cps, err := bpmnEngine.persistence.FindProcessInstanceByParentExecutionTokenKey(t.Context(), mainToken.Key)
	assert.NoError(t, err)

	for _, cp := range cps {
		assert.Equal(t, runtime.ActivityStateTerminated, cp.ProcessInstance().State, "expected cancelled state for terminated definition, but found %s", cp.ProcessInstance().State)
	}
}

func TestEventBasedGatewaySelectsMessagePath(t *testing.T) {
	// setup
	cp := CallPath{}

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/message-intermediate-timer-event.bpmn")
	mH := bpmnEngine.NewTaskHandler().Id("task-for-message").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(mH)
	tH := bpmnEngine.NewTaskHandler().Id("task-for-timer").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(tH)
	_, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	// when
	for _, message := range engineStorage.MessageSubscriptions {
		if message.Name == "message" {
			err := bpmnEngine.PublishMessage(t.Context(), message.Key, nil)
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
	process, _ := bpmnEngine.LoadFromFile(filepath.Join(".", "test-cases", "simple-business-rule-task-local.bpmn"))

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
	assert.Equal(t, true, (instance.ProcessInstance().VariableHolder.LocalVariables()["testResultVariable"]).(map[string]interface{})["canAutoLiquidate"])
	assert.Equal(t, true, instance.ProcessInstance().VariableHolder.LocalVariables()["OutputTestResultVariable"])
	assert.Nil(t, instance.ProcessInstance().VariableHolder.LocalVariables()["testResultVariable2"])
	assert.Equal(t, true, (instance.ProcessInstance().VariableHolder.LocalVariables()["testResultVariable3"]).(map[string]interface{})["canAutoLiquidate"])

	assert.Equal(t, runtime.ActivityStateCompleted, instance.ProcessInstance().State)
}
