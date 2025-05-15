package bpmn

import (
	"os"
	"strings"
	"testing"
	"time"

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
	bpmnEngine.NewTaskHandler().Id("id").Handler(handler)

	// when
	bpmnEngine.CreateAndRunInstance(process.Key, nil)

	// then
	assert.True(t, wasCalled)
}

func TestRegisterHandlerByTaskIdGetsCalledAfterLateRegister(t *testing.T) {

	// setup
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")
	wasCalled := false
	handler := func(job ActivatedJob) {
		wasCalled = true
		job.Complete()
	}
	bpmnEngine.clearTaskHandlers()
	// // given
	pi, err := bpmnEngine.CreateAndRunInstance(process.Key, nil)
	if err != nil {
		t.Fatal(err)
	}
	bpmnEngine.NewTaskHandler().Id("id").Handler(handler)
	bpmnEngine.RunOrContinueInstance(pi.Key)

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
		job.SetVariable(variableName, "newVal")
		job.Complete()
	}
	bpmnEngine.clearTaskHandlers()

	// given
	bpmnEngine.NewTaskHandler().Id(taskId).Handler(handler)

	// when
	instance, _ := bpmnEngine.CreateAndRunInstance(process.Key, variableContext)

	v := engineStorage.ProcessInstances[instance.Key]
	// then
	assert.NotNil(t, v, "Process isntance needs to be present")
	assert.Equal(t, "newVal", v.VariableHolder.GetVariable(variableName))
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

func TestMultipleInstancesCanBeCreated(t *testing.T) {
	// setup
	beforeCreation := time.Now()

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")

	// when
	instance1, _ := bpmnEngine.CreateInstance(process, nil)
	instance2, _ := bpmnEngine.CreateInstance(process, nil)

	// then
	assert.GreaterOrEqual(t, instance1.CreatedAt.UnixNano(), beforeCreation.UnixNano(), "make sure we have creation time set")
	assert.Equal(t, instance2.Definition.Key, instance1.Definition.Key)
}

func TestSimpleAndUncontrolledForkingTwoTasks(t *testing.T) {
	// setup
	cp := CallPath{}
	bpmnEngine.clearTaskHandlers()

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/forked-flow.bpmn")
	bpmnEngine.NewTaskHandler().Id("id-a-1").Handler(cp.TaskHandler)
	bpmnEngine.NewTaskHandler().Id("id-b-1").Handler(cp.TaskHandler)
	bpmnEngine.NewTaskHandler().Id("id-b-2").Handler(cp.TaskHandler)

	// when
	bpmnEngine.CreateAndRunInstance(process.Key, nil)

	// then
	assert.Equal(t, "id-a-1,id-b-1,id-b-2", cp.CallPath)
}

func TestParallelGateWayTwoTasks(t *testing.T) {
	// setup
	cp := CallPath{}
	bpmnEngine.clearTaskHandlers()

	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/parallel-gateway-flow.bpmn")
	bpmnEngine.NewTaskHandler().Id("id-a-1").Handler(cp.TaskHandler)
	bpmnEngine.NewTaskHandler().Id("id-b-1").Handler(cp.TaskHandler)
	bpmnEngine.NewTaskHandler().Id("id-b-2").Handler(cp.TaskHandler)

	// when
	bpmnEngine.CreateAndRunInstance(process.Key, nil)

	// then
	assert.Equal(t, "id-a-1,id-b-1,id-b-2", cp.CallPath)
}

func Test_multiple_engines_create_unique_Ids(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine1 := NewEngine(EngineWithStorage(store))
	store2 := inmemory.NewStorage()
	bpmnEngine2 := NewEngine(EngineWithStorage(store2))

	// when
	process1, _ := bpmnEngine1.LoadFromFile("./test-cases/simple_task.bpmn")
	process2, _ := bpmnEngine2.LoadFromFile("./test-cases/simple_task.bpmn")

	// then
	assert.NotEqual(t, process2.Key, process1.Key)
}

func Test_CreateInstanceById_uses_latest_process_version(t *testing.T) {
	// when
	v1, err := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")
	assert.Nil(t, err)
	assert.Equal(t, "aName", v1.Definitions.Process.Name)
	// when
	v2, err := bpmnEngine.LoadFromFile("./test-cases/simple_task_v2.bpmn")
	assert.Nil(t, err)
	assert.Equal(t, "aName", v2.Definitions.Process.Name)

	instance, err := bpmnEngine.CreateInstanceById("Simple_Task_Process", nil)
	assert.Nil(t, err)
	assert.NotNil(t, instance)
	assert.Equal(t, int32(v2.Version), instance.Definition.Version)
}

func Test_CreateAndRunInstanceById_uses_latest_process_version(t *testing.T) {
	// when
	v1, err := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")
	assert.Nil(t, err)
	assert.Equal(t, "aName", v1.Definitions.Process.Name)
	// when
	v2, err := bpmnEngine.LoadFromFile("./test-cases/simple_task_v2.bpmn")
	assert.Nil(t, err)
	assert.Equal(t, "aName", v2.Definitions.Process.Name)

	instance, err := bpmnEngine.CreateAndRunInstanceById("Simple_Task_Process", nil)
	assert.Nil(t, err)
	assert.NotNil(t, instance)

	// then
	assert.Equal(t, int32(v2.Version), instance.Definition.Version)
}

func Test_CreateInstanceById_return_error_when_no_ID_found(t *testing.T) {
	// when
	instance, err := bpmnEngine.CreateInstanceById("Simple_Task_Process_not_existing", nil)

	// then
	assert.Nil(t, instance)
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "no process with id=Simple_Task_Process_not_existing was found (prior loaded into the engine)"))
}
