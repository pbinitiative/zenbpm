package tests

import (
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"os"
	"testing"
	"time"

	"github.com/corbym/gocrest/has"
	"github.com/corbym/gocrest/is"
	"github.com/corbym/gocrest/then"
	bpmn_engine "github.com/pbinitiative/zenbpm/pkg/bpmn"
)

type CallPath struct {
	CallPath string
}

const enableJsonDataDump = true

func (callPath *CallPath) CallPathHandler(job bpmn_engine.ActivatedJob) {
	if len(callPath.CallPath) > 0 {
		callPath.CallPath += ","
	}
	callPath.CallPath += job.ElementId()
	job.Complete()
}

func Test_Unmarshal_restores_processKey(t *testing.T) {
	t.Skip("TODO: re-enable once refactoring is done")

	// setup
	var store storage.PersistentStorage = &TestStorage{}
	bpmnEngine := bpmn_engine.New(bpmn_engine.WithStorage(store))

	// given
	piBefore, err := bpmnEngine.LoadFromFile("../test-cases/simple_task.bpmn")
	then.AssertThat(t, err, is.Nil())

	// when
	bytes := bpmnEngine.Marshal()

	// when
	bpmnEngine, err = bpmn_engine.Unmarshal(bytes)
	then.AssertThat(t, err, is.Nil())
	processes := bpmnEngine.FindProcessesById("Simple_Task_Process")

	// then
	then.AssertThat(t, processes, has.Length(1))
	then.AssertThat(t, processes[0].ProcessKey, is.EqualTo(piBefore.ProcessKey))
}

func Test_preserve_engine_name(t *testing.T) {
	t.Skip("TODO: re-enable once refactoring is done")

	// setup
	var store storage.PersistentStorage = &TestStorage{}
	originEngine := bpmn_engine.New(bpmn_engine.WithStorage(store))

	// given
	bytes := originEngine.Marshal()
	intermediateEngine, err := bpmn_engine.Unmarshal(bytes)
	then.AssertThat(t, err, is.Nil())

	// when
	finalEngine, err := bpmn_engine.Unmarshal(intermediateEngine.Marshal())
	then.AssertThat(t, err, is.Nil())

	// then
	then.AssertThat(t, finalEngine.Name(), is.EqualTo(originEngine.Name()))
}

func Test_Marshal_Unmarshal_Jobs(t *testing.T) {
	t.Skip("TODO: re-enable once refactoring is done")

	// setup
	var store storage.PersistentStorage = &TestStorage{}
	bpmnEngine := bpmn_engine.New(bpmn_engine.WithStorage(store))

	// given
	pi, err := bpmnEngine.LoadFromFile("../test-cases/simple_task.bpmn")
	then.AssertThat(t, err, is.Nil())

	// when
	instance, err := bpmnEngine.CreateAndRunInstance(pi.ProcessKey, nil)
	then.AssertThat(t, err, is.Nil())
	bytes := bpmnEngine.Marshal()
	then.AssertThat(t, len(bytes), is.GreaterThan(32))

	if enableJsonDataDump {
		_ = os.WriteFile("temp.marshal.jobs.json", bytes, 0644)
	}

	// when
	bpmnEngine, err = bpmn_engine.Unmarshal(bytes)
	then.AssertThat(t, err, is.Nil())

	// then
	instance, err = bpmnEngine.RunOrContinueInstance(instance.GetInstanceKey())
	then.AssertThat(t, err, is.Nil())
	then.AssertThat(t, instance.GetState(), is.EqualTo(runtime.Active))
}

func Test_Marshal_Unmarshal_partially_executed_jobs_continue_where_left_of_before_marshalling(t *testing.T) {
	t.Skip("TODO: re-enable once refactoring is done")

	// setup
	var store storage.PersistentStorage = &TestStorage{}
	bpmnEngine := bpmn_engine.New(bpmn_engine.WithStorage(store))
	cp := CallPath{}
	bpmnEngine.NewTaskHandler().Id("id-a-1").Handler(cp.CallPathHandler)

	// given
	pi, err := bpmnEngine.LoadFromFile("../test-cases/parallel-gateway-flow.bpmn")
	then.AssertThat(t, err, is.Nil())

	// when
	instance, err := bpmnEngine.CreateAndRunInstance(pi.ProcessKey, nil)
	then.AssertThat(t, err, is.Nil())
	then.AssertThat(t, cp.CallPath, is.EqualTo("id-a-1"))

	instance, err = bpmnEngine.RunOrContinueInstance(instance.InstanceKey)
	bytes := bpmnEngine.Marshal()
	then.AssertThat(t, len(bytes), is.GreaterThan(32))

	if enableJsonDataDump {
		os.WriteFile("temp.marshal.parallel-gateway-flow.json", bytes, 0644)
	}

	// when
	bpmnEngine, err = bpmn_engine.Unmarshal(bytes)
	bpmnEngine.NewTaskHandler().Id("id-b-1").Handler(cp.CallPathHandler)
	bpmnEngine.NewTaskHandler().Id("id-b-2").Handler(cp.CallPathHandler)
	then.AssertThat(t, err, is.Nil())

	// then
	instance, err = bpmnEngine.RunOrContinueInstance(instance.GetInstanceKey())
	then.AssertThat(t, err, is.Nil())
	then.AssertThat(t, instance.GetState(), is.EqualTo(runtime.Completed))
	then.AssertThat(t, cp.CallPath, is.EqualTo("id-a-1,id-b-1,id-b-2"))

}

func Test_Marshal_Unmarshal_Remain_Handler(t *testing.T) {
	t.Skip("TODO: re-enable once refactoring is done")

	// setup
	var store storage.PersistentStorage = &TestStorage{}
	bpmnEngine := bpmn_engine.New(bpmn_engine.WithStorage(store))
	cp := CallPath{}

	// given
	pi, err := bpmnEngine.LoadFromFile("../test-cases/simple_task.bpmn")
	then.AssertThat(t, err, is.Nil())
	bpmnEngine.NewTaskHandler().Id("id").Handler(cp.CallPathHandler)

	// when
	instance, err := bpmnEngine.CreateInstance(pi, nil)
	then.AssertThat(t, err, is.Nil())
	then.AssertThat(t, instance.GetState(), is.EqualTo(runtime.Ready))
	bytes := bpmnEngine.Marshal()

	if enableJsonDataDump {
		os.WriteFile("temp.marshal.remain.json", bytes, 0644)
	}

	// when
	newEngine, err := bpmn_engine.Unmarshal(bytes)
	then.AssertThat(t, err, is.Nil())
	newEngine.NewTaskHandler().Id("id").Handler(cp.CallPathHandler)

	// then
	instance, err = newEngine.RunOrContinueInstance(instance.GetInstanceKey())
	then.AssertThat(t, err, is.Nil())
	then.AssertThat(t, instance.GetState(), is.EqualTo(runtime.Completed))

	then.AssertThat(t, cp.CallPath, is.EqualTo("id"))
}

func Test_Marshal_Unmarshal_IntermediateCatchEvents(t *testing.T) {
	t.Skip("TODO: re-enable once refactoring is done")

	// setup
	var store storage.PersistentStorage = &TestStorage{}
	bpmnEngine := bpmn_engine.New(bpmn_engine.WithStorage(store))

	// given
	pi, err := bpmnEngine.LoadFromFile("../test-cases/simple-intermediate-message-catch-event.bpmn")
	then.AssertThat(t, err, is.Nil())

	// when
	_, err = bpmnEngine.CreateAndRunInstance(pi.ProcessKey, nil)
	then.AssertThat(t, err, is.Nil())
	bytes := bpmnEngine.Marshal()
	then.AssertThat(t, len(bytes), is.GreaterThan(32))

	if enableJsonDataDump {
		_ = os.WriteFile("temp.marshal.intermediate-catch-event.json", bytes, 0644)
	}

	// when
	newBpmnEngine, err := bpmn_engine.Unmarshal(bytes)
	then.AssertThat(t, err, is.Nil())

	// then
	subscriptions := newBpmnEngine.GetMessageSubscriptions()
	then.AssertThat(t, subscriptions, has.Length(1))
}

func Test_Marshal_Unmarshal_IntermediateTimerEvents_timer_is_completing(t *testing.T) {
	t.Skip("TODO: re-enable once refactoring is done")

	// setup
	var store storage.PersistentStorage = &TestStorage{}
	bpmnEngine := bpmn_engine.New(bpmn_engine.WithStorage(store))
	cp := CallPath{}

	// given
	pi, err := bpmnEngine.LoadFromFile("../test-cases/message-intermediate-timer-event.bpmn")
	then.AssertThat(t, err, is.Nil())

	// when
	instance, err := bpmnEngine.CreateAndRunInstance(pi.ProcessKey, nil)
	then.AssertThat(t, err, is.Nil())
	bytes := bpmnEngine.Marshal()
	then.AssertThat(t, len(bytes), is.GreaterThan(32))

	if enableJsonDataDump {
		os.WriteFile("temp.marshal.message-intermediate-timer-event.json", bytes, 0644)
	}

	// when
	bpmnEngine, err = bpmn_engine.Unmarshal(bytes)
	then.AssertThat(t, err, is.Nil())

	// then
	timers := bpmnEngine.GetTimersScheduled()
	then.AssertThat(t, timers, has.Length(1))
	pii := bpmnEngine.FindProcessInstance(instance.InstanceKey)
	then.AssertThat(t, pii, is.Not(is.Nil()))

	// when
	bpmnEngine.NewTaskHandler().Id("task-for-timer").Handler(cp.CallPathHandler)
	bpmnEngine.NewTaskHandler().Id("task-for-message").Handler(cp.CallPathHandler)
	time.Sleep(1 * time.Second)
	pii, err = bpmnEngine.RunOrContinueInstance(pii.InstanceKey)
	then.AssertThat(t, pii, is.Not(is.Nil()))
	then.AssertThat(t, pii.State, is.EqualTo(runtime.Completed))
	then.AssertThat(t, cp.CallPath, is.EqualTo("task-for-timer"))
}

func Test_Marshal_Unmarshal_IntermediateTimerEvents_message_is_completing(t *testing.T) {
	t.Skip("TODO: re-enable once refactoring is done")

	// setup
	var store storage.PersistentStorage = &TestStorage{}
	bpmnEngine := bpmn_engine.New(bpmn_engine.WithStorage(store))
	cp := CallPath{}

	// given
	pi, err := bpmnEngine.LoadFromFile("../test-cases/message-intermediate-timer-event.bpmn")
	then.AssertThat(t, err, is.Nil())

	// when
	instance, err := bpmnEngine.CreateAndRunInstance(pi.ProcessKey, nil)
	then.AssertThat(t, err, is.Nil())
	bytes := bpmnEngine.Marshal()
	then.AssertThat(t, len(bytes), is.GreaterThan(32))

	// when
	bpmnEngine, err = bpmn_engine.Unmarshal(bytes)
	then.AssertThat(t, err, is.Nil())

	// then
	subscriptions := bpmnEngine.GetMessageSubscriptions()
	then.AssertThat(t, subscriptions, has.Length(1))
	pii := bpmnEngine.FindProcessInstance(instance.InstanceKey)
	then.AssertThat(t, pii, is.Not(is.Nil()))

	// when
	bpmnEngine.NewTaskHandler().Id("task-for-timer").Handler(cp.CallPathHandler)
	bpmnEngine.NewTaskHandler().Id("task-for-message").Handler(cp.CallPathHandler)
	err = bpmnEngine.PublishEventForInstance(pii.InstanceKey, "message", nil)
	then.AssertThat(t, err, is.Nil())
	pii, err = bpmnEngine.RunOrContinueInstance(pii.InstanceKey)
	then.AssertThat(t, pii, is.Not(is.Nil()))
	then.AssertThat(t, pii.State, is.EqualTo(runtime.Completed))
	then.AssertThat(t, cp.CallPath, is.EqualTo("task-for-message"))
}
