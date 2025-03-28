package bpmn

import (
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/storage"

	"github.com/corbym/gocrest/has"
	"github.com/corbym/gocrest/is"
	"github.com/corbym/gocrest/then"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/tests"
)

const (
	varCounter                  = "counter"
	varEngineValidationAttempts = "engineValidationAttempts"
	varHasReachedMaxAttempts    = "hasReachedMaxAttempts"
)

func increaseCounterHandler(job ActivatedJob) {
	counter := job.Variable(varCounter).(float64)
	counter = counter + 1
	job.SetVariable(varCounter, counter)
	job.Complete()
}

func jobFailHandler(job ActivatedJob) {
	job.Fail("just because I can")
}

func jobCompleteHandler(job ActivatedJob) {
	job.Complete()
}

func Test_job_implements_Activity(t *testing.T) {
	var _ runtime.Activity = &runtime.Job{}
}

func Test_a_job_can_fail_and_keeps_the_instance_in_active_state(t *testing.T) {
	// setup
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")
	bpmnEngine.clearTaskHandlers()
	bpmnEngine.NewTaskHandler().Id("id").Handler(jobFailHandler)

	instance, _ := bpmnEngine.CreateAndRunInstance(process.ProcessKey, nil)

	then.AssertThat(t, instance.State, is.EqualTo(runtime.Active))

	// cleanup
	bpmnEngine.Stop()
}

// Test_simple_count_loop requires correct Task-Output-Mapping in the BPMN file
func Test_simple_count_loop(t *testing.T) {
	// setup
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-count-loop.bpmn")
	bpmnEngine.NewTaskHandler().Id("id-increaseCounter").Handler(increaseCounterHandler)

	vars := map[string]interface{}{}
	vars[varCounter] = 0.0
	instance, _ := bpmnEngine.CreateAndRunInstance(process.ProcessKey, vars)

	then.AssertThat(t, instance.GetVariable(varCounter), is.EqualTo(4.0))
	then.AssertThat(t, instance.State, is.EqualTo(runtime.Completed))

	// cleanup
	bpmnEngine.Stop()
}

func Test_simple_count_loop_with_message(t *testing.T) {
	t.Skip("TODO: re-enable once refactoring is done")
	// setup
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-count-loop-with-message.bpmn")

	vars := map[string]interface{}{}
	vars[varEngineValidationAttempts] = 0.0
	bpmnEngine.NewTaskHandler().Id("do-nothing").Handler(jobCompleteHandler)
	bpmnEngine.NewTaskHandler().Id("validate").Handler(func(job ActivatedJob) {
		attemptsVariable := job.Variable(varEngineValidationAttempts)
		attempts := attemptsVariable.(float64)
		foobar := attempts >= 1
		attempts++
		job.SetVariable(varEngineValidationAttempts, attempts)
		job.SetVariable(varHasReachedMaxAttempts, foobar)
		job.Complete()
	})

	instance, _ := bpmnEngine.CreateAndRunInstance(process.ProcessKey, vars) // should stop at the intermediate message catch event

	_ = bpmnEngine.PublishEventForInstance(instance.GetInstanceKey(), "msg", nil)
	_, _ = bpmnEngine.RunOrContinueInstance(instance.GetInstanceKey()) // again, should stop at the intermediate message catch event
	// validation happened
	_ = bpmnEngine.PublishEventForInstance(instance.GetInstanceKey(), "msg", nil)
	instance, _ = bpmnEngine.RunOrContinueInstance(instance.GetInstanceKey()) // should finish
	// validation happened

	then.AssertThat(t, instance.GetVariable(varHasReachedMaxAttempts), is.True())
	then.AssertThat(t, instance.GetVariable(varEngineValidationAttempts), is.EqualTo(2))
	then.AssertThat(t, instance.State, is.EqualTo(runtime.Completed))

	// internal State expected
	then.AssertThat(t, bpmnEngine.GetMessageSubscriptions(), has.Length(2))
	then.AssertThat(t, bpmnEngine.GetMessageSubscriptions()[0].MessageState, is.EqualTo(runtime.Completed))
	then.AssertThat(t, bpmnEngine.GetMessageSubscriptions()[1].MessageState, is.EqualTo(runtime.Completed))

	// cleanup
	bpmnEngine.Stop()
}

func Test_activated_job_data(t *testing.T) {
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")
	bpmnEngine.NewTaskHandler().Id("id").Handler(func(aj ActivatedJob) {
		then.AssertThat(t, aj.ElementId(), is.Not(is.Empty()))
		then.AssertThat(t, aj.CreatedAt(), is.Not(is.Nil()))
		then.AssertThat(t, aj.Key(), is.Not(is.EqualTo(int64(0))))
		then.AssertThat(t, aj.BpmnProcessId(), is.Not(is.Empty()))
		then.AssertThat(t, aj.ProcessDefinitionKey(), is.Not(is.EqualTo(int64(0))))
		then.AssertThat(t, aj.ProcessDefinitionVersion(), is.Not(is.EqualTo(int32(0))))
		then.AssertThat(t, aj.ProcessInstanceKey(), is.Not(is.EqualTo(int64(0))))
	})

	instance, _ := bpmnEngine.CreateAndRunInstance(process.ProcessKey, nil)

	then.AssertThat(t, instance.State, is.EqualTo(runtime.Active))

	// cleanup
	bpmnEngine.Stop()
}

func Test_task_InputOutput_mapping_happy_path(t *testing.T) {

	// setup
	cp := CallPath{}

	// give
	process, _ := bpmnEngine.LoadFromFile("./test-cases/service-task-input-output.bpmn")
	bpmnEngine.NewTaskHandler().Id("service-task-1").Handler(cp.TaskHandler)
	bpmnEngine.NewTaskHandler().Id("user-task-2").Handler(cp.TaskHandler)

	// when
	pi, err := bpmnEngine.CreateAndRunInstance(process.ProcessKey, nil)
	then.AssertThat(t, err, is.Nil())

	// then
	for _, job := range bpmnEngine.persistence.FindJobs(nil, nil, pi, nil) {
		then.AssertThat(t, job.JobState, is.EqualTo(runtime.Completed))
	}
	then.AssertThat(t, cp.CallPath, is.EqualTo("service-task-1,user-task-2"))
	// id from input should not exist in instance scope
	then.AssertThat(t, pi.GetVariable("id"), is.Nil())
	// output should exist in instance scope
	then.AssertThat(t, pi.GetVariable("dstcity"), is.EqualTo("beijing"))
	then.AssertThat(t, pi.GetVariable("order"), is.EqualTo(map[string]interface{}{
		"name": "order1",
		"id":   "1234",
	}))
	then.AssertThat(t, pi.GetVariable("orderId"), is.EqualTo(1234.0))
	then.AssertThat(t, pi.GetVariable("orderName"), is.EqualTo("order1"))

	// cleanup
	bpmnEngine.Stop()
}

func Test_instance_fails_on_Invalid_Input_mapping(t *testing.T) {
	// setup
	cp := CallPath{}

	// give
	process, _ := bpmnEngine.LoadFromFile("./test-cases/service-task-invalid-input.bpmn")
	bpmnEngine.NewTaskHandler().Id("invalid-input").Handler(cp.TaskHandler)

	// when
	pi, err := bpmnEngine.CreateAndRunInstance(process.ProcessKey, nil)
	then.AssertThat(t, err, is.Nil())

	// then
	then.AssertThat(t, cp.CallPath, is.EqualTo(""))
	then.AssertThat(t, pi.GetVariable("id"), is.Nil())
	then.AssertThat(t, bpmnEngine.persistence.FindJobs(nil, nil, pi, nil)[0].JobState, is.EqualTo(runtime.Failed))
	then.AssertThat(t, pi.GetState(), is.EqualTo(runtime.Failed))

	// cleanup
	bpmnEngine.Stop()
}

func Test_job_fails_on_Invalid_Output_mapping(t *testing.T) {
	t.Skip("TODO: re-enable once refactoring is done")
	// setup
	cp := CallPath{}

	// give
	process, _ := bpmnEngine.LoadFromFile("./test-cases/service-task-invalid-output.bpmn")
	bpmnEngine.NewTaskHandler().Id("invalid-output").Handler(cp.TaskHandler)

	// when
	pi, err := bpmnEngine.CreateAndRunInstance(process.ProcessKey, nil)
	then.AssertThat(t, err, is.Nil())

	// then
	then.AssertThat(t, cp.CallPath, is.EqualTo("invalid-output"))
	then.AssertThat(t, pi.GetVariable("order"), is.Nil())
	then.AssertThat(t, bpmnEngine.persistence.FindJobs(nil, nil, pi, nil)[0].JobState, is.EqualTo(runtime.Failed))
	then.AssertThat(t, pi.GetState(), is.EqualTo(runtime.Failed))

	// cleanup
	bpmnEngine.Stop()
}

func Test_task_type_handler(t *testing.T) {
	t.Skip("TODO: re-enable once refactoring is done")

	// setup
	var store storage.PersistentStorage = &tests.TestStorage{}
	bpmnEngine := New(WithStorage(store))
	cp := CallPath{}

	// give
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-task-with-type.bpmn")
	bpmnEngine.NewTaskHandler().Type("foobar").Handler(cp.TaskHandler)

	// when
	pi, err := bpmnEngine.CreateAndRunInstance(process.ProcessKey, nil)
	then.AssertThat(t, err, is.Nil())

	// then
	then.AssertThat(t, cp.CallPath, is.EqualTo("id"))
	then.AssertThat(t, pi.GetState(), is.EqualTo(runtime.Completed))

	// cleanup
	bpmnEngine.Stop()
}

func Test_task_type_handler_ID_handler_has_precedence(t *testing.T) {
	t.Skip("TODO: re-enable once refactoring is done")

	// setup
	var store storage.PersistentStorage = &tests.TestStorage{}
	bpmnEngine := New(WithStorage(store))
	calledHandler := "none"
	idHandler := func(job ActivatedJob) {
		calledHandler = "ID"
		job.Complete()
	}
	typeHandler := func(job ActivatedJob) {
		calledHandler = "TYPE"
		job.Complete()
	}
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-task-with-type.bpmn")

	// given reverse order of definition, means 'type:foobar' before 'id'
	bpmnEngine.NewTaskHandler().Type("foobar").Handler(typeHandler)
	bpmnEngine.NewTaskHandler().Id("id").Handler(idHandler)

	// when
	pi, err := bpmnEngine.CreateAndRunInstance(process.ProcessKey, nil)
	then.AssertThat(t, err, is.Nil())

	// then
	then.AssertThat(t, calledHandler, is.EqualTo("ID"))
	then.AssertThat(t, pi.GetState(), is.EqualTo(runtime.Completed))

	// cleanup
	bpmnEngine.Stop()
}

func Test_just_one_handler_called(t *testing.T) {
	t.Skip("TODO: re-enable once refactoring is done")

	// setup
	var store storage.PersistentStorage = &tests.TestStorage{}
	bpmnEngine := New(WithStorage(store))
	cp := CallPath{}
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-task-with-type.bpmn")

	// given multiple matching handlers executed
	bpmnEngine.NewTaskHandler().Id("id").Handler(cp.TaskHandler)
	bpmnEngine.NewTaskHandler().Id("id").Handler(cp.TaskHandler)
	bpmnEngine.NewTaskHandler().Type("foobar").Handler(cp.TaskHandler)

	// when
	pi, err := bpmnEngine.CreateAndRunInstance(process.ProcessKey, nil)
	then.AssertThat(t, err, is.Nil())

	// then
	then.AssertThat(t, cp.CallPath, is.EqualTo("id").Reason("just one execution"))
	then.AssertThat(t, pi.GetState(), is.EqualTo(runtime.Completed))

	// cleanup
	bpmnEngine.Stop()
}

func Test_assignee_and_candidate_groups_are_assigned_to_handler(t *testing.T) {
	t.Skip("TODO: re-enable once refactoring is done")

	// setup
	var store storage.PersistentStorage = &tests.TestStorage{}
	bpmnEngine := New(WithStorage(store))
	cp := CallPath{}
	process, _ := bpmnEngine.LoadFromFile("./test-cases/user-tasks-with-assignments.bpmn")

	// given multiple matching handlers executed
	bpmnEngine.NewTaskHandler().Assignee("john.doe").Handler(cp.TaskHandler)
	bpmnEngine.NewTaskHandler().CandidateGroups("marketing", "support").Handler(cp.TaskHandler)

	// when
	pi, err := bpmnEngine.CreateAndRunInstance(process.ProcessKey, nil)
	then.AssertThat(t, err, is.Nil())

	// then
	then.AssertThat(t, cp.CallPath, is.EqualTo("assignee-task,group-task"))
	then.AssertThat(t, pi.GetState(), is.EqualTo(runtime.Completed))

	// cleanup
	bpmnEngine.Stop()
}

func Test_task_default_all_output_variables_map_to_process_instance(t *testing.T) {
	// setup
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple_task-no_output_mapping.bpmn")
	bpmnEngine.clearTaskHandlers()
	bpmnEngine.NewTaskHandler().Id("id").Handler(func(job ActivatedJob) {
		job.SetVariable("aVariable", true)
		job.Complete()
	})

	instance, _ := bpmnEngine.CreateAndRunInstance(process.ProcessKey, nil)
	then.AssertThat(t, instance.State, is.EqualTo(runtime.Completed))

	then.AssertThat(t, instance.GetVariable("aVariable"), is.True())

	// cleanup
	bpmnEngine.Stop()
}

func Test_task_no_output_variables_mapping_on_failure(t *testing.T) {
	t.Skip("TODO: re-enable once refactoring is done")

	// setup
	var store storage.PersistentStorage = &tests.TestStorage{}
	bpmnEngine := New(WithStorage(store))
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple_task-no_output_mapping.bpmn")
	bpmnEngine.NewTaskHandler().Id("id").Handler(func(job ActivatedJob) {
		job.SetVariable("aVariable", true)
		job.Fail("because I can")
	})

	instance, _ := bpmnEngine.CreateAndRunInstance(process.ProcessKey, nil)
	then.AssertThat(t, instance.State, is.EqualTo(runtime.Active))

	then.AssertThat(t, instance.GetVariable("aVariable"), is.Nil())

	// cleanup
	bpmnEngine.Stop()
}

func Test_task_just_declared_output_variables_map_to_process_instance(t *testing.T) {
	t.Skip("TODO: re-enable once refactoring is done")

	// setup
	var store storage.PersistentStorage = &tests.TestStorage{}
	bpmnEngine := New(WithStorage(store))
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple_task-with_output_mapping.bpmn")
	bpmnEngine.NewTaskHandler().Id("id").Handler(func(job ActivatedJob) {
		job.SetVariable("valueFromHandler", true)
		job.SetVariable("otherVariable", "value")
		job.Complete()
	})

	instance, _ := bpmnEngine.CreateAndRunInstance(process.ProcessKey, nil)
	then.AssertThat(t, instance.State, is.EqualTo(runtime.Completed))

	then.AssertThat(t, instance.GetVariable("valueFromHandler"), is.True())
	then.AssertThat(t, instance.GetVariable("otherVariable"), is.Nil())

	// cleanup
	bpmnEngine.Stop()
}

func Test_missing_task_handlers_break_execution_and_can_be_continued_later(t *testing.T) {
	t.Skip("TODO: re-enable once refactoring is done")

	cp := CallPath{}
	// setup
	var store storage.PersistentStorage = &tests.TestStorage{}
	bpmnEngine := New(WithStorage(store))
	process, _ := bpmnEngine.LoadFromFile("./test-cases/parallel-gateway-flow.bpmn")

	// given
	bpmnEngine.NewTaskHandler().Id("id-a-1").Handler(cp.TaskHandler)
	instance, err := bpmnEngine.CreateAndRunInstance(process.ProcessKey, nil)
	then.AssertThat(t, err, is.Nil())
	then.AssertThat(t, instance.State, is.EqualTo(runtime.Active))
	then.AssertThat(t, cp.CallPath, is.EqualTo("id-a-1"))

	// when
	bpmnEngine.NewTaskHandler().Id("id-b-1").Handler(cp.TaskHandler)
	bpmnEngine.NewTaskHandler().Id("id-b-2").Handler(cp.TaskHandler)
	instance, err = bpmnEngine.RunOrContinueInstance(instance.GetInstanceKey())
	then.AssertThat(t, instance, is.Not(is.Nil()))
	then.AssertThat(t, instance.State, is.EqualTo(runtime.Completed))

	// then
	then.AssertThat(t, err, is.Nil())
	then.AssertThat(t, cp.CallPath, is.EqualTo("id-a-1,id-b-1,id-b-2"))

	// cleanup
	bpmnEngine.Stop()
}
