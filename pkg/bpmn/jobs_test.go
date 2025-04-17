package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
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

	instance, _ := bpmnEngine.CreateAndRunInstance(process.Key, nil)

	assert.Equal(t, runtime.ActivityStateActive, instance.State)
}

// Test_simple_count_loop requires correct Task-Output-Mapping in the BPMN file
func Test_simple_count_loop(t *testing.T) {
	// setup
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-count-loop.bpmn")
	bpmnEngine.NewTaskHandler().Id("id-increaseCounter").Handler(increaseCounterHandler)

	vars := map[string]interface{}{}
	vars[varCounter] = 0.0
	instance, _ := bpmnEngine.CreateAndRunInstance(process.Key, vars)

	assert.Equal(t, 4.0, instance.GetVariable(varCounter))
	assert.Equal(t, runtime.ActivityStateCompleted, instance.State)
}

func Test_simple_count_loop_with_message(t *testing.T) {
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

	instance, _ := bpmnEngine.CreateAndRunInstance(process.Key, vars) // should stop at the intermediate message catch event

	_ = bpmnEngine.PublishEventForInstance(instance.GetInstanceKey(), "msg", nil)
	_, _ = bpmnEngine.RunOrContinueInstance(instance.GetInstanceKey()) // again, should stop at the intermediate message catch event
	// validation happened
	_ = bpmnEngine.PublishEventForInstance(instance.GetInstanceKey(), "msg", nil)
	instance, _ = bpmnEngine.RunOrContinueInstance(instance.GetInstanceKey()) // should finish
	// validation happened

	assert.True(t, instance.GetVariable(varHasReachedMaxAttempts).(bool))
	assert.Equal(t, 2.0, instance.GetVariable(varEngineValidationAttempts))
	assert.Equal(t, runtime.ActivityStateCompleted, instance.State)

	// internal State expected
	messages := make([]runtime.MessageSubscription, 0)
	for _, mes := range engineStorage.MessageSubscriptions {
		if mes.ProcessInstanceKey == instance.Key {
			messages = append(messages, mes)
		}

	}
	assert.Len(t, messages, 2)
	assert.Equal(t, runtime.ActivityStateCompleted, messages[0].MessageState)
	assert.Equal(t, runtime.ActivityStateCompleted, messages[1].MessageState)
}

func Test_activated_job_data(t *testing.T) {
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")

	bpmnEngine.NewTaskHandler().Id("id").Handler(func(aj ActivatedJob) {
		assert.NotEmpty(t, aj.ElementId())
		assert.NotNil(t, aj.CreatedAt())
		assert.NotEqual(t, int64(0), aj.Key())
		assert.NotEmpty(t, aj.BpmnProcessId())
		assert.NotEqual(t, int64(0), aj.ProcessDefinitionKey())
		assert.NotEqual(t, int32(0), aj.ProcessDefinitionVersion())
		assert.NotEqual(t, int64(0), aj.ProcessInstanceKey())
	})

	instance, _ := bpmnEngine.CreateAndRunInstance(process.Key, nil)

	assert.Equal(t, runtime.ActivityStateActive, instance.State)
}

func Test_task_InputOutput_mapping_happy_path(t *testing.T) {
	// setup
	cp := CallPath{}

	// give
	process, _ := bpmnEngine.LoadFromFile("./test-cases/service-task-input-output.bpmn")
	bpmnEngine.NewTaskHandler().Id("service-task-1").Handler(cp.TaskHandler)
	bpmnEngine.NewTaskHandler().Id("user-task-2").Handler(cp.TaskHandler)

	// when
	pi, err := bpmnEngine.CreateAndRunInstance(process.Key, nil)
	assert.Nil(t, err)

	// then
	jobs := make([]runtime.Job, 0)
	for _, job := range engineStorage.Jobs {
		if job.ProcessInstanceKey == pi.Key {
			jobs = append(jobs, job)
		}
	}
	for _, job := range jobs {
		assert.Equal(t, runtime.ActivityStateCompleted, job.State)
	}
	assert.Equal(t, "service-task-1,user-task-2", cp.CallPath)
	// id from input should not exist in instance scope
	assert.Nil(t, pi.GetVariable("id"))
	// output should exist in instance scope
	assert.Equal(t, "beijing", pi.GetVariable("dstcity"))
	assert.Equal(t, pi.GetVariable("order"), map[string]interface{}{
		"name": "order1",
		"id":   "1234",
	})
	assert.Equal(t, 1234.0, pi.GetVariable("orderId"))
	assert.Equal(t, "order1", pi.GetVariable("orderName"))
}

func Test_instance_fails_on_Invalid_Input_mapping(t *testing.T) {
	// setup
	cp := CallPath{}

	// give
	process, _ := bpmnEngine.LoadFromFile("./test-cases/service-task-invalid-input.bpmn")
	bpmnEngine.NewTaskHandler().Id("invalid-input").Handler(cp.TaskHandler)

	// when
	pi, err := bpmnEngine.CreateAndRunInstance(process.Key, nil)
	assert.Nil(t, err)

	// then
	jobs := make([]runtime.Job, 0)
	for _, job := range engineStorage.Jobs {
		if job.ProcessInstanceKey == pi.Key {
			jobs = append(jobs, job)
		}
	}
	assert.Equal(t, "", cp.CallPath)
	assert.Nil(t, pi.GetVariable("id"))
	assert.Equal(t, runtime.ActivityStateFailed, jobs[0].State)
	assert.Equal(t, runtime.ActivityStateFailed, pi.GetState())
}

func Test_job_fails_on_Invalid_Output_mapping(t *testing.T) {
	// setup
	cp := CallPath{}

	// give
	process, _ := bpmnEngine.LoadFromFile("./test-cases/service-task-invalid-output.bpmn")
	bpmnEngine.NewTaskHandler().Id("invalid-output").Handler(cp.TaskHandler)

	// when
	pi, err := bpmnEngine.CreateAndRunInstance(process.Key, nil)
	assert.Nil(t, err)

	// then
	assert.Equal(t, "invalid-output", cp.CallPath)
	assert.Nil(t, pi.GetVariable("order"))

	jobs := make([]runtime.Job, 0)
	for _, job := range engineStorage.Jobs {
		if job.ProcessInstanceKey == pi.Key {
			jobs = append(jobs, job)
		}
	}
	assert.Equal(t, runtime.ActivityStateFailed.String(), jobs[0].State.String())
	assert.Equal(t, runtime.ActivityStateFailed.String(), pi.GetState().String())
}

func Test_task_type_handler(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	cp := CallPath{}

	// give
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-task-with-type.bpmn")
	bpmnEngine.NewTaskHandler().Type("foobar").Handler(cp.TaskHandler)

	// when
	pi, err := bpmnEngine.CreateAndRunInstance(process.Key, nil)
	assert.Nil(t, err)

	// then
	assert.Equal(t, "id", cp.CallPath)
	assert.Equal(t, runtime.ActivityStateCompleted, pi.GetState())
}

func Test_task_type_handler_ID_handler_has_precedence(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
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
	pi, err := bpmnEngine.CreateAndRunInstance(process.Key, nil)
	assert.Nil(t, err)

	// then
	assert.Equal(t, "ID", calledHandler)
	assert.Equal(t, runtime.ActivityStateCompleted, pi.GetState())
}

func Test_just_one_handler_called(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	cp := CallPath{}
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-task-with-type.bpmn")

	// given multiple matching handlers executed
	bpmnEngine.NewTaskHandler().Id("id").Handler(cp.TaskHandler)
	bpmnEngine.NewTaskHandler().Id("id").Handler(cp.TaskHandler)
	bpmnEngine.NewTaskHandler().Type("foobar").Handler(cp.TaskHandler)

	// when
	pi, err := bpmnEngine.CreateAndRunInstance(process.Key, nil)
	assert.Nil(t, err)

	// then
	assert.Equal(t, "id", cp.CallPath, "just one execution")
	assert.Equal(t, runtime.ActivityStateCompleted, pi.GetState())
}

func Test_assignee_and_candidate_groups_are_assigned_to_handler(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	cp := CallPath{}
	process, _ := bpmnEngine.LoadFromFile("./test-cases/user-tasks-with-assignments.bpmn")

	// given multiple matching handlers executed
	bpmnEngine.NewTaskHandler().Assignee("john.doe").Handler(cp.TaskHandler)
	bpmnEngine.NewTaskHandler().CandidateGroups("marketing", "support").Handler(cp.TaskHandler)

	// when
	pi, err := bpmnEngine.CreateAndRunInstance(process.Key, nil)
	assert.Nil(t, err)

	// then
	assert.Equal(t, "assignee-task,group-task", cp.CallPath)
	assert.Equal(t, runtime.ActivityStateCompleted, pi.GetState())
}

func Test_task_default_all_output_variables_map_to_process_instance(t *testing.T) {
	// setup
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple_task-no_output_mapping.bpmn")
	bpmnEngine.clearTaskHandlers()
	bpmnEngine.NewTaskHandler().Id("id").Handler(func(job ActivatedJob) {
		job.SetVariable("aVariable", true)
		job.Complete()
	})

	instance, _ := bpmnEngine.CreateAndRunInstance(process.Key, nil)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.State)

	assert.True(t, instance.GetVariable("aVariable").(bool))
}

func Test_task_no_output_variables_mapping_on_failure(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple_task-no_output_mapping.bpmn")
	bpmnEngine.NewTaskHandler().Id("id").Handler(func(job ActivatedJob) {
		job.SetVariable("aVariable", true)
		job.Fail("because I can")
	})

	instance, _ := bpmnEngine.CreateAndRunInstance(process.Key, nil)
	assert.Equal(t, runtime.ActivityStateActive, instance.State)
	assert.Nil(t, instance.GetVariable("aVariable"))
}

func Test_task_just_declared_output_variables_map_to_process_instance(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple_task-with_output_mapping.bpmn")
	bpmnEngine.NewTaskHandler().Id("id").Handler(func(job ActivatedJob) {
		job.SetVariable("valueFromHandler", true)
		job.SetVariable("otherVariable", "value")
		job.Complete()
	})

	instance, _ := bpmnEngine.CreateAndRunInstance(process.Key, nil)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.State)

	assert.True(t, instance.GetVariable("valueFromHandler").(bool))
	assert.Nil(t, instance.GetVariable("otherVariable"))
}

func Test_missing_task_handlers_break_execution_and_can_be_continued_later(t *testing.T) {
	// TODO: flaky test...sometimes the call path is id-a-1,id-b-2,id-b-1
	t.Skip("TODO: re-enable once refactoring is done")

	cp := CallPath{}
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	process, _ := bpmnEngine.LoadFromFile("./test-cases/parallel-gateway-flow.bpmn")

	// given
	bpmnEngine.NewTaskHandler().Id("id-a-1").Handler(cp.TaskHandler)
	instance, err := bpmnEngine.CreateAndRunInstance(process.Key, nil)
	assert.Nil(t, err)
	assert.Equal(t, runtime.ActivityStateActive, instance.State)
	assert.Equal(t, "id-a-1", cp.CallPath)

	// when
	bpmnEngine.NewTaskHandler().Id("id-b-1").Handler(cp.TaskHandler)
	bpmnEngine.NewTaskHandler().Id("id-b-2").Handler(cp.TaskHandler)
	instance, err = bpmnEngine.RunOrContinueInstance(instance.GetInstanceKey())
	assert.NotNil(t, instance)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.State)

	// then
	assert.Nil(t, err)
	assert.Equal(t, "id-a-1,id-b-1,id-b-2", cp.CallPath)
}
