// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

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

func Test_a_job_can_fail_and_moves_process_to_failed_state(t *testing.T) {
	// setup
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")
	h := bpmnEngine.NewTaskHandler().Id("id").Handler(jobFailHandler)
	defer bpmnEngine.RemoveHandler(h)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	incidents, err := bpmnEngine.persistence.FindIncidentsByProcessInstanceKey(t.Context(), instance.Key)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(incidents))

	assert.Equal(t, runtime.ActivityStateFailed, instance.State)
}

// Test_simple_count_loop requires correct Task-Output-Mapping in the BPMN file
func Test_simple_count_loop(t *testing.T) {
	// setup
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-count-loop.bpmn")
	h := bpmnEngine.NewTaskHandler().Id("id-increaseCounter").Handler(increaseCounterHandler)
	defer bpmnEngine.RemoveHandler(h)

	vars := map[string]interface{}{}
	vars[varCounter] = 0.0
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, vars)
	assert.NoError(t, err)

	assert.Equal(t, 4.0, instance.GetVariable(varCounter))
	assert.Equal(t, runtime.ActivityStateCompleted, instance.State)
}

func Test_simple_count_loop_with_message(t *testing.T) {
	cleanUpMessageSubscriptions()
	// setup
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-count-loop-with-message.bpmn")

	vars := map[string]interface{}{}
	vars[varEngineValidationAttempts] = 0.0
	nothingH := bpmnEngine.NewTaskHandler().Id("do-nothing").Handler(jobCompleteHandler)
	defer bpmnEngine.RemoveHandler(nothingH)
	validate := bpmnEngine.NewTaskHandler().Id("validate").Handler(func(job ActivatedJob) {
		attemptsVariable := job.Variable(varEngineValidationAttempts)
		attempts := attemptsVariable.(float64)
		foobar := attempts >= 1
		attempts++
		job.SetVariable(varEngineValidationAttempts, attempts)
		job.SetVariable(varHasReachedMaxAttempts, foobar)
		job.Complete()
	})
	defer bpmnEngine.RemoveHandler(validate)

	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, vars) // should stop at the intermediate message catch event
	assert.NoError(t, err)

	for _, message := range engineStorage.MessageSubscriptions {
		if message.Name == "msg" {
			err := bpmnEngine.PublishMessage(t.Context(), message.Key, nil)
			assert.NoError(t, err)
		}
	}

	for _, message := range engineStorage.MessageSubscriptions {
		if message.Name == "msg" && message.State == runtime.ActivityStateActive {
			err := bpmnEngine.PublishMessage(t.Context(), message.Key, nil)
			assert.NoError(t, err)
		}
	}

	*instance, err = bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.Key)
	assert.NoError(t, err)

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
	assert.Equal(t, runtime.ActivityStateCompleted, messages[0].State)
	assert.Equal(t, runtime.ActivityStateCompleted, messages[1].State)
}

func Test_activated_job_data(t *testing.T) {
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple_task.bpmn")

	h := bpmnEngine.NewTaskHandler().Id("id").Handler(func(aj ActivatedJob) {
		assert.NotEmpty(t, aj.ElementId())
		assert.NotNil(t, aj.CreatedAt())
		assert.NotEqual(t, int64(0), aj.Key())
		assert.NotEmpty(t, aj.BpmnProcessId())
		assert.NotEqual(t, int64(0), aj.ProcessDefinitionKey())
		assert.NotEqual(t, int32(0), aj.ProcessDefinitionVersion())
		assert.NotEqual(t, int64(0), aj.ProcessInstanceKey())
	})
	defer bpmnEngine.RemoveHandler(h)

	instance, _ := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)

	assert.Equal(t, runtime.ActivityStateActive, instance.State)
}

func Test_task_InputOutput_mapping_happy_path(t *testing.T) {
	// setup
	cp := CallPath{}

	// give
	process, _ := bpmnEngine.LoadFromFile("./test-cases/service-task-input-output.bpmn")
	st1 := bpmnEngine.NewTaskHandler().Id("service-task-1").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(st1)
	ut1 := bpmnEngine.NewTaskHandler().Id("user-task-2").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(ut1)

	// when
	pi, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
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
	assert.Equal(t, runtime.ActivityStateCompleted, pi.State)
}

func Test_instance_fails_on_Invalid_Input_mapping(t *testing.T) {
	// setup
	cp := CallPath{}

	// give
	process, _ := bpmnEngine.LoadFromFile("./test-cases/service-task-invalid-input.bpmn")
	h := bpmnEngine.NewTaskHandler().Id("invalid-input").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(h)

	// when
	pi, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.ErrorContains(t, err, "failed to evaluate input variables")

	// then
	jobs := make([]runtime.Job, 0)
	for _, job := range engineStorage.Jobs {
		if job.ProcessInstanceKey == pi.Key {
			jobs = append(jobs, job)
		}
	}
	assert.Equal(t, "", cp.CallPath)
	assert.Len(t, jobs, 0)
	assert.Equal(t, runtime.ActivityStateFailed, pi.GetState())
}

func Test_job_fails_on_Invalid_Output_mapping(t *testing.T) {
	// setup
	cp := CallPath{}

	// give
	process, _ := bpmnEngine.LoadFromFile("./test-cases/service-task-invalid-output.bpmn")
	h := bpmnEngine.NewTaskHandler().Id("invalid-output").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(h)

	// when
	pi, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
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
	h := bpmnEngine.NewTaskHandler().Type("foobar").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(h)

	// when
	pi, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
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
	foobarH := bpmnEngine.NewTaskHandler().Type("foobar").Handler(typeHandler)
	defer bpmnEngine.RemoveHandler(foobarH)
	idH := bpmnEngine.NewTaskHandler().Id("id").Handler(idHandler)
	defer bpmnEngine.RemoveHandler(idH)

	// when
	pi, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
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
	id1H := bpmnEngine.NewTaskHandler().Id("id").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(id1H)
	id2H := bpmnEngine.NewTaskHandler().Id("id").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(id2H)
	foobarH := bpmnEngine.NewTaskHandler().Type("foobar").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(foobarH)

	// when
	pi, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
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
	johnH := bpmnEngine.NewTaskHandler().Assignee("john.doe").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(johnH)
	markH := bpmnEngine.NewTaskHandler().CandidateGroups("marketing", "support").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(markH)

	// when
	pi, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.Nil(t, err)

	// then
	assert.Equal(t, "assignee-task,group-task", cp.CallPath)
	assert.Equal(t, runtime.ActivityStateCompleted, pi.GetState())
}

func Test_task_default_all_output_variables_map_to_process_instance(t *testing.T) {
	// setup
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple_task-no_output_mapping.bpmn")
	h := bpmnEngine.NewTaskHandler().Id("id").Handler(func(job ActivatedJob) {
		job.SetVariable("aVariable", true)
		job.Complete()
	})
	defer bpmnEngine.RemoveHandler(h)

	instance, _ := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.State)

	assert.True(t, instance.GetVariable("aVariable").(bool))
}

func Test_task_no_output_variables_mapping_on_failure(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple_task-no_output_mapping.bpmn")
	h := bpmnEngine.NewTaskHandler().Id("id").Handler(func(job ActivatedJob) {
		job.SetVariable("aVariable", true)
		job.Fail("because I can")
	})
	defer bpmnEngine.RemoveHandler(h)

	instance, _ := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.Equal(t, runtime.ActivityStateFailed, instance.State)
	assert.Nil(t, instance.GetVariable("aVariable"))
}

func Test_task_just_declared_output_variables_map_to_process_instance(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple_task-with_output_mapping.bpmn")
	h := bpmnEngine.NewTaskHandler().Id("id").Handler(func(job ActivatedJob) {
		job.SetVariable("valueFromHandler", true)
		job.SetVariable("otherVariable", "value")
		job.Complete()
	})
	defer bpmnEngine.RemoveHandler(h)

	instance, _ := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
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
	ah := bpmnEngine.NewTaskHandler().Id("id-a-1").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(ah)
	instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.Nil(t, err)
	assert.Equal(t, runtime.ActivityStateActive, instance.State)
	assert.Equal(t, "id-a-1", cp.CallPath)

	// when
	bh := bpmnEngine.NewTaskHandler().Id("id-b-1").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(bh)
	b2h := bpmnEngine.NewTaskHandler().Id("id-b-2").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(b2h)
	tokens, err := bpmnEngine.persistence.GetTokensForProcessInstance(t.Context(), instance.Key)
	assert.NoError(t, err)
	bpmnEngine.runProcessInstance(t.Context(), instance, tokens)
	assert.NotNil(t, instance)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.State)

	// then
	assert.Nil(t, err)
	assert.Equal(t, "id-a-1,id-b-1,id-b-2", cp.CallPath)
}

func TestJobCompleteIsHandledCorrectly(t *testing.T) {
	process, _ := bpmnEngine.LoadFromFile("./test-cases/service-task-input-output.bpmn")

	pi, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.Nil(t, err)

	foundServiceJob := runtime.Job{}
	for _, job := range engineStorage.Jobs {
		if job.ProcessInstanceKey == pi.Key && job.ElementId == "service-task-1" {
			foundServiceJob = job
			break
		}
	}
	assert.NotZero(t, foundServiceJob, "expected to find service-task-1 job created for process instance")

	err = bpmnEngine.JobCompleteByKey(t.Context(), foundServiceJob.Key, foundServiceJob.Variables)
	assert.NoError(t, err)

	serviceToken := runtime.ExecutionToken{}
	for _, tok := range engineStorage.ExecutionTokens {
		if tok.Key == foundServiceJob.Token.Key {
			serviceToken = tok
		}
	}
	assert.NotZero(t, serviceToken, "expected to find token from service-task-1 job")
	assert.NotEqual(t, foundServiceJob.ElementId, serviceToken.ElementId)

	foundUserJob := runtime.Job{}
	for _, job := range engineStorage.Jobs {
		if job.ProcessInstanceKey == pi.Key && job.ElementId == "user-task-2" {
			foundUserJob = job
			break
		}
	}
	assert.NotZero(t, foundUserJob, "expected to find user-task-2 job created for process instance")

	err = bpmnEngine.JobCompleteByKey(t.Context(), foundUserJob.Key, foundUserJob.Variables)
	assert.NoError(t, err)

	userToken := runtime.ExecutionToken{}
	for _, tok := range engineStorage.ExecutionTokens {
		if tok.Key == foundUserJob.Token.Key {
			userToken = tok
		}
	}
	assert.NotZero(t, userToken, "expected to find token from user-task-2 job")
	assert.NotEqual(t, foundUserJob.ElementId, userToken.ElementId)

	*pi, err = engineStorage.FindProcessInstanceByKey(t.Context(), pi.Key)
	assert.NoError(t, err)

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
	assert.Equal(t, runtime.ActivityStateCompleted, pi.State)
}

func TestJobFailIsHandledCorrectly(t *testing.T) {
	process, _ := bpmnEngine.LoadFromFile("./test-cases/service-task-input-output.bpmn")

	pi, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.Nil(t, err)

	foundServiceJob := runtime.Job{}
	for _, job := range engineStorage.Jobs {
		if job.ProcessInstanceKey == pi.Key && job.ElementId == "service-task-1" {
			foundServiceJob = job
			break
		}
	}
	assert.NotZero(t, foundServiceJob, "expected to find service-task-1 job created for process instance")

	err = bpmnEngine.JobFailByKey(t.Context(), foundServiceJob.Key, "testing fail job", nil, nil)
	assert.NoError(t, err)

	for _, job := range engineStorage.Jobs {
		if job.ProcessInstanceKey == pi.Key && job.ElementId == "service-task-1" {
			foundServiceJob = job
			break
		}
	}

	assert.Equal(t, foundServiceJob.State, runtime.ActivityStateFailed)

	var incidents []runtime.Incident
	incidents, err = engineStorage.FindIncidentsByProcessInstanceKey(t.Context(), pi.Key)
	assert.NoError(t, err)
	assert.Len(t, incidents, 1)
	assert.Contains(t, incidents[0].Message, "testing fail job")

}

func TestBusinessRuleTaskExternalActivated(t *testing.T) {
	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-business-rule-task-external.bpmn")

	h := bpmnEngine.NewTaskHandler().Type("test-business-rule-task-job").Handler(func(aj ActivatedJob) {
		assert.NotEmpty(t, aj.ElementId())
		assert.NotNil(t, aj.CreatedAt())
		assert.NotEqual(t, int64(0), aj.Key())
		assert.NotEmpty(t, aj.BpmnProcessId())
		assert.NotEqual(t, int64(0), aj.ProcessDefinitionKey())
		assert.NotEqual(t, int32(0), aj.ProcessDefinitionVersion())
		assert.NotEqual(t, int64(0), aj.ProcessInstanceKey())
	})
	defer bpmnEngine.RemoveHandler(h)

	instance, _ := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)

	assert.Equal(t, runtime.ActivityStateActive, instance.State)
}

func TestBusinessRuleTaskExternalComplete(t *testing.T) {
	cp := CallPath{}

	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-business-rule-task-external.bpmn")

	st1 := bpmnEngine.NewTaskHandler().Id("BusinessRuleTask1").Handler(cp.TaskHandler)
	defer bpmnEngine.RemoveHandler(st1)

	instance, _ := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)

	assert.Equal(t, "BusinessRuleTask1", cp.CallPath)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.State)
}
