package bpmn

import (
	"context"
	"fmt"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

// createInternalTask creates the job of an element which is entered and left
// in one go: it evaluates the element's input mappings into the given
// variables, creates the job (createJob), and writes the element's history
// row with the variables as they were before the mappings.
func (engine *Engine) createInternalTask(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	element bpmn20.InternalTask,
	currentToken runtime.ExecutionToken,
	jobVarHolder runtime.VariableHolder,
) (runtime.ActivityState, error) {
	created := jobCreation{}
	// the element of a listener saved its history row before, and the
	// listener's job carries the variables the row would copy
	if _, isListener := element.(*bpmn20.ListenerTask); !isListener {
		created.writesHistoryInput = true
		created.historyInput = jobVarHolder.ExecutionScopeSnapshot()
	}
	if err := jobVarHolder.EvaluateAndSetMappingsToLocalVariables(element.GetInputMapping(), engine.evaluateExpression); err != nil {
		return runtime.ActivityStateFailed, fmt.Errorf("failed to evaluate input variables: %w", err)
	}
	job, err := engine.createJob(ctx, batch, instance, element, currentToken, jobVarHolder, created)
	return job.State, err
}

// jobCreation says what createJob does beside creating the job.
type jobCreation struct {
	// writesHistoryInput has createJob write the element's history row, with
	// historyInput as its input variables. It is false where the row was
	// written when the element was entered.
	writesHistoryInput bool
	historyInput       map[string]any
}

// createJob creates the job of a task, or of an execution listener, with the
// given variables as they are: every input mapping was evaluated before. A
// task handler registered in the engine completes or fails the job of a task
// at once; a listener's job is always left to an external worker. The job is
// answered in the state it was left in: active while it waits for a worker,
// completed or failed after a handler in the engine.
func (engine *Engine) createJob(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	element bpmn20.InternalTask,
	currentToken runtime.ExecutionToken,
	jobVarHolder runtime.VariableHolder,
	created jobCreation,
) (job runtime.Job, retErr error) {
	failed := runtime.Job{State: runtime.ActivityStateFailed}
	listener, isListener := element.(*bpmn20.ListenerTask)
	// taskHeaders are static worker parameters; only elements that declare the
	// extension provide them (see bpmn20.TaskHeaderProvider).
	var taskHeaders []extensions.THeader
	if provider, ok := element.(bpmn20.TaskHeaderProvider); ok {
		taskHeaders = provider.GetTaskHeaders()
	}
	// a listener's job is for external workers only: a handler registered for
	// the id of the listener's element must not complete it inline
	var handler func(job ActivatedJob)
	if isListener {
		if err := ensureListenerCanRun(instance, listener); err != nil {
			return failed, err
		}
	} else {
		handler = engine.findTaskHandler(element)
	}
	// retries are spent by external workers only: a handler in the engine fails
	// the job with an incident at once, so an expression which cannot be
	// evaluated must not fail a task which never reads it. Resolving such an
	// incident evaluates the retries of the job it leaves waiting.
	var retries int32
	var retryBackoff []time.Duration
	if handler == nil {
		// only an expression reads the variables, so literals spare the copy of every scope
		var jobScope map[string]any
		definition := element.GetTaskDefinition()
		if extensions.IsExpression(definition.Retries) || extensions.IsExpression(definition.RetryBackoff) {
			jobScope = jobVarHolder.ExecutionScopeSnapshot()
		}
		var err error
		if retries, err = engine.initialRetries(element, jobScope); err != nil {
			return failed, fmt.Errorf("failed to create job: %w", err)
		}
		if retryBackoff, err = engine.retryBackoffPolicy(element, jobScope); err != nil {
			return failed, fmt.Errorf("failed to create job: %w", err)
		}
	}
	job = runtime.Job{
		ElementId:          currentToken.ElementId,
		ElementType:        string(element.GetType()),
		ElementInstanceKey: currentToken.ElementInstanceKey,
		ProcessInstanceKey: currentToken.ProcessInstanceKey,
		Key:                engine.generateKey(),
		Type:               element.GetTaskType(),
		State:              runtime.ActivityStateActive,
		InputVariables:     jobVarHolder.LocalVariables(),
		Headers:            extensions.HeadersToMap(taskHeaders),
		CreatedAt:          time.Now(),
		Token:              currentToken,
		Retries:            retries,
		RetryBackoff:       retryBackoff,
	}
	if isListener {
		job.Kind = runtime.JobKindExecutionListener
		job.ListenerEventType = listener.EventType()
	}
	// Only evaluate assignee for UserTask elements
	if userTask, ok := element.(bpmn20.UserTask); ok {
		assigneeResult, err := engine.evaluateExpression(userTask.GetAssignmentAssignee(), jobVarHolder.LocalVariables())
		if err != nil {
			job.State = runtime.ActivityStateFailed
			return job, fmt.Errorf("failed to create job: %w", err)
		}

		if assigneeResult != nil {
			assigneeStr, ok := assigneeResult.(string)
			if !ok {
				assigneeStr = fmt.Sprintf("%v", assigneeResult)
			}
			job.Assignee = new(assigneeStr)
		}
	}

	err := batch.SaveJob(ctx, job)
	if err != nil {
		job.State = runtime.ActivityStateFailed
		return job, fmt.Errorf("failed to create job: %w", err)
	}

	if created.writesHistoryInput {
		err = batch.SaveFlowElementInstance(ctx, runtime.FlowElementInstance{
			Key:                currentToken.ElementInstanceKey,
			ProcessInstanceKey: instance.ProcessInstance().Key,
			ElementId:          element.GetId(),
			ElementType:        string(element.GetType()),
			CreatedAt:          time.Now(),
			ExecutionTokenKey:  currentToken.Key,
			InputVariables:     created.historyInput,
			OutputVariables:    nil,
		})
		if err != nil {
			job.State = runtime.ActivityStateFailed
			return job, err
		}
	}

	if handler == nil {
		engine.metrics.JobsCreated.Add(ctx, 1, metric.WithAttributes(attribute.String("type", element.GetTaskType()), attribute.Bool("internal", false)))
		return job, nil
	}
	engine.metrics.JobsCreated.Add(ctx, 1, metric.WithAttributes(attribute.String("type", element.GetTaskType()), attribute.Bool("internal", true)))
	// if we have the handler handle the task directly
	var jobError error
	// TODO: pull this out into function that will be called by API as well
	if job.State != runtime.ActivityStateCompleting {
		job.State = runtime.ActivityStateActive
		var failReason string = ""
		activatedJob := &activatedJob{
			processInstanceInfo: instance,
			failHandler: func(reason string) {
				job.State = runtime.ActivityStateFailing
				failReason = reason
			},
			completeHandler:          func() { job.State = runtime.ActivityStateCompleting },
			key:                      engine.generateKey(),
			processInstanceKey:       instance.ProcessInstance().Key,
			bpmnProcessId:            instance.ProcessInstance().Definition.BpmnProcessId,
			processDefinitionVersion: instance.ProcessInstance().Definition.Version,
			processDefinitionKey:     instance.ProcessInstance().Definition.Key,
			elementId:                job.ElementId,
			createdAt:                job.CreatedAt,
			localVariables:           jobVarHolder.LocalVariables(),
			outputVariables:          map[string]any{},
			headers:                  job.Headers,
		}
		ctx, internalCompleteSpan := engine.tracer.Start(ctx, fmt.Sprintf("job:%s", activatedJob.ElementId()), trace.WithAttributes(
			attribute.Int64("key", activatedJob.key),
		))
		defer func() {
			if retErr != nil {
				internalCompleteSpan.RecordError(retErr)
				internalCompleteSpan.SetStatus(codes.Error, retErr.Error())
			}
			internalCompleteSpan.End()
		}()
		handler(activatedJob)
		switch job.State {
		case runtime.ActivityStateFailing:
			job.State = runtime.ActivityStateFailed
			jobError = newEngineErrorf("failing internal job with message: %s", failReason)
		case runtime.ActivityStateCompleting:
			job.OutputVariables = activatedJob.GetOutputVariables()
			output, err := jobVarHolder.PropagateOnlyMappedOutputs(element.GetOutputMapping(), job.OutputVariables, engine.evaluateExpression)
			if err != nil {
				job.State = runtime.ActivityStateFailed
				jobError = newEngineErrorf("failing internal job with message: %s", err)
			} else {
				job.State = runtime.ActivityStateCompleted
				if updateErr := batch.UpdateOutputFlowElementInstance(ctx,
					runtime.FlowElementInstance{
						Key:                currentToken.ElementInstanceKey,
						ProcessInstanceKey: instance.ProcessInstance().GetInstanceKey(),
						ElementId:          currentToken.ElementId,
						ElementType:        string(element.GetType()),
						ExecutionTokenKey:  currentToken.Key,
						OutputVariables:    output,
						CompletedAt:        new(time.Now()),
					},
				); updateErr != nil {
					job.State = runtime.ActivityStateFailed
					jobError = newEngineErrorf("failed to update flow element instance for internal job: %s", updateErr)
				}
			}
		}
		if job.State == runtime.ActivityStateCompleted {
			if err := batch.SaveJob(ctx, job); err != nil {
				return failed, fmt.Errorf("failed to save terminal state of internal job %d: %w", job.Key, err)
			}
		}
		switch job.State {
		case runtime.ActivityStateCompleted:
			if engine.metrics != nil && engine.metrics.JobsCompleted != nil {
				engine.metrics.JobsCompleted.Add(ctx, 1, metric.WithAttributes(attribute.String("type", element.GetTaskType()), attribute.Bool("internal", true)))
			}
			engine.recordJobLifetime(ctx, job, "completed")
		case runtime.ActivityStateFailed:
			if engine.metrics != nil && engine.metrics.JobsFailed != nil {
				engine.metrics.JobsFailed.Add(ctx, 1, metric.WithAttributes(attribute.String("type", element.GetTaskType()), attribute.Bool("internal", true)))
			}
			engine.recordJobLifetime(ctx, job, "failed")
		}
	}

	return job, jobError
}

func (engine *Engine) terminateActiveJobsForToken(ctx context.Context, batch *EngineBatch, tokenKey int64) error {

	jobs, err := engine.persistence.GetJobsInStateByTokenKey(ctx, tokenKey, []runtime.ActivityState{runtime.ActivityStateActive, runtime.ActivityStateCompleting, runtime.ActivityStateFailed})
	if err != nil {
		return fmt.Errorf("failed to find jobs for execution token %d: %w", tokenKey, err)
	}
	for _, job := range jobs {
		job.State = runtime.ActivityStateTerminated
		err = batch.SaveJob(ctx, job)
		if err != nil {
			return fmt.Errorf("failed to save changes to job %d: %w", job.Key, err)
		}
	}

	return nil
}
