package bpmn

import (
	"context"
	"fmt"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

func (engine *Engine) createInternalTask(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, element bpmn20.InternalTask, currentToken runtime.ExecutionToken) (state runtime.ActivityState, retErr error) {
	jobVarHolder := runtime.NewVariableHolder(&instance.ProcessInstance().VariableHolder, nil)
	if err := jobVarHolder.EvaluateAndSetMappingsToLocalVariables(element.GetInputMapping(), engine.evaluateExpression); err != nil {
		return runtime.ActivityStateFailed, fmt.Errorf("failed to evaluate input variables: %w", err)
	}
	job := runtime.Job{
		ElementId:          currentToken.ElementId,
		ElementInstanceKey: currentToken.ElementInstanceKey,
		ProcessInstanceKey: currentToken.ProcessInstanceKey,
		Key:                engine.generateKey(),
		Type:               element.GetTaskType(),
		State:              runtime.ActivityStateActive,
		Variables:          jobVarHolder.LocalVariables(),
		CreatedAt:          time.Now(),
		Token:              currentToken,
	}
	batch.SaveJob(ctx, job)
	batch.SaveFlowElementInstance(ctx, runtime.FlowElementInstance{
		Key:                currentToken.ElementInstanceKey,
		ProcessInstanceKey: instance.ProcessInstance().Key,
		ElementId:          element.GetId(),
		CreatedAt:          time.Now(),
		ExecutionTokenKey:  currentToken.Key,
		InputVariables:     jobVarHolder.LocalVariables(),
		OutputVariables:    nil,
	})
	//TODO: check this from main
	// Only evaluate assignee for UserTask elements
	if userTask, ok := element.(bpmn20.UserTask); ok {
		assigneeResult, err := engine.evaluateExpression(userTask.GetAssignmentAssignee(), jobVarHolder.LocalVariables())
		if err != nil {
			job.State = runtime.ActivityStateFailed
			return job.State, fmt.Errorf("failed to create job: %w", err)
		}

		// Cast the result to string
		assigneeStr, ok := assigneeResult.(string)
		if !ok {
			assigneeStr = fmt.Sprintf("%v", assigneeResult)
		}
		job.Assignee = ptr.To(assigneeStr)
	}

	err := batch.SaveJob(ctx, job)
	if err != nil {
		job.State = runtime.ActivityStateFailed
		return job.State, fmt.Errorf("failed to create job: %w", err)
	}

	handler := engine.findTaskHandler(element)
	if handler == nil {
		engine.metrics.JobsCreated.Add(ctx, 1, metric.WithAttributes(attribute.String("type", element.GetTaskType()), attribute.Bool("internal", false)))
		return job.State, nil
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
			output, err := jobVarHolder.PropagateOutputVariablesToParent(element.GetOutputMapping(), activatedJob.GetOutputVariables(), engine.evaluateExpression)
			if err != nil {
				job.State = runtime.ActivityStateFailed
				jobError = newEngineErrorf("failing internal job with message: %s", err)
			} else {
				job.State = runtime.ActivityStateCompleted
				batch.UpdateOutputFlowElementInstance(ctx,
					runtime.FlowElementInstance{
						Key:             currentToken.ElementInstanceKey,
						OutputVariables: output,
					},
				)
			}
			batch.SaveJob(ctx, job)
			engine.metrics.JobsCompleted.Add(ctx, 1, metric.WithAttributes(attribute.String("type", element.GetTaskType()), attribute.Bool("internal", true)))
		}
	}

	return job.State, jobError
}
