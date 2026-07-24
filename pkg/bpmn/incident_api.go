package bpmn

import (
	"context"
	"fmt"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	otelPkg "github.com/pbinitiative/zenbpm/pkg/otel"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

func createNewIncidentFromToken(err error, token runtime.ExecutionToken, engine *Engine) runtime.Incident {
	return runtime.Incident{
		Key:                engine.generateKey(),
		ElementInstanceKey: token.ElementInstanceKey,
		ElementId:          token.ElementId,
		ProcessInstanceKey: token.ProcessInstanceKey,
		Message:            err.Error(),
		CreatedAt:          time.Now(),
		Token:              token,
		ResolvedAt:         nil,
	}
}

func (engine *Engine) retryEventSubprocessSubscriptionIncident(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, incident runtime.Incident) error {
	processDefinition := instance.ProcessInstance().Definition
	if processDefinition == nil {
		return fmt.Errorf("process instance %d has no process definition", instance.ProcessInstance().Key)
	}

	subProcess, startEvent := processDefinition.Definitions.Process.GetSubprocessAndStartEventById(incident.ElementId)
	if subProcess == nil || startEvent == nil {
		return fmt.Errorf("failed to find event subprocess start event %s in process definition %d", incident.ElementId, processDefinition.Key)
	}

	return engine.createStartEventSubscriptions(ctx, batch, subProcess.TProcess, *processDefinition, &instance)
}

func (engine *Engine) reevaluateJobInputVariables(ctx context.Context, batch *EngineBatch, instance runtime.ProcessInstance, job *runtime.Job) error {
	element := instance.ProcessInstance().Definition.Definitions.Process.GetFlowNodeById(job.ElementId)
	task, ok := element.(bpmn20.InternalTask)
	if !ok {
		return fmt.Errorf("failed to find task %s for job %d", job.ElementId, job.Key)
	}

	flowElementInstance, err := engine.persistence.GetFlowElementInstanceByKey(ctx, job.ElementInstanceKey)
	if err != nil {
		return fmt.Errorf("failed to find flow element instance %d for job %d: %w", job.ElementInstanceKey, job.Key, err)
	}

	variableHolder := runtime.NewVariableHolder(&instance.ProcessInstance().VariableHolder, nil)
	if activity, ok := element.(bpmn20.Activity); ok && activity.GetMultiInstance() != nil {
		inputElementName := activity.GetMultiInstance().LoopCharacteristics.InputElementName
		inputElement, ok := flowElementInstance.InputVariables[inputElementName]
		if !ok {
			return fmt.Errorf("failed to find multi-instance input variable %s for job %d", inputElementName, job.Key)
		}
		variableHolder.SetLocalVariable(inputElementName, inputElement)
	}

	flowElementInput := variableHolder.ExecutionScopeSnapshot()
	if err := variableHolder.EvaluateAndSetMappingsToLocalVariables(task.GetInputMapping(), engine.evaluateExpression); err != nil {
		return fmt.Errorf("failed to evaluate input variables for job %d: %w", job.Key, err)
	}
	job.InputVariables = variableHolder.LocalVariables()

	flowElementInstance.InputVariables = flowElementInput
	if err := batch.SaveFlowElementInstance(ctx, flowElementInstance); err != nil {
		return fmt.Errorf("failed to update input variables for flow element instance %d: %w", flowElementInstance.Key, err)
	}
	return nil
}

func (engine *Engine) ResolveIncident(ctx context.Context, key int64) (retErr error) {
	ctx, resoveIncidentSpan := engine.tracer.Start(ctx, fmt.Sprintf("incident:%d", key))
	defer func() {
		if retErr != nil {
			resoveIncidentSpan.RecordError(retErr)
			resoveIncidentSpan.SetStatus(codes.Error, retErr.Error())
		}
		resoveIncidentSpan.End()
	}()

	incident, err := engine.persistence.FindIncidentByKey(ctx, key)
	if err != nil {
		return fmt.Errorf("%w: %w", newEngineErrorf("failed to find incident with key %d", key), err)
	}

	resoveIncidentSpan.SetAttributes(
		attribute.Int64(otelPkg.AttributeIncidentKey, incident.Key),
		attribute.Int64(otelPkg.AttributeProcessInstanceKey, incident.ProcessInstanceKey),
		attribute.Int64(otelPkg.AttributeToken, incident.Token.Key),
	)

	if incident.ResolvedAt != nil {
		return newEngineErrorf("incident already resolved")
	}

	instance, err := engine.persistence.FindProcessInstanceByKey(ctx, incident.ProcessInstanceKey)
	if err != nil {
		return fmt.Errorf("%w: %w", newEngineErrorf("failed to find process instance with key %d", incident.ProcessInstanceKey), err)
	}

	batch, err := engine.NewEngineBatch(ctx, instance)
	if err != nil {
		return newEngineErrorf("failed to create engine batch")
	}
	defer func() {
		if retErr != nil {
			batch.Clear(ctx)
		}
	}()

	//refresh
	incident, err = engine.persistence.FindIncidentByKey(ctx, key)
	if err != nil {
		return fmt.Errorf("%w: %w", newEngineErrorf("failed to find incident with key %d", key), err)
	}
	if incident.ResolvedAt != nil {
		return newEngineErrorf("incident with key %d was already resolved", key)
	}

	if incident.Token.Key == 0 {
		if err := engine.retryEventSubprocessSubscriptionIncident(ctx, &batch, instance, incident); err != nil {
			return fmt.Errorf("failed to recreate event subprocess subscription for incident %d: %w", key, err)
		}
		incident.ResolvedAt = ptr.To(time.Now())
		if err := batch.SaveIncident(ctx, incident); err != nil {
			return fmt.Errorf("failed to save resolved incident %d: %w", incident.Key, err)
		}
		if err := batch.Flush(ctx); err != nil {
			return newEngineErrorf("failed to complete incident with key: %d", key)
		}
		return nil
	}

	// FindIncidentByKey keeps dangling token-bound incidents readable for history/listing purposes,
	// but resolving such an incident requires the current persisted token.
	incident.Token, err = engine.persistence.GetTokenByKey(ctx, incident.Token.Key)
	if err != nil {
		return fmt.Errorf("failed to find execution token %d for incident %d: %w", incident.Token.Key, key, err)
	}

	jobs, err := engine.persistence.FindPendingProcessInstanceJobs(ctx, incident.ProcessInstanceKey)
	if err != nil {
		return newEngineErrorf("failed to find jobs for token key: %d", incident.Token.Key)
	}

	incident.ResolvedAt = ptr.To(time.Now())
	err = batch.SaveIncident(ctx, incident)
	if err != nil {
		return err
	}

	// Checking for linked jobs as these need to be resolved as well
	var job *runtime.Job
	for _, j := range jobs {
		if j.Token.Key == incident.Token.Key {
			job = &j
			break
		}
	}

	// TODO: the same thing has to happen for other waiting subscriptions
	if job != nil {
		if err := engine.reevaluateJobInputVariables(ctx, &batch, instance, job); err != nil {
			return err
		}
		incident.Token.State = runtime.TokenStateWaiting
		job.State = runtime.ActivityStateActive
		err := batch.SaveJob(ctx, *job)
		if err != nil {
			return err
		}
	} else {
		incident.Token.State = runtime.TokenStateRunning
	}
	err = batch.SaveToken(ctx, incident.Token)
	if err != nil {
		return err
	}

	instance.ProcessInstance().State = runtime.ActivityStateActive
	err = batch.SaveProcessInstance(ctx, instance)
	if err != nil {
		return fmt.Errorf("failed to save changes to process instance %d: %w", instance.ProcessInstance().Key, err)
	}

	err = batch.Flush(ctx)
	if err != nil {
		return newEngineErrorf("failed to complete incident with key: %d", key)
	}

	err = engine.RunProcessInstance(ctx, instance, []runtime.ExecutionToken{incident.Token})
	if err != nil {
		return err
	}
	return nil
}

func (engine *Engine) resolveIncidentsForToken(ctx context.Context, batch *EngineBatch, tokenKey int64) error {

	incidents, err := engine.persistence.FindIncidentsByExecutionTokenKey(ctx, tokenKey)
	if err != nil {
		return fmt.Errorf("failed to find incidents for execution token %d: %w", tokenKey, err)
	}

	for _, incident := range incidents {
		if incident.ResolvedAt != nil {
			continue
		}

		incident.ResolvedAt = ptr.To(time.Now())
		err = batch.SaveIncident(ctx, incident)
		if err != nil {
			return fmt.Errorf("failed to save changes to incident %d: %w", incident.Key, err)
		}
	}

	return nil
}
