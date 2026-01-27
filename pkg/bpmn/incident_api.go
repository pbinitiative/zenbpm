package bpmn

import (
	"context"
	"errors"
	"fmt"
	"time"

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

func (engine *Engine) ResolveIncident(ctx context.Context, key int64) (err error) {
	ctx, resoveIncidentSpan := engine.tracer.Start(ctx, fmt.Sprintf("incident:%d", key))
	defer func() {
		if err != nil {
			resoveIncidentSpan.RecordError(err)
			resoveIncidentSpan.SetStatus(codes.Error, err.Error())
		}
		resoveIncidentSpan.End()
	}()

	incident, err := engine.persistence.FindIncidentByKey(ctx, key)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to find incident with key: %d", key), err)
	}

	resoveIncidentSpan.SetAttributes(
		attribute.Int64(otelPkg.AttributeIncidentKey, incident.Key),
		attribute.Int64(otelPkg.AttributeProcessInstanceKey, incident.ProcessInstanceKey),
		attribute.Int64(otelPkg.AttributeToken, incident.Token.Key),
	)

	if incident.ResolvedAt != nil {
		return errors.New("incident already resolved")
	}

	instance, err := engine.persistence.FindProcessInstanceByKey(ctx, incident.ProcessInstanceKey)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to find process instance with key: %d", incident.ProcessInstanceKey), err)
	}

	jobs, err := engine.persistence.FindPendingProcessInstanceJobs(ctx, incident.ProcessInstanceKey)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to find jobs for token key: %d", incident.Token.Key), err)
	}

	batch, err := engine.NewEngineBatch(ctx, instance)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to create engine batch"), err)
	}

	incident.ResolvedAt = ptr.To(time.Now())
	batch.SaveIncident(ctx, incident)

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
		incident.Token.State = runtime.TokenStateWaiting
		job.State = runtime.ActivityStateActive
		batch.SaveJob(ctx, *job)
	} else {
		incident.Token.State = runtime.TokenStateRunning
	}
	batch.SaveToken(ctx, incident.Token)

	instance.ProcessInstance().State = runtime.ActivityStateActive
	batch.SaveProcessInstance(ctx, instance)

	err = batch.Flush(ctx)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to complete incident with key: %d", key), err)
	}

	err = engine.RunProcessInstance(ctx, instance, []runtime.ExecutionToken{incident.Token})
	if err != nil {
		return err
	}
	return nil
}
