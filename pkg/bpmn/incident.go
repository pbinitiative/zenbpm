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
	incident, instance, err := handleIncident(ctx, engine, key, err)
	if err != nil {
		return err
	}

	// TODO: make sure that process instance is not running and if so modify currently running instance
	engine.runProcessInstance(ctx, &instance, []runtime.ExecutionToken{incident.Token})
	return nil
}

func handleIncident(ctx context.Context, engine *Engine, key int64, err error) (runtime.Incident, runtime.ProcessInstance, error) {
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
		return runtime.Incident{}, runtime.ProcessInstance{}, errors.Join(newEngineErrorf("failed to find incident with key: %d", key), err)
	}

	resoveIncidentSpan.SetAttributes(
		attribute.Int64(otelPkg.AttributeIncidentKey, incident.Key),
		attribute.Int64(otelPkg.AttributeProcessInstanceKey, incident.ProcessInstanceKey),
		attribute.Int64(otelPkg.AttributeToken, incident.Token.Key),
	)

	if incident.ResolvedAt != nil {
		return runtime.Incident{}, runtime.ProcessInstance{}, errors.New("incident already resolved")
	}

	instance, err := engine.persistence.FindProcessInstanceByKey(ctx, incident.ProcessInstanceKey)
	if err != nil {
		return runtime.Incident{}, runtime.ProcessInstance{}, errors.Join(newEngineErrorf("failed to find process instance with key: %d", incident.ProcessInstanceKey), err)
	}

	batch := engine.persistence.NewBatch()
	incident.ResolvedAt = ptr.To(time.Now())
	batch.SaveIncident(ctx, incident)

	instance.State = runtime.ActivityStateActive
	batch.SaveProcessInstance(ctx, instance)

	incident.Token.State = runtime.TokenStateRunning
	batch.SaveToken(ctx, incident.Token)

	err = batch.Flush(ctx)
	if err != nil {
		return runtime.Incident{}, runtime.ProcessInstance{}, errors.Join(newEngineErrorf("failed to complete incident with key: %d", key), err)
	}

	return incident, instance, nil
}
