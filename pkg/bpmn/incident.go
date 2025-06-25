package bpmn

import (
	"context"
	"errors"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
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

func (engine *Engine) ResolveIncident(ctx context.Context, key int64) error {
	incident, err := engine.persistence.FindIncidentByKey(ctx, key)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to find incident with key: %d", key), err)
	}

	instance, err := engine.persistence.FindProcessInstanceByKey(ctx, incident.ProcessInstanceKey)
	if err != nil {
		return errors.Join(newEngineErrorf("failed to find process instance with key: %d", incident.ProcessInstanceKey), err)
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
		return errors.Join(newEngineErrorf("failed to complete incident with key: %d", key), err)
	}

	// TODO: make sure that process instance is not running and if so modify currently running instance
	engine.runProcessInstance(ctx, &instance, []runtime.ExecutionToken{incident.Token})
	return nil
}
