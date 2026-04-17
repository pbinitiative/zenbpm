package bpmn

import (
	"context"
	"fmt"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

type errorEventContext struct {
	instance      runtime.ProcessInstance
	token         runtime.ExecutionToken
	attachedToRef string
}

// loadParentErrorEventContext returns the error scope of the parent process when
// the given instance is a child (subprocess, call activity, multi-instance).
// Returns nil when there is no parent or the parent already finished.
//
// When lockInBatch is true, the parent instance is locked in the batch —
// callers that intend to mutate parent state must pass true. Read-only
// traversal (e.g. searching for a matching boundary handler) can pass false.
func (engine *Engine) loadParentErrorEventContext(
	ctx context.Context,
	batch *EngineBatch,
	instance runtime.ProcessInstance,
	lockInBatch bool,
) (*errorEventContext, error) {
	parentTokenKey, attachedToRef, expectedElementInstanceKey, hasParent := parentScopeLookup(instance)
	if !hasParent {
		return nil, nil
	}

	parentProcessInstanceKey := *instance.GetParentProcessInstanceKey()
	parentInstance, err := engine.persistence.FindProcessInstanceByKey(ctx, parentProcessInstanceKey)
	if err != nil {
		return nil, fmt.Errorf("failed to find parent process instance %d: %w", parentProcessInstanceKey, err)
	}
	if lockInBatch {
		if err := batch.AddParentLockedInstance(ctx, parentInstance); err != nil {
			return nil, err
		}
	}
	if parentInstance.ProcessInstance().State == runtime.ActivityStateCompleted ||
		parentInstance.ProcessInstance().State == runtime.ActivityStateTerminated {
		return nil, nil
	}

	parentToken, err := engine.persistence.GetTokenByKey(ctx, parentTokenKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get parent token %d: %w", parentTokenKey, err)
	}
	if parentToken.ElementInstanceKey != expectedElementInstanceKey {
		return nil, nil
	}

	return &errorEventContext{
		instance:      parentInstance,
		token:         parentToken,
		attachedToRef: attachedToRef,
	}, nil
}

func parentScopeLookup(instance runtime.ProcessInstance) (
	parentTokenKey int64,
	attachedToRef string,
	expectedElementInstanceKey int64,
	hasParent bool,
) {
	switch inst := instance.(type) {
	case *runtime.CallActivityInstance:
		return inst.ParentProcessExecutionToken.Key,
			inst.ParentProcessExecutionToken.ElementId,
			inst.ParentProcessTargetElementInstanceKey,
			true
	case *runtime.SubProcessInstance:
		return inst.ParentProcessExecutionToken.Key,
			inst.ParentProcessTargetElementId,
			inst.ParentProcessTargetElementInstanceKey,
			true
	case *runtime.MultiInstanceInstance:
		return inst.ParentProcessExecutionToken.Key,
			inst.ParentProcessTargetElementId,
			inst.ParentProcessTargetElementInstanceKey,
			true
	default:
		return 0, "", 0, false
	}
}
