package bpmn

import (
	"context"
	"fmt"

	"github.com/pbinitiative/zenbpm/pkg/storage"
)

// SetProcessSuspended suspends (true) or resumes (false) the BPMN process with the given id and returns the
// version of the change.
//
// A suspended process starts no new instance of any of its versions, including versions deployed while it is
// suspended: not through the API, not from a timer or message start event and not from a call activity.
// Instances which are already running are not affected.
//
// The version orders the changes of the process. A cluster hands the version of a change to the other
// partitions, which record it with ApplyProcessSuspension, and asks for a version above the ones the other
// partitions hold with storage.AboveVersion; an application which embeds the engine passes no options.
func (engine *Engine) SetProcessSuspended(ctx context.Context, bpmnProcessId string, suspended bool, options ...storage.ChangeProcessSuspensionOption) (int64, error) {
	version, err := engine.persistence.ChangeProcessSuspension(ctx, bpmnProcessId, suspended, options...)
	if err != nil {
		return 0, fmt.Errorf("failed to set suspended=%t of process %s: %w", suspended, bpmnProcessId, err)
	}
	return version, nil
}

// ApplyProcessSuspension records a change of the suspension of a process which was made under the given version
// elsewhere, unless the engine already holds a change with the same or a higher version. It returns the change
// the engine holds afterwards. It is for the cluster, which hands the changes of one partition to the others.
func (engine *Engine) ApplyProcessSuspension(ctx context.Context, bpmnProcessId string, suspended bool, version int64) (storage.ProcessSuspension, error) {
	if version < 1 {
		return storage.ProcessSuspension{}, fmt.Errorf("the version of a change of the suspension of process %s must be at least 1, got %d", bpmnProcessId, version)
	}
	held, err := engine.persistence.ApplyProcessSuspension(ctx, bpmnProcessId, suspended, version)
	if err != nil {
		return storage.ProcessSuspension{}, fmt.Errorf("failed to apply suspended=%t (version %d) of process %s: %w", suspended, version, bpmnProcessId, err)
	}
	return held, nil
}

// IsProcessSuspended reports whether the BPMN process with the given id is suspended.
func (engine *Engine) IsProcessSuspended(ctx context.Context, bpmnProcessId string) (bool, error) {
	return engine.persistence.IsProcessSuspended(ctx, bpmnProcessId)
}

// ensureProcessNotSuspended returns a *ProcessSuspendedError when the BPMN process is suspended. It reads the
// storage the way the engine reads everything else; in a cluster a partition whose leader has just changed may
// not have applied a suspension it committed a moment ago, and answers from before it.
func (engine *Engine) ensureProcessNotSuspended(ctx context.Context, bpmnProcessId string) error {
	suspended, err := engine.persistence.IsProcessSuspended(ctx, bpmnProcessId)
	if err != nil {
		return err
	}
	if suspended {
		return &ProcessSuspendedError{BpmnProcessId: bpmnProcessId}
	}
	return nil
}
