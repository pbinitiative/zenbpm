package bpmn

import (
	"context"
	"fmt"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
)

// ImportProcessDefinition stores a process definition under the key and
// version it already has elsewhere (another partition of the same cluster).
// It is the cluster restore reconciliation path and differs from
// LoadFromBytes on purpose: the version is copied instead of assigned, so a
// historical definition never becomes the latest one, and no deployment
// event is exported. A definition already stored under key is left alone.
//
// A different definition of the same process that already holds the version
// (or the version tag) means the version histories diverged; the import is
// refused with storage.ErrUniqueConstraint and nothing is written.
//
// With registerSubscriptions the definition-level subscriptions (timer and
// message start events, instantiating receive tasks) are created, but only
// when the imported definition is the newest version of its process. The
// subscriptions of the previously newest definition are removed first, as a
// deployment would do. A historical version never touches subscriptions.
func (engine *Engine) ImportProcessDefinition(ctx context.Context, xmlData []byte, key int64, version int32, registerSubscriptions bool) (*runtime.ProcessDefinition, error) {
	if version < 1 {
		return nil, fmt.Errorf("failed to import process definition %d: version must be positive, got %d", key, version)
	}
	definition, err := parseProcessDefinition(xmlData, key)
	if err != nil {
		return nil, fmt.Errorf("failed to import process definition %d: %w", key, err)
	}
	definition.Version = version

	existing, err := engine.persistence.FindProcessDefinitionsById(ctx, definition.BpmnProcessId)
	if err != nil {
		return nil, fmt.Errorf("failed to load processes by id %s: %w", definition.BpmnProcessId, err)
	}
	var previousLatest *runtime.ProcessDefinition
	for i := range existing {
		other := &existing[i]
		if other.Key == key {
			return other, nil
		}
		if other.Version == version {
			return nil, fmt.Errorf("process definition %d cannot be imported as version %d of %q: definition %d already holds that version: %w",
				key, version, definition.BpmnProcessId, other.Key, storage.ErrUniqueConstraint)
		}
		if definition.VersionTag != "" && other.VersionTag == definition.VersionTag {
			return nil, fmt.Errorf("process definition %d cannot be imported with version tag %q of %q: definition %d already holds that tag: %w",
				key, definition.VersionTag, definition.BpmnProcessId, other.Key, storage.ErrUniqueConstraint)
		}
		if previousLatest == nil || other.Version > previousLatest.Version {
			previousLatest = other
		}
	}
	if err := engine.persistence.SaveProcessDefinition(ctx, definition); err != nil {
		return nil, fmt.Errorf("failed to save imported process definition %d: %w", key, err)
	}
	latest := previousLatest == nil || previousLatest.Version < version
	if !registerSubscriptions || !latest {
		return &definition, nil
	}
	if previousLatest != nil {
		if err := engine.deleteProcessDefinitionSubscriptions(ctx, previousLatest); err != nil {
			return nil, fmt.Errorf("failed to retire subscriptions of process definition %d: %w", previousLatest.Key, err)
		}
	}
	if err := engine.RegisterProcessDefinitionSubscriptions(ctx, key); err != nil {
		return nil, err
	}
	return &definition, nil
}
