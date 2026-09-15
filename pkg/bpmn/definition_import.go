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
// event is exported. The definition already stored under key is left alone;
// a different definition (another version or content) stored under key is
// refused with storage.ErrUniqueConstraint.
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

	engine.definitionMu.Lock()
	defer engine.definitionMu.Unlock()
	existing, err := engine.persistence.FindProcessDefinitionsById(ctx, definition.BpmnProcessId)
	if err != nil {
		return nil, fmt.Errorf("failed to load processes by id %s: %w", definition.BpmnProcessId, err)
	}
	for i := range existing {
		if existing[i].Key == key {
			// the partition already holds the key: the same definition (a
			// previous import may have stored it and failed before its
			// subscriptions were registered; registration is idempotent and
			// leaves a definition that is not the latest version alone), or
			// another one, which must not be mistaken for it
			if existing[i].Version != version || existing[i].BpmnChecksum != definition.BpmnChecksum {
				return nil, fmt.Errorf("process definition %d cannot be imported as version %d of %q: the partition already holds version %d of different content under that key: %w",
					key, version, definition.BpmnProcessId, existing[i].Version, storage.ErrUniqueConstraint)
			}
			if registerSubscriptions {
				if err := engine.registerProcessDefinitionSubscriptionsLocked(ctx, key); err != nil {
					return nil, err
				}
			}
			return &existing[i], nil
		}
	}
	previousLatest := latestProcessDefinition(existing)
	if _, err := engine.storeProcessDefinitionVersion(ctx, definition, existing); err != nil {
		return nil, fmt.Errorf("failed to import process definition %d: %w", key, err)
	}
	latest := previousLatest == nil || previousLatest.Version < version
	if !registerSubscriptions || !latest {
		return &definition, nil
	}
	if err := engine.registerProcessDefinitionSubscriptionsLocked(ctx, key); err != nil {
		return nil, err
	}
	return &definition, nil
}
