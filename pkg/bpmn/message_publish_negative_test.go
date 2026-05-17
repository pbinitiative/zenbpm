package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPublishMessageByName_NoMatchingSubscription verifies that publishing a message
// with an unknown name (or before the definition has been deployed/registered) returns
// a not-found-style error and never spawns a process instance.
func TestPublishMessageByName_NoMatchingSubscription(t *testing.T) {
	t.Run("unknown message name returns not-found", func(t *testing.T) {
		store := inmemory.NewStorage()
		engine := NewEngine(EngineWithStorage(store))
		engine.Start(t.Context())
		defer engine.Stop()

		err := engine.PublishMessageByName(t.Context(), "totally-unknown-message", nil, nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, storage.ErrNotFound,
			"publishing a message that nobody is subscribed to must surface as storage.ErrNotFound")
		assert.Empty(t, store.ProcessInstances,
			"no process instance should have been created for an unknown message name")
	})

	t.Run("definition loaded but RegisterProcessDefinitionSubscriptions never called", func(t *testing.T) {
		store := inmemory.NewStorage()
		engine := NewEngine(EngineWithStorage(store))
		engine.Start(t.Context())
		defer engine.Stop()

		// Load the BPMN but skip RegisterProcessDefinitionSubscriptions — no
		// DefinitionMessageSubscription is created, so the publishing must fail.
		_, err := engine.LoadFromFile(t.Context(), "./test-cases/process_definition_start_event/message-start-event-process.bpmn")
		require.NoError(t, err)

		err = engine.PublishMessageByName(t.Context(), "messageStartEventProcessRef", nil, nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, storage.ErrNotFound,
			"publishing before the definition's start-event subscriptions are registered must be a not-found")
		assert.Empty(t, store.ProcessInstances,
			"no process instance should have been created when no subscription is registered yet")
	})

	t.Run("publish by non-existent subscription key returns not-found", func(t *testing.T) {
		store := inmemory.NewStorage()
		engine := NewEngine(EngineWithStorage(store))
		engine.Start(t.Context())
		defer engine.Stop()

		subsBefore := len(store.MessageSubscriptions)
		err := engine.PublishMessageByKey(t.Context(), int64(0xdeadbeef), nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, storage.ErrNotFound,
			"publishing onto a non-existent subscription key must surface storage.ErrNotFound")
		assert.Equal(t, subsBefore, len(store.MessageSubscriptions),
			"failed publish must not create or mutate any subscription")
	})
}
