package bpmn

import (
	"fmt"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestImportProcessDefinitionPreservesKeyAndVersion(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	defer engine.Stop()
	ctx := t.Context()

	// the partition holds version 2 only; version 1 was lost in a partial deployment
	v2, err := engine.ImportProcessDefinition(ctx, timerStartXML("imported-process", "PT2H"), 200, 2, true)
	require.NoError(t, err)
	assert.Equal(t, int32(2), v2.Version)
	assert.Len(t, timersOf(t, store, 200), 1, "the newest version owns the timer start subscription")

	v1, err := engine.ImportProcessDefinition(ctx, timerStartXML("imported-process", "PT1H"), 100, 1, true)
	require.NoError(t, err)
	assert.Equal(t, int64(100), v1.Key)
	assert.Equal(t, int32(1), v1.Version, "the version is copied, not assigned")

	latest, err := store.FindLatestProcessDefinitionById(ctx, "imported-process")
	require.NoError(t, err)
	assert.Equal(t, int64(200), latest.Key, "importing a historical version must not make it the latest one")
	assert.Len(t, timersOf(t, store, 200), 1, "subscriptions of the actual latest version are kept")
	assert.Empty(t, timersOf(t, store, 100), "a historical version registers no subscriptions")

	// a newer version takes the subscriptions over from the previous latest
	v3, err := engine.ImportProcessDefinition(ctx, timerStartXML("imported-process", "PT3H"), 300, 3, true)
	require.NoError(t, err)
	assert.Equal(t, int32(3), v3.Version)
	assert.Empty(t, timersOf(t, store, 200))
	assert.Len(t, timersOf(t, store, 300), 1)

	// importing the same key again is a no-op
	again, err := engine.ImportProcessDefinition(ctx, timerStartXML("imported-process", "PT1H"), 100, 1, true)
	require.NoError(t, err)
	assert.Equal(t, int64(100), again.Key)
	all, err := store.FindProcessDefinitionsById(ctx, "imported-process")
	require.NoError(t, err)
	assert.Len(t, all, 3)
}

func TestImportProcessDefinitionRefusesDivergedVersionHistories(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	defer engine.Stop()
	ctx := t.Context()

	_, err := engine.ImportProcessDefinition(ctx, timerStartXML("diverged-process", "PT1H"), 100, 1, false)
	require.NoError(t, err)

	_, err = engine.ImportProcessDefinition(ctx, timerStartXML("diverged-process", "PT2H"), 101, 1, false)
	require.ErrorIs(t, err, storage.ErrUniqueConstraint, "another key already holds version 1")
	all, err := store.FindProcessDefinitionsById(ctx, "diverged-process")
	require.NoError(t, err)
	assert.Len(t, all, 1, "nothing is written on a conflict")

	_, err = engine.ImportProcessDefinition(ctx, timerStartXML("diverged-process", "PT2H"), 102, 0, false)
	require.Error(t, err, "a version has to be positive")
}

func TestImportProcessDefinitionRetryRegistersMissingSubscriptions(t *testing.T) {
	// the definition landed but its subscriptions did not (the first import
	// failed after the save): the retried import completes the registration
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	defer engine.Stop()
	ctx := t.Context()

	_, err := engine.ImportProcessDefinition(ctx, timerStartXML("retried-import", "PT1H"), 100, 1, false)
	require.NoError(t, err)
	require.Empty(t, timersOf(t, store, 100))

	again, err := engine.ImportProcessDefinition(ctx, timerStartXML("retried-import", "PT1H"), 100, 1, true)
	require.NoError(t, err)
	assert.Equal(t, int64(100), again.Key)
	assert.Len(t, timersOf(t, store, 100), 1)

	_, err = engine.ImportProcessDefinition(ctx, timerStartXML("retried-import", "PT1H"), 100, 1, true)
	require.NoError(t, err)
	assert.Len(t, timersOf(t, store, 100), 1, "a further retry registers nothing twice")
}

func TestImportProcessDefinitionDoesNotRegisterSubscriptionsWhenNotAsked(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	defer engine.Stop()

	def, err := engine.ImportProcessDefinition(t.Context(), timerStartXML("silent-process", "PT1H"), 100, 1, false)
	require.NoError(t, err)
	assert.Equal(t, int32(1), def.Version)
	assert.Empty(t, timersOf(t, store, 100))
}

func timerStartXML(processID string, cycle string) []byte {
	return []byte(fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" id="defs-%[1]s">
  <bpmn:process id="%[1]s" isExecutable="true">
    <bpmn:startEvent id="start">
      <bpmn:outgoing>to-end</bpmn:outgoing>
      <bpmn:timerEventDefinition><bpmn:timeCycle>R/%[2]s</bpmn:timeCycle></bpmn:timerEventDefinition>
    </bpmn:startEvent>
    <bpmn:endEvent id="end"><bpmn:incoming>to-end</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="to-end" sourceRef="start" targetRef="end" />
  </bpmn:process>
</bpmn:definitions>`, processID, cycle))
}

func timersOf(t *testing.T, store *inmemory.Storage, definitionKey int64) []runtime.Timer {
	t.Helper()
	timers, err := store.FindProcessDefinitionTimers(t.Context(), definitionKey, runtime.TimerStateCreated)
	require.NoError(t, err)
	return timers
}
