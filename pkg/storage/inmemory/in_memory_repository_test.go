package inmemory_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/pbinitiative/zenbpm/pkg/storage/storagetest"
)

func TestInMemoryStorage(t *testing.T) {
	var store storage.Storage = inmemory.NewStorage()

	tester := storagetest.StorageTester{}

	tests := tester.GetTests()
	tester.PrepareTestData(store, t)
	for name, testFunc := range tests {
		t.Run(name, testFunc(store, t))
	}
}

// Verifies UpdateOutputFlowElementInstance mirrors SQL COALESCE on completed_at:
// once set, later updates (including nil) must not overwrite it.
func TestInMemoryUpdateOutputFlowElementInstancePreservesCompletedAt(t *testing.T) {
	store := inmemory.NewStorage()
	ctx := t.Context()

	first := time.Now().Truncate(time.Millisecond)
	second := first.Add(1 * time.Hour)

	saved := bpmnruntime.FlowElementInstance{
		Key:                store.GenerateId(),
		ProcessInstanceKey: store.GenerateId(),
		ElementId:          "preserve-completed-at",
		CreatedAt:          time.Now().Truncate(time.Millisecond),
		ExecutionTokenKey:  store.GenerateId(),
		InputVariables:     map[string]any{"in": "v"},
		OutputVariables:    map[string]any{},
	}
	assert.NoError(t, store.SaveFlowElementInstance(ctx, saved))

	assert.NoError(t, store.UpdateOutputFlowElementInstance(ctx, bpmnruntime.FlowElementInstance{
		Key:             saved.Key,
		OutputVariables: map[string]any{"out": "first"},
		CompletedAt:     &first,
	}))

	got, err := store.GetFlowElementInstanceByKey(ctx, saved.Key)
	assert.NoError(t, err)
	assert.NotNil(t, got.CompletedAt, "CompletedAt should be set after the first update")
	assert.True(t, got.CompletedAt.Equal(first), "CompletedAt should equal the first value, got %v want %v", *got.CompletedAt, first)
	assert.Equal(t, map[string]any{"out": "first"}, got.OutputVariables, "OutputVariables should reflect the first update")
	assert.Equal(t, saved.InputVariables, got.InputVariables, "InputVariables should be preserved")

	assert.NoError(t, store.UpdateOutputFlowElementInstance(ctx, bpmnruntime.FlowElementInstance{
		Key:             saved.Key,
		OutputVariables: map[string]any{"out": "second"},
		CompletedAt:     &second,
	}))

	got, err = store.GetFlowElementInstanceByKey(ctx, saved.Key)
	assert.NoError(t, err)
	if assert.NotNil(t, got.CompletedAt, "CompletedAt should remain set after a later update") {
		assert.True(t, got.CompletedAt.Equal(first),
			"CompletedAt must still be the first value (COALESCE), got %v want %v", *got.CompletedAt, first)
	}
	assert.Equal(t, map[string]any{"out": "second"}, got.OutputVariables, "OutputVariables should reflect the second update")

	assert.NoError(t, store.UpdateOutputFlowElementInstance(ctx, bpmnruntime.FlowElementInstance{
		Key:             saved.Key,
		OutputVariables: map[string]any{"out": "third"},
		CompletedAt:     nil,
	}))

	got, err = store.GetFlowElementInstanceByKey(ctx, saved.Key)
	assert.NoError(t, err)
	if assert.NotNil(t, got.CompletedAt, "CompletedAt must not be erased by a later nil update") {
		assert.True(t, got.CompletedAt.Equal(first),
			"CompletedAt must still be the first value after a nil update, got %v want %v", *got.CompletedAt, first)
	}
	assert.Equal(t, map[string]any{"out": "third"}, got.OutputVariables, "OutputVariables should reflect the third update")
}
