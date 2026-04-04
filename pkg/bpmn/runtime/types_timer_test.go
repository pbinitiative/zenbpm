package runtime

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func ptrInt64(v int64) *int64 {
	return &v
}

func TestTimerEqualTo_BothNilPointers(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	t1 := Timer{
		ElementId:            "elem1",
		Key:                  1,
		ElementInstanceKey:   nil,
		ProcessDefinitionKey: 100,
		ProcessInstanceKey:   nil,
		TimerState:           TimerStateCreated,
		CreatedAt:            now,
		DueAt:                now.Add(time.Hour),
		Duration:             time.Hour,
		Token:                nil,
	}
	t2 := Timer{
		ElementId:            "elem1",
		Key:                  1,
		ElementInstanceKey:   nil,
		ProcessDefinitionKey: 100,
		ProcessInstanceKey:   nil,
		TimerState:           TimerStateCreated,
		CreatedAt:            now,
		DueAt:                now.Add(time.Hour),
		Duration:             time.Hour,
		Token:                nil,
	}
	assert.True(t, t1.EqualTo(t2))
}

func TestTimerEqualTo_BothNonNilPointers(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	token := ExecutionToken{Key: 42}
	t1 := Timer{
		ElementId:            "elem1",
		Key:                  1,
		ElementInstanceKey:   ptrInt64(10),
		ProcessDefinitionKey: 100,
		ProcessInstanceKey:   ptrInt64(200),
		TimerState:           TimerStateCreated,
		CreatedAt:            now,
		DueAt:                now.Add(time.Hour),
		Duration:             time.Hour,
		Token:                &token,
	}
	t2 := Timer{
		ElementId:            "elem1",
		Key:                  1,
		ElementInstanceKey:   ptrInt64(10),
		ProcessDefinitionKey: 100,
		ProcessInstanceKey:   ptrInt64(200),
		TimerState:           TimerStateCreated,
		CreatedAt:            now,
		DueAt:                now.Add(time.Hour),
		Duration:             time.Hour,
		Token:                &token,
	}
	assert.True(t, t1.EqualTo(t2))
}

func TestTimerEqualTo_MismatchedNilPointers(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	t1 := Timer{
		ElementId:            "elem1",
		Key:                  1,
		ElementInstanceKey:   ptrInt64(10),
		ProcessDefinitionKey: 100,
		ProcessInstanceKey:   nil,
		TimerState:           TimerStateCreated,
		CreatedAt:            now,
		DueAt:                now.Add(time.Hour),
		Duration:             time.Hour,
		Token:                nil,
	}
	t2 := Timer{
		ElementId:            "elem1",
		Key:                  1,
		ElementInstanceKey:   nil,
		ProcessDefinitionKey: 100,
		ProcessInstanceKey:   nil,
		TimerState:           TimerStateCreated,
		CreatedAt:            now,
		DueAt:                now.Add(time.Hour),
		Duration:             time.Hour,
		Token:                nil,
	}
	assert.False(t, t1.EqualTo(t2), "ElementInstanceKey mismatch: one nil, one non-nil")
}

func TestTimerEqualTo_DifferentProcessInstanceKey(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	t1 := Timer{
		ElementId:            "elem1",
		Key:                  1,
		ElementInstanceKey:   nil,
		ProcessDefinitionKey: 100,
		ProcessInstanceKey:   ptrInt64(200),
		TimerState:           TimerStateCreated,
		CreatedAt:            now,
		DueAt:                now.Add(time.Hour),
		Duration:             time.Hour,
		Token:                nil,
	}
	t2 := Timer{
		ElementId:            "elem1",
		Key:                  1,
		ElementInstanceKey:   nil,
		ProcessDefinitionKey: 100,
		ProcessInstanceKey:   ptrInt64(999),
		TimerState:           TimerStateCreated,
		CreatedAt:            now,
		DueAt:                now.Add(time.Hour),
		Duration:             time.Hour,
		Token:                nil,
	}
	assert.False(t, t1.EqualTo(t2))
}

func TestTimerEqualTo_DifferentTokenValues(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	tok1 := ExecutionToken{Key: 42}
	tok2 := ExecutionToken{Key: 99}
	t1 := Timer{
		ElementId:            "elem1",
		Key:                  1,
		ElementInstanceKey:   nil,
		ProcessDefinitionKey: 100,
		ProcessInstanceKey:   nil,
		TimerState:           TimerStateCreated,
		CreatedAt:            now,
		DueAt:                now.Add(time.Hour),
		Duration:             time.Hour,
		Token:                &tok1,
	}
	t2 := Timer{
		ElementId:            "elem1",
		Key:                  1,
		ElementInstanceKey:   nil,
		ProcessDefinitionKey: 100,
		ProcessInstanceKey:   nil,
		TimerState:           TimerStateCreated,
		CreatedAt:            now,
		DueAt:                now.Add(time.Hour),
		Duration:             time.Hour,
		Token:                &tok2,
	}
	assert.False(t, t1.EqualTo(t2))
}

func TestTimerEqualTo_OneNilToken(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	tok := ExecutionToken{Key: 42}
	t1 := Timer{
		ElementId:            "elem1",
		Key:                  1,
		ElementInstanceKey:   nil,
		ProcessDefinitionKey: 100,
		ProcessInstanceKey:   nil,
		TimerState:           TimerStateCreated,
		CreatedAt:            now,
		DueAt:                now.Add(time.Hour),
		Duration:             time.Hour,
		Token:                &tok,
	}
	t2 := Timer{
		ElementId:            "elem1",
		Key:                  1,
		ElementInstanceKey:   nil,
		ProcessDefinitionKey: 100,
		ProcessInstanceKey:   nil,
		TimerState:           TimerStateCreated,
		CreatedAt:            now,
		DueAt:                now.Add(time.Hour),
		Duration:             time.Hour,
		Token:                nil,
	}
	assert.False(t, t1.EqualTo(t2))
}

func TestTimerGetState(t *testing.T) {
	tests := []struct {
		name     string
		state    TimerState
		expected ActivityState
	}{
		{"Created returns Active", TimerStateCreated, ActivityStateActive},
		{"Triggered returns Completed", TimerStateTriggered, ActivityStateCompleted},
		{"Cancelled returns Withdrawn", TimerStateCancelled, ActivityStateWithdrawn},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			timer := Timer{TimerState: tt.state}
			assert.Equal(t, tt.expected, timer.GetState())
		})
	}
}
