package rest

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/require"
)

func TestJobStateToActivityState(t *testing.T) {
	tests := []struct {
		name   string
		state  public.JobState
		want   runtime.ActivityState
		wantOk bool
	}{
		{name: "active", state: public.JobStateActive, want: runtime.ActivityStateActive, wantOk: true},
		{name: "completed", state: public.JobStateCompleted, want: runtime.ActivityStateCompleted, wantOk: true},
		{name: "failed", state: public.JobStateFailed, want: runtime.ActivityStateFailed, wantOk: true},
		{name: "terminated", state: public.JobStateTerminated, want: runtime.ActivityStateTerminated, wantOk: true},
		{name: "unsupported", state: public.JobState("unsupported"), wantOk: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := jobStateToActivityState(test.state)
			require.Equal(t, test.wantOk, ok)
			require.Equal(t, test.want, got)
		})
	}
}

// TestJobStateFilterRoundTrip checks that every supported request filter maps to
// an activity state that the response mapping turns back into the same filter.
// The two mappings disagree when a filter hides jobs of another state.
func TestJobStateFilterRoundTrip(t *testing.T) {
	states := []public.JobState{
		public.JobStateActive,
		public.JobStateCompleted,
		public.JobStateFailed,
		public.JobStateTerminated,
	}

	for _, state := range states {
		t.Run(string(state), func(t *testing.T) {
			activityState, ok := jobStateToActivityState(state)
			require.True(t, ok)

			roundTripped, err := getRestJobState(activityState)
			require.NoError(t, err)
			require.Equal(t, state, roundTripped)
		})
	}
}
