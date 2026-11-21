package rest

import (
	"encoding/json"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMapProtoJobNamesTheListenerEventType(t *testing.T) {
	cases := []struct {
		name              string
		listenerEventType *string
		expected          *public.JobListenerEventType
	}{
		{name: "the job of an end listener", listenerEventType: new("end"), expected: new(public.JobListenerEventTypeEnd)},
		{name: "the job of an element", listenerEventType: nil, expected: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := int64(runtime.ActivityStateActive)
			// a listener's job carries the type of its element, which says nothing about the listener
			mapped, err := (&Server{}).mapProtoJob(&proto.Job{
				State: &state, InputVariables: []byte(`{}`), ElementType: new("START_EVENT"), ListenerEventType: tc.listenerEventType,
			})
			require.NoError(t, err)

			assert.Equal(t, tc.expected, mapped.ListenerEventType)
			encoded, err := json.Marshal(mapped)
			require.NoError(t, err)
			if tc.expected == nil {
				assert.NotContains(t, string(encoded), `"listenerEventType"`, "absent for the job of a task")
			}
		})
	}
}
