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
		elementType string
		expected    *public.JobListenerEventType
	}{
		{elementType: "START_EVENT", expected: new(public.JobListenerEventTypeEnd)},
		{elementType: "SERVICE_TASK", expected: nil},
	}
	for _, tc := range cases {
		t.Run(tc.elementType, func(t *testing.T) {
			state := int64(runtime.ActivityStateActive)
			mapped, err := (&Server{}).mapProtoJob(&proto.Job{State: &state, InputVariables: []byte(`{}`), ElementType: &tc.elementType})
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
