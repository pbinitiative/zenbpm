package rest

import (
	"encoding/json"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/require"
)

func TestMapProtoJobTaskHeaders(t *testing.T) {
	t.Run("omits task headers when the job has none", func(t *testing.T) {
		mapped, err := (&Server{}).mapProtoJob(testProtoJobWithHeaders(nil))
		require.NoError(t, err)
		require.Nil(t, mapped.TaskHeaders)

		encoded, err := json.Marshal(mapped)
		require.NoError(t, err)
		require.NotContains(t, string(encoded), `"taskHeaders"`)
	})

	t.Run("preserves configured task headers", func(t *testing.T) {
		headers := map[string]string{"url": "https://example.com", "method": "POST"}

		mapped, err := (&Server{}).mapProtoJob(testProtoJobWithHeaders(headers))
		require.NoError(t, err)
		require.NotNil(t, mapped.TaskHeaders)
		require.Equal(t, headers, *mapped.TaskHeaders)

		encoded, err := json.Marshal(mapped)
		require.NoError(t, err)
		require.Contains(t, string(encoded), `"taskHeaders":{"method":"POST","url":"https://example.com"}`)
	})
}

func testProtoJobWithHeaders(headers map[string]string) *proto.Job {
	state := int64(runtime.ActivityStateActive)
	return &proto.Job{
		State:          &state,
		InputVariables: []byte(`{}`),
		Headers:        headers,
	}
}
