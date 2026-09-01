package rest

import (
	"encoding/json"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/require"
)

func TestMapProtoJobAssignee(t *testing.T) {
	t.Run("omits a nil assignee", func(t *testing.T) {
		mapped, err := (&Server{}).mapProtoJob(testProtoJobWithAssignee(nil))
		require.NoError(t, err)
		require.Nil(t, mapped.Assignee)

		encoded, err := json.Marshal(mapped)
		require.NoError(t, err)
		require.NotContains(t, string(encoded), `"assignee"`)
	})

	t.Run("omits the legacy nil sentinel", func(t *testing.T) {
		assignee := "<nil>"

		mapped, err := (&Server{}).mapProtoJob(testProtoJobWithAssignee(&assignee))
		require.NoError(t, err)
		require.Nil(t, mapped.Assignee)

		encoded, err := json.Marshal(mapped)
		require.NoError(t, err)
		require.NotContains(t, string(encoded), `"assignee"`)
	})

	t.Run("preserves an empty assignee", func(t *testing.T) {
		assignee := ""

		mapped, err := (&Server{}).mapProtoJob(testProtoJobWithAssignee(&assignee))
		require.NoError(t, err)
		require.NotNil(t, mapped.Assignee)
		require.Empty(t, *mapped.Assignee)

		encoded, err := json.Marshal(mapped)
		require.NoError(t, err)
		require.Contains(t, string(encoded), `"assignee":""`)
	})

	t.Run("preserves a configured assignee", func(t *testing.T) {
		assignee := "john.doe"

		mapped, err := (&Server{}).mapProtoJob(testProtoJobWithAssignee(&assignee))
		require.NoError(t, err)
		require.NotNil(t, mapped.Assignee)
		require.Equal(t, assignee, *mapped.Assignee)

		encoded, err := json.Marshal(mapped)
		require.NoError(t, err)
		require.Contains(t, string(encoded), `"assignee":"john.doe"`)
	})
}

func testProtoJobWithAssignee(assignee *string) *proto.Job {
	state := int64(runtime.ActivityStateActive)
	return &proto.Job{
		State:          &state,
		InputVariables: []byte(`{}`),
		Assignee:       assignee,
	}
}
