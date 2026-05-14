package bpmn

import (
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/stretchr/testify/require"
)

// waitForProcessInstanceState polls the store until the process instance with
// the given key reaches expectedState, or fails the test after a timeout.
func waitForProcessInstanceState(t testing.TB, store storage.Storage, processInstanceKey int64, expectedState runtime.ActivityState) {
	t.Helper()
	require.Eventually(t, func() bool {
		pi, err := store.FindProcessInstanceByKey(t.Context(), processInstanceKey)
		return err == nil && pi.ProcessInstance().GetState() == expectedState
	}, 5*time.Second, 50*time.Millisecond,
		"process instance %d should reach state %s", processInstanceKey, expectedState)
}
