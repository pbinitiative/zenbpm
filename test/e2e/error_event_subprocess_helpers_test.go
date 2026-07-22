package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

// setupErrorEventSubProcessInstance deploys the shared error-event-subprocess-interrupting BPMN
// definition, creates a process instance with the given variables, registers a cleanup, and returns
// the created instance. Pass nil for variables to start with an empty variable set.
func setupErrorEventSubProcessInstance(t *testing.T, variables map[string]any) zenclient.ProcessInstance {
	t.Helper()

	definition, err := deployGetDefinition(t, "error_event_subprocess/error-event-subprocess-interrupting.bpmn", "error-event-subprocess-interrupting")
	require.NoError(t, err)
	require.NotZero(t, definition.Key)

	instance, err := createProcessInstance(t, &definition.Key, variables)
	require.NoError(t, err)
	require.NotZero(t, instance.Key)
	t.Cleanup(func() {
		cleanupOwnedProcessInstance(t, instance.Key)
	})

	return instance
}
