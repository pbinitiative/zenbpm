package e2e

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func completeMultiInstanceJobs(t *testing.T, parentProcessInstanceKey int64, activityElementID string, count int, parallel bool) int64 {
	t.Helper()

	childProcessInstance := waitForChildProcessInstance(t, parentProcessInstanceKey, 0)
	if parallel {
		jobs := waitForProcessInstanceActiveJobsByElementId(t, childProcessInstance.Key, activityElementID, count)
		for _, job := range jobs {
			require.NoError(t, completeJob(t, job.Key, nil))
		}
		return childProcessInstance.Key
	}

	for range count {
		job := waitForProcessInstanceActiveJobByElementId(t, childProcessInstance.Key, activityElementID)
		require.NoError(t, completeJob(t, job.Key, nil))
	}
	return childProcessInstance.Key
}

func assertNoMultiInstanceOutputVariables(t *testing.T, processInstanceKey int64, activityElementID string, expectedHistoryEntries int) {
	t.Helper()

	processInstance, err := getProcessInstance(t, processInstanceKey)
	require.NoError(t, err)
	require.NotContains(t, processInstance.Variables, "", "process variables must not contain an empty key")

	history := getFlowElementInstancesByElementId(t, processInstanceKey, activityElementID)
	require.Len(t, history, expectedHistoryEntries)
	for _, elementInstance := range history {
		require.Empty(t, elementInstance.OutputVariables, "activity history must not contain output variables when outputCollection is omitted")
	}
}
