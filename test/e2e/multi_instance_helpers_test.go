package e2e

import (
	"sort"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
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

// maxProcessInstancePageSize is the largest page the process instance listing serves.
const maxProcessInstancePageSize = 100

// waitForChildProcessInstancesOfType waits until the parent has exactly count
// direct children of the given type and returns them ordered by key, which is
// their creation order. It reads a single page, so count is limited to
// maxProcessInstancePageSize children of the parent in all.
func waitForChildProcessInstancesOfType(t testing.TB, parentProcessInstanceKey int64, processType zenclient.ProcessInstanceProcessType, count int) []zenclient.ProcessInstancesSimple {
	t.Helper()
	require.LessOrEqual(t, count, maxProcessInstancePageSize, "more children than one page of the process instance listing holds")

	var children []zenclient.ProcessInstancesSimple
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		page, err := getChildInstancesPage(t, parentProcessInstanceKey, new(int32(maxProcessInstancePageSize)))
		if !assert.NoError(collect, err) {
			return
		}
		children = children[:0]
		for _, partition := range page.Partitions {
			for _, child := range partition.Items {
				if child.ProcessType == processType {
					children = append(children, child)
				}
			}
		}
		assert.Len(collect, children, count, "process instance %d should have %d %s children", parentProcessInstanceKey, count, processType)
	}, 10*time.Second, 50*time.Millisecond)

	sort.Slice(children, func(i, j int) bool { return children[i].Key < children[j].Key })
	return children
}

// stringVariable returns the string variable name of vars, failing the test
// rather than panicking the shared suite when it is missing or not a string.
func stringVariable(t testing.TB, vars map[string]any, name string) string {
	t.Helper()

	value, ok := vars[name].(string)
	require.True(t, ok, "variable %q must be a string, got %T in %v", name, vars[name], vars)
	return value
}
