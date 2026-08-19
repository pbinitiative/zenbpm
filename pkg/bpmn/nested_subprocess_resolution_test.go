package bpmn

import (
	"context"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSubProcessInstanceLookup_NestedParent exercises the case where a
// SubProcessInstance is nested inside another SubProcess. The
// ParentProcessTargetElementId refers to the inner sub-process, which
// must be reachable through the lookup.
func TestSubProcessInstanceLookup_NestedParent(t *testing.T) {
	// Tree (from test-cases/nested_sub_process.bpmn):
	//   Root
	//   └─ Activity_1f5yxes (sub-process A)
	//      └─ Activity_0z4w1h2 (sub-process B)
	//         └─ Activity_1gbwlgl (service task)
	def, err := bpmnEngine.LoadFromFile(t.Context(), "model/bpmn20/test-cases/nested_sub_process.bpmn")
	require.NoError(t, err)

	// An instance of inner sub-process B; ParentProcessTargetElementId
	// is "Activity_0z4w1h2" (the inner sub-process id, two levels deep).
	instance := &runtime.SubProcessInstance{
		ParentProcessExecutionToken: runtime.ExecutionToken{
			Key:                100,
			ElementInstanceKey: 200,
			ElementId:          "Activity_0z4w1h2",
			ProcessInstanceKey: 1,
			State:              runtime.TokenStateRunning,
			CreatedAt:          time.Now(),
		},
		ParentProcessTargetElementId: "Activity_0z4w1h2",
		ProcessInstanceData: runtime.ProcessInstanceData{
			Definition: def,
			Key:        42,
			State:      runtime.ActivityStateReady,
		},
	}

	// Token targets a deeply nested service task inside B.
	activity, err := bpmnEngine.getExecutionTokenActivity(
		context.Background(),
		instance,
		runtime.ExecutionToken{
			Key:                101,
			ElementInstanceKey: 201,
			ElementId:          "Activity_1gbwlgl",
			ProcessInstanceKey: 42,
			State:              runtime.TokenStateRunning,
			CreatedAt:          time.Now(),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, activity)
	assert.Equal(t, "Activity_1gbwlgl", activity.Element().GetId())
}
