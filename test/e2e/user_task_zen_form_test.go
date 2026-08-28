package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

// TestUserTaskWithZenForm is a regression test for the combination of a
// custom zenbpm:taskDefinition type and the ZEN_FORM input variable on a
// User Task. The ZenBPM ecosystem convention (see
// docs/getting-started/orchestrate-human-tasks.mdx) is for the frontend to
// render a bpmn-io form from a job input variable named ZEN_FORM and to
// submit form values via completeJob. This test exercises that contract on
// the engine side without involving the UI:
//
//  1. The BPMN declares zenbpm:taskDefinition type="approval" so the
//     engine creates a job with a non-default worker-routing Type while
//     keeping ElementType="USER_TASK" (the regression introduced by
//     feature #789).
//  2. An input mapping copies a process-level formSchema variable into
//     the ZEN_FORM input variable, which is what the UI reads to render
//     the form.
//  3. An output mapping copies a form-submitted variable (approver) into
//     a process variable (approved_by), proving that values returned by
//     completeJob still flow through output mapping when a custom task
//     type is used.
//
// If any of these break, the engine has lost the ability to drive a Zen
// Form with a custom-typed user task.
func TestUserTaskWithZenForm(t *testing.T) {
	// A minimal bpmn-io form schema. The engine stores it verbatim as the
	// ZEN_FORM input variable; only the frontend parses it.
	const formSchema = `{"components":[{"key":"approver","label":"Approver","type":"textfield"}],"type":"default","id":"Form_1","schemaVersion":19}`
	initialVariables := map[string]any{
		"formSchema": formSchema,
	}

	processInstance := deployAndCreateUniqueProcessDefinition(t,
		"testdata/user_task/user_task_with_zen_form.bpmn", initialVariables)

	t.Cleanup(func() {
		cleanupOwnedProcessInstance(t, processInstance.Key)
	})

	job := waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, "user_task")

	// Custom taskDefinition must be preserved on the Job.Type while the
	// ElementType remains "USER_TASK" so workers can still recognise the
	// task as a user task.
	require.Equal(t, "approval", job.Type)
	require.Equal(t, "USER_TASK", job.ElementType)

	// ZEN_FORM must be present on the job's input variables and equal the
	// value supplied at process-instance creation. This is the variable the
	// UI reads to render the form.
	require.Contains(t, job.InputVariables, "ZEN_FORM",
		"ZEN_FORM must be exposed as a job input variable for the UI to render the form")
	require.Equal(t, formSchema, job.InputVariables["ZEN_FORM"],
		"ZEN_FORM must preserve the form schema verbatim")

	waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateActive)

	// Simulate the user submitting the form. The `approver` value mirrors
	// the form's textfield key.
	completeJobForElementId(t, processInstance.Key, "user_task", map[string]any{
		"approver": "alice",
	})
	waitForProcessInstanceJobByElementId(t, processInstance.Key, "user_task", public.JobStateCompleted)

	waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)

	// The output mapping copies the form-submitted `approver` into the
	// process-level `approved_by` variable. If this is missing the engine
	// silently dropped the form value after completeJob. The initial
	// `formSchema` variable is also retained in process scope.
	assertProcessInstanceVariables(t, processInstance.Key, map[string]any{
		"formSchema": formSchema,
		"approved_by": "alice",
	})
}
