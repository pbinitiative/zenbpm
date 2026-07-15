package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestBusinessRuleMessageBoundaryFlow(t *testing.T) {
	t.Run("Interrupting boundary message cancels the business rule task and completes the boundary path", func(t *testing.T) {
		definitionKey, messageName, correlationKey := deployInterruptingMessageBoundaryDefinition(
			t,
			"testdata/business_rule/business_rule_task_external_with_message_boundary_event_without_output_mapping.bpmn",
			"business-rule-message-boundary-event-without-output-mapping",
		)
		instance := createMessageBoundaryInstance(t, definitionKey)

		waitForProcessInstanceActiveJobByElementId(t, instance.Key, "business_rule")
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, instance.Key, "business_rule", runtime.TokenStateWaiting)
		assertMessageSubscriptionState(t, instance.Key, "business_rule", zenclient.EventSubscriptionStateActive)
		assertExactProcessInstanceHistory(t, instance.Key, []string{
			"start_event",
			"flow_to_business_rule",
			"business_rule",
		})

		err := publishMessage(t, messageName, correlationKey, &map[string]any{})
		require.NoError(t, err)

		waitForProcessInstanceJobByElementId(t, instance.Key, "business_rule", public.JobStateTerminated)
		assertProcessInstanceIsCompleted(t, instance.Key, "end_event_boundary")
		assertProcessInstanceTokenState(t, instance.Key, "end_event_boundary", runtime.TokenStateCompleted)
		assertProcessInstanceTokenCount(t, instance.Key, "end_event_main", 0)
		assertMessageSubscriptionStateCount(t, instance.Key, "business_rule", zenclient.EventSubscriptionStateActive, 0)
		assertMessageSubscriptionStateCount(t, instance.Key, "business_rule", zenclient.EventSubscriptionStateTerminated, 1)
		assertExactProcessInstanceHistory(t, instance.Key, []string{
			"start_event",
			"flow_to_business_rule",
			"business_rule",
			"boundary_message_event",
			"flow_to_boundary_end",
			"end_event_boundary",
		})
	})

	t.Run("Non-interrupting boundary message keeps the business rule task active and completes both paths", func(t *testing.T) {
		definitionKey, messageName, correlationKey := deployMessageBoundaryDefinition(
			t,
			"testdata/business_rule/business_rule_task_external_with_message_boundary_event_without_output_mapping.bpmn",
			"business-rule-message-boundary-event-without-output-mapping",
		)
		instance := createMessageBoundaryInstance(t, definitionKey)

		waitForProcessInstanceActiveJobByElementId(t, instance.Key, "business_rule")
		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateActive)
		assertProcessInstanceTokenState(t, instance.Key, "business_rule", runtime.TokenStateWaiting)
		assertMessageSubscriptionState(t, instance.Key, "business_rule", zenclient.EventSubscriptionStateActive)
		assertExactProcessInstanceHistory(t, instance.Key, []string{
			"start_event",
			"flow_to_business_rule",
			"business_rule",
		})

		err := publishMessage(t, messageName, correlationKey, &map[string]any{})
		require.NoError(t, err)

		waitForProcessInstanceState(t, instance.Key, zenclient.ProcessInstanceStateActive)
		waitForProcessInstanceActiveJobByElementId(t, instance.Key, "business_rule")
		assertProcessInstanceTokenState(t, instance.Key, "business_rule", runtime.TokenStateWaiting)
		assertProcessInstanceTokenState(t, instance.Key, "end_event_boundary", runtime.TokenStateCompleted)
		assertMessageSubscriptionStateCount(t, instance.Key, "business_rule", zenclient.EventSubscriptionStateCompleted, 1)
		assertMessageSubscriptionStateCount(t, instance.Key, "business_rule", zenclient.EventSubscriptionStateActive, 1)
		assertExactProcessInstanceHistory(t, instance.Key, []string{
			"start_event",
			"flow_to_business_rule",
			"business_rule",
			"boundary_message_event",
			"flow_to_boundary_end",
			"end_event_boundary",
		})

		completeJobForElementId(t, instance.Key, "business_rule", nil)

		assertProcessInstanceIsCompleted(t, instance.Key, "end_event_main")
		assertProcessInstanceTokenState(t, instance.Key, "end_event_main", runtime.TokenStateCompleted)
		assertMessageSubscriptionStateCount(t, instance.Key, "business_rule", zenclient.EventSubscriptionStateActive, 0)
		assertMessageSubscriptionStateCount(t, instance.Key, "business_rule", zenclient.EventSubscriptionStateTerminated, 1)
		assertExactProcessInstanceHistory(t, instance.Key, []string{
			"start_event",
			"flow_to_business_rule",
			"business_rule",
			"boundary_message_event",
			"flow_to_boundary_end",
			"end_event_boundary",
			"flow_to_main_end",
			"end_event_main",
		})
	})
}
