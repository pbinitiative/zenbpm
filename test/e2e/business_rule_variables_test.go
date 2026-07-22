package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBusinessRuleLocalVariables(t *testing.T) {
	t.Run("local business rule with no output mapping keeps decision result local", func(t *testing.T) {
		_, err := deployDmnResourceDefinitionE2e(t, "testdata/dmn/hit_policy/hit_policy_first.dmn")
		require.NoError(t, err)

		processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(
			t,
			"testdata/dmn/business_rule_local_dynamic_decision_id_no_output_mapping.bpmn",
			"dmn_hit_policy_first",
			map[string]any{
				"amount":        10,
				"customer_type": nil,
			},
		)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"amount": float64(10), "customer_type": interface{}(nil)})
		assertFlowElementOutputVariables(t, processInstance.Key, "business_rule", map[string]interface{}{})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
	})

	t.Run("local business rule output mapping replaces an existing process variable", func(t *testing.T) {
		_, err := deployDmnResourceDefinitionE2e(t, "testdata/dmn/hit_policy/hit_policy_first.dmn")
		require.NoError(t, err)

		processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(
			t,
			"testdata/dmn/business_rule_local_dynamic_decision_id.bpmn",
			"dmn_hit_policy_first",
			map[string]any{
				"amount":        10,
				"customer_type": nil,
				"discount":      "old-value",
			},
		)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"amount": float64(10), "customer_type": interface{}(nil), "discount": float64(0)})
		assertFlowElementOutputVariables(t, processInstance.Key, "business_rule", map[string]interface{}{"discount": float64(0)})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
	})

	t.Run("local business rule invalid output mapping creates an incident after decision evaluation", func(t *testing.T) {
		_, err := deployDmnResourceDefinitionE2e(t, "testdata/dmn/hit_policy/hit_policy_first.dmn")
		require.NoError(t, err)

		processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(
			t,
			"testdata/dmn/business_rule_local_dynamic_decision_id_invalid_output_mapping.bpmn",
			"dmn_hit_policy_first",
			map[string]any{
				"amount":        10,
				"customer_type": nil,
			},
		)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"amount": float64(10), "customer_type": interface{}(nil)})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 1)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"business_rule"}, nil)
		require.NotZero(t, getDecisionInstance(t, processInstance.Key).Key)
	})

	t.Run("local business rule missing output member maps nil", func(t *testing.T) {
		_, err := deployDmnResourceDefinitionE2e(t, "testdata/dmn/hit_policy/hit_policy_first.dmn")
		require.NoError(t, err)

		processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(
			t,
			"testdata/dmn/business_rule_local_dynamic_decision_id_missing_output_mapping.bpmn",
			"dmn_hit_policy_first",
			map[string]any{
				"amount":        10,
				"customer_type": nil,
			},
		)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"amount": float64(10), "customer_type": interface{}(nil), "missing": interface{}(nil)})
		assertFlowElementOutputVariables(t, processInstance.Key, "business_rule", map[string]interface{}{"missing": interface{}(nil)})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
	})

	t.Run("The business rule with DMN and input data should successfully map process variables to the DMN when the process variables are sent.", func(t *testing.T) {
		_, err := deployDmnResourceDefinition(t, "definition/dmn_with_input_data.dmn")
		assert.NoError(t, err)

		definition, err := deployGetDefinition(t, "business_rule/business_rule_task_with_dmn_input_data.bpmn", "DMNWithInputData")
		assert.NoError(t, err)

		processInstance, err := createProcessInstance(t, &definition.Key, map[string]any{
			"row.is": true,
			"row.as": false,
		})
		assert.NoError(t, err)
		assert.NotEmpty(t, processInstance.Key)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"output": map[string]interface{}{"output": "OK"}, "row.is": true, "row.as": false})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"end"}, nil)
	})

	t.Run("The business rule with DMN and input data should pass empty variables to the DMN when the process variables are empty.", func(t *testing.T) {
		_, err := deployDmnResourceDefinition(t, "definition/dmn_with_input_data.dmn")
		assert.NoError(t, err)

		definition, err := deployGetDefinition(t, "business_rule/business_rule_task_with_dmn_input_data.bpmn", "DMNWithInputData")
		assert.NoError(t, err)

		processInstance, err := createProcessInstance(t, &definition.Key, map[string]any{})
		assert.NoError(t, err)
		assert.NotEmpty(t, processInstance.Key)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"output": interface{}(nil)})
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"end"}, nil)
	})

	t.Run("The business rule with DMN and invalid variable mapping should result in an incident.", func(t *testing.T) {
		_, err := deployDmnResourceDefinition(t, "definition/dmn_with_input_data.dmn")
		assert.NoError(t, err)

		definition, err := deployGetDefinition(t, "business_rule/business_rule_task_with_dmn_reversed_input_data_mapping.bpmn", "DMNWithInputData")
		assert.NoError(t, err)

		processInstance, err := createProcessInstance(t, &definition.Key, map[string]any{})
		assert.NoError(t, err)
		assert.NotEmpty(t, processInstance.Key)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateFailed)
		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{})
		assertProcessInstanceTokenElements(t, processInstance.Key, []string{"drd_bug"}, []string{"end"})

		incidents, err := getProcessInstanceIncidents(t, processInstance.Key)
		assert.NoError(t, err)
		assert.NotEmpty(t, incidents)
		assert.Equal(t, 1, len(incidents))

		assert.Equal(t, "drd_bug", incidents[0].ElementId)
		assert.NotEmpty(t, incidents[0].Message)
		assert.Contains(t, incidents[0].Message, "required input missing for append_data")
	})
}
