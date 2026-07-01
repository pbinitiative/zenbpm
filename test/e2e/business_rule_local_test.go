package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestBusinessRuleLocalVersionTagBinding(t *testing.T) {
	_, err := deployDmnResourceDefinitionE2e(t, "testdata/dmn/business_rule_version_tag_stable.dmn")
	require.NoError(t, err)
	_, err = deployDmnResourceDefinitionE2e(t, "testdata/dmn/business_rule_version_tag_latest.dmn")
	require.NoError(t, err)

	processInstance := deployAndCreateUniqueProcessDefinitionWithDmnDefinitionId(
		t,
		"testdata/dmn/business_rule_local_dynamic_decision_id_version_tag.bpmn",
		"dmn_business_rule_version_tag",
		map[string]any{"score": 1},
	)

	waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
	assertProcessInstanceVariables(t, processInstance.Key, map[string]any{"score": float64(1), "discount": float64(7)})
	assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
	assertProcessInstanceTokenElements(t, processInstance.Key, []string{"end_event"}, nil)
}

func TestBusinessRuleLocalMultiInstance(t *testing.T) {
	_, err := deployDmnResourceDefinition(t, "bulk-evaluation-test/can-autoliquidate-rule.dmn")
	require.NoError(t, err)

	processId, err := deployUniqueDefinition(t, "multi_instance_business_rule.bpmn")
	require.NoError(t, err)
	definitions, err := listProcessDefinitions(t)
	require.NoError(t, err)

	var definition zenclient.ProcessDefinitionSimple
	for _, candidate := range definitions {
		if candidate.BpmnProcessId == *processId {
			definition = candidate
			break
		}
	}
	require.NotZero(t, definition.Key)

	processInstance, err := createProcessInstance(t, &definition.Key, map[string]any{
		"testInputCollection": []any{1000, 500000, 200},
	})
	require.NoError(t, err)
	require.NotZero(t, processInstance.Key)

	waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
	assertProcessInstanceVariables(t, processInstance.Key, map[string]any{
		"testInputCollection": []interface{}{float64(1000), float64(500000), float64(200)},
		"testOutputCollection": []interface{}{
			map[string]interface{}{"canAutoLiquidate": true},
			map[string]interface{}{"canAutoLiquidate": false},
			map[string]interface{}{"canAutoLiquidate": true},
		},
	})
	assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)
}
