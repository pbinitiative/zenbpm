package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
)

func TestMultilineDMNBusinessRules(t *testing.T) {
	_, err := deployDmnResourceDefinition(t, "multiline/literal-expression.dmn")
	assert.NoError(t, err)

	_, err = deployDmnResourceDefinition(t, "multiline/decision-table-output.dmn")
	assert.NoError(t, err)

	definition, err := deployGetDefinition(t, "business_rule/multiline-dmn-business-rules.bpmn", "MultilineDMNBusinessRulesProcess")
	assert.NoError(t, err)

	t.Run("literal expression multiline quoted string resolves correctly via business rule task", func(t *testing.T) {
		processInstance, err := createProcessInstance(t, &definition.Key, map[string]any{})
		assert.NoError(t, err)
		assert.NotEmpty(t, processInstance.Key)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)

		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{
			"literalResult": "Hello,\nWorld!",
			"tableResult":   map[string]interface{}{"message": "Hello,\n\nthis is a multiline\nmessage."},
		})
	})

	t.Run("decision table multiline output resolves correctly via business rule task", func(t *testing.T) {
		processInstance, err := createProcessInstance(t, &definition.Key, map[string]any{})
		assert.NoError(t, err)
		assert.NotEmpty(t, processInstance.Key)

		waitForProcessInstanceState(t, processInstance.Key, zenclient.ProcessInstanceStateCompleted)
		assertProcessInstanceIncidentsLength(t, processInstance.Key, 0)

		assertProcessInstanceVariables(t, processInstance.Key, map[string]any{
			"literalResult": "Hello,\nWorld!",
			"tableResult":   map[string]interface{}{"message": "Hello,\n\nthis is a multiline\nmessage."},
		})
	})
}
