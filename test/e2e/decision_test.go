package e2e

import (
	"fmt"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
)

func TestRestApiEvaluateDecision(t *testing.T) {
	var result *zenclient.EvaluatedDRDResult
	var definition zenclient.DmnResourceDefinitionSimple
	err := deployDmnResourceDefinition(t, "can-autoliquidate-rule.dmn")
	assert.NoError(t, err)
	definitions, err := listDecisionDefinitions(t)
	assert.NoError(t, err)
	for _, def := range definitions {
		if *def.DmnResourceDefinitionId == "example_canAutoLiquidate" {
			definition = def
			break
		}
	}

	t.Run("evaluate decision BindingType Latest with DecisionDefinitionId", func(t *testing.T) {
		result, err = evaluateDecision(
			t,
			zenclient.EvaluateDecisionJSONBodyBindingTypeLatest,
			definition.DmnResourceDefinitionId,
			"example_canAutoLiquidateRule",
			nil,
			map[string]any{
				"claim.amountOfDamage": 1000,
				"claim.insuranceType":  "MAJ",
			},
		)
		assert.NoError(t, err)
		assert.NotEmpty(t, result.DecisionOutput)
		assert.NotEmpty(t, result.EvaluatedDecisions)
	})

	t.Run("evaluate decision BindingType Latest without DecisionDefinitionId", func(t *testing.T) {
		result, err = evaluateDecision(
			t,
			zenclient.EvaluateDecisionJSONBodyBindingTypeLatest,
			nil,
			"example_canAutoLiquidateRule",
			nil,
			map[string]any{
				"claim.amountOfDamage": 1000,
				"claim.insuranceType":  "MAJ",
			},
		)
		assert.NoError(t, err)
		assert.NotEmpty(t, result.DecisionOutput)
		assert.NotEmpty(t, result.EvaluatedDecisions)
	})

	t.Run("evaluate decision BindingType VersionTag with DecisionDefinitionId", func(t *testing.T) {
		versionTag := "versionTagTest"
		result, err = evaluateDecision(
			t,
			zenclient.EvaluateDecisionJSONBodyBindingTypeVersionTag,
			definition.DmnResourceDefinitionId,
			"example_canAutoLiquidateRule",
			&versionTag,
			map[string]any{
				"claim.amountOfDamage": 1000,
				"claim.insuranceType":  "MAJ",
			},
		)
		assert.NoError(t, err)
		assert.NotEmpty(t, result.DecisionOutput)
		assert.NotEmpty(t, result.EvaluatedDecisions)
	})

	t.Run("evaluate decision BindingType Deployment with DecisionDefinitionId", func(t *testing.T) {
		result, err = evaluateDecision(
			t,
			zenclient.EvaluateDecisionJSONBodyBindingTypeDeployment,
			definition.DmnResourceDefinitionId,
			"example_canAutoLiquidateRule",
			nil,
			map[string]any{
				"claim.amountOfDamage": 1000,
				"claim.insuranceType":  "MAJ",
			},
		)
		assert.Error(t, err)
	})
}

func evaluateDecision(t testing.TB, bindingType zenclient.EvaluateDecisionJSONBodyBindingType, decisionDefinitionId *string, decisionId string, versionTag *string, variables map[string]any) (*zenclient.EvaluatedDRDResult, error) {
	req := zenclient.EvaluateDecisionJSONRequestBody{
		BindingType:          bindingType,
		DecisionDefinitionId: decisionDefinitionId,
		Variables:            &variables,
		VersionTag:           versionTag,
	}
	resp, err := app.restClient.EvaluateDecisionWithResponse(t.Context(), decisionId, req)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate decision: %w", err)
	}
	if resp.JSON500 != nil {
		return nil, fmt.Errorf("failed to evaluate decision: %v", resp.JSON500)
	}
	return resp.JSON200, nil
}
