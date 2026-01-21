package e2e

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/stretchr/testify/assert"
)

func TestRestApiEvaluateDecision(t *testing.T) {
	var result public.EvaluatedDRDResult
	var definition public.DmnResourceDefinitionSimple
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
			public.EvaluateDecisionJSONBodyBindingTypeLatest,
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
			public.EvaluateDecisionJSONBodyBindingTypeLatest,
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
			public.EvaluateDecisionJSONBodyBindingTypeVersionTag,
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
			public.EvaluateDecisionJSONBodyBindingTypeDeployment,
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

func evaluateDecision(t testing.TB, bindingType public.EvaluateDecisionJSONBodyBindingType, decisionDefinitionId *string, decisionId string, versionTag *string, variables map[string]any) (public.EvaluatedDRDResult, error) {
	req := public.EvaluateDecisionJSONRequestBody{
		BindingType:          bindingType,
		DecisionDefinitionId: decisionDefinitionId,
		Variables:            &variables,
		VersionTag:           versionTag,
	}
	resp, err := app.NewRequest(t).
		WithPath(fmt.Sprintf("/v1/decision-definitions/%s/evaluate", decisionId)).
		WithMethod("POST").
		WithBody(req).
		DoOk()
	if err != nil {
		return public.EvaluatedDRDResult{}, fmt.Errorf("failed to evaluate decision: %w", err)
	}
	instance := public.EvaluatedDRDResult{}

	err = json.Unmarshal(resp, &instance)
	if err != nil {
		return public.EvaluatedDRDResult{}, fmt.Errorf("failed to unmarshal response: %w", err)
	}
	return instance, nil
}
