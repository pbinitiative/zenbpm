package e2e

import (
	"fmt"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
)

func TestRestApiEvaluateDecision(t *testing.T) {
	var result *zenclient.EvaluatedDRDResult
	var dmnResourceDefinition zenclient.DmnResourceDefinitionSimple
	err := deployDmnResourceDefinition(t, "can-autoliquidate-rule.dmn")
	assert.NoError(t, err)
	definitions, err := listDecisionDefinitions(t)
	assert.NoError(t, err)
	for _, def := range definitions {
		if def.DmnResourceDefinitionId == "example_canAutoLiquidate" {
			dmnResourceDefinition = def
			break
		}
	}

	t.Run("evaluate decision BindingType Latest with DmnResourceDefinitionId", func(t *testing.T) {
		result, err = evaluateDecision(
			t,
			zenclient.EvaluateDecisionJSONBodyBindingTypeLatest,
			&dmnResourceDefinition.DmnResourceDefinitionId,
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
			&dmnResourceDefinition.DmnResourceDefinitionId,
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
			&dmnResourceDefinition.DmnResourceDefinitionId,
			"example_canAutoLiquidateRule",
			nil,
			map[string]any{
				"claim.amountOfDamage": 1000,
				"claim.insuranceType":  "MAJ",
			},
		)
		assert.Error(t, err)
	})

	t.Run("GetDecisionInstanceWithResponse loads decision instance with correct evaluated decisions object", func(t *testing.T) {
		result, err = evaluateDecision(
			t,
			zenclient.EvaluateDecisionJSONBodyBindingTypeLatest,
			&dmnResourceDefinition.DmnResourceDefinitionId,
			"example_canAutoLiquidateRule",
			nil,
			map[string]any{
				"claim.amountOfDamage": 15000,
				"claim.insuranceType":  "MAJ",
			},
		)
		assert.NoError(t, err)
		assert.NotEmpty(t, result.DecisionOutput)
		assert.NotEmpty(t, result.EvaluatedDecisions)

		var decisionInstanceRes *zenclient.GetDecisionInstanceResponse
		decisionInstanceRes, err = app.restClient.GetDecisionInstanceWithResponse(t.Context(), result.DecisionInstanceKey)
		assert.NoError(t, err)
		decisionInstanceDetail := decisionInstanceRes.JSON200
		assert.Equal(t, map[string]interface{}{"canAutoLiquidate": true}, *decisionInstanceDetail.DecisionOutput)
		assert.Equal(t, dmnResourceDefinition.Key, decisionInstanceDetail.DmnResourceDefinitionKey)
		assert.NotEmpty(t, decisionInstanceDetail.EvaluatedAt)
		assert.Equal(t, 1, len(decisionInstanceDetail.EvaluatedDecisions))
		evaluateDec := decisionInstanceDetail.EvaluatedDecisions[0]
		assert.Equal(t, "example_canAutoLiquidateRule", *evaluateDec.DecisionId)
		assert.Equal(t, "Decision of auto liquidation", *evaluateDec.DecisionName)
		assert.Equal(t, zenclient.EvaluatedDecisionDecisionTypeDECISIONTABLE, *evaluateDec.DecisionType)
		assert.Equal(t, 2, len(*evaluateDec.Inputs))
		inputs := *evaluateDec.Inputs
		assert.Equal(t, "claim.amountOfDamage", *inputs[0].InputExpression)
		assert.Equal(t, "Input_1", *inputs[0].InputId)
		assert.Equal(t, "Value", *inputs[0].InputName)
		assert.Equal(t, 15000.0, *inputs[0].InputValue)
		assert.Equal(t, "claim.insuranceType", *inputs[1].InputExpression)
		assert.Equal(t, "InputClause_137jnlm", *inputs[1].InputId)
		assert.Equal(t, "Insurance Type", *inputs[1].InputName)
		assert.Equal(t, "MAJ", *inputs[1].InputValue)
		assert.Equal(t, 1, len(*evaluateDec.MatchedRules))
		matchedRule := (*evaluateDec.MatchedRules)[0]
		assert.Equal(t, "DecisionRule_1k1p1ib", *matchedRule.RuleId)
		assert.Equal(t, 1, *matchedRule.RuleIndex)
		assert.Equal(t, 1, len(*matchedRule.EvaluatedOutputs))
		evaluatedOutput := (*matchedRule.EvaluatedOutputs)[0]
		assert.Equal(t, "Output_1", *evaluatedOutput.OutputId)
		assert.Equal(t, "canAutoLiquidate", *evaluatedOutput.OutputName)
		assert.Equal(t, true, *evaluatedOutput.OutputValue)
	})
}

func evaluateDecision(t testing.TB, bindingType zenclient.EvaluateDecisionJSONBodyBindingType, dmnResourceDefinitionId *string, decisionId string, versionTag *string, variables map[string]any) (*zenclient.EvaluatedDRDResult, error) {
	req := zenclient.EvaluateDecisionJSONRequestBody{
		BindingType:             bindingType,
		DmnResourceDefinitionId: dmnResourceDefinitionId,
		Variables:               &variables,
		VersionTag:              versionTag,
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
