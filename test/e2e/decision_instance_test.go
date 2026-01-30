package e2e

import (
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
)

func TestDecisionInstance(t *testing.T) {
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

	t.Run("GetDecisionInstance by key loads decision instance with correct evaluated decisions object", func(t *testing.T) {
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

func TestDecisionInstances(t *testing.T) {
	var resDefId1Key int64
	var redDefId2Key int64
	t.Run("deploy dmn resource definition", func(t *testing.T) {
		err := deployDmnResourceDefinition(t, "can-autoliquidate-rule.dmn")
		assert.NoError(t, err)
		resDefId1Key, err = deployDmnResourceDefinitionWithNewNameAndId(t, "can-autoliquidate-rule.dmn", ptr.To("resDefName1"), ptr.To("resDefId1"))
		assert.NoError(t, err)
		redDefId2Key, err = deployDmnResourceDefinitionWithNewNameAndId(t, "can-autoliquidate-rule.dmn", ptr.To("resDefName2"), ptr.To("resDefId2"))
		assert.NoError(t, err)
		_, err = deployDmnResourceDefinitionWithNewNameAndId(t, "can-autoliquidate-rule.dmn", ptr.To("resDefName3"), ptr.To("resDefId3"))
		assert.NoError(t, err)
	})

	t.Run("GetDecisionInstances get by DmnResourceDefinitionKey", func(t *testing.T) {
		result, err := evaluateDecision(
			t,
			zenclient.EvaluateDecisionJSONBodyBindingTypeLatest,
			ptr.To("resDefId1"),
			"resDefId1Rule",
			nil,
			map[string]any{
				"claim.amountOfDamage": 15000,
				"claim.insuranceType":  "MAJ",
			},
		)
		assert.NoError(t, err)
		assert.NotEmpty(t, result.DecisionOutput)
		assert.NotEmpty(t, result.EvaluatedDecisions)

		var decisionInstanceRes *zenclient.GetDecisionInstancesResponse
		decisionInstanceRes, err = app.restClient.GetDecisionInstancesWithResponse(t.Context(), &zenclient.GetDecisionInstancesParams{
			DmnResourceDefinitionKey: ptr.To(resDefId1Key),
		})
		assert.NoError(t, err)
		decisionInstancePartitionPage := decisionInstanceRes.JSON200
		assert.NotEmpty(t, decisionInstancePartitionPage)
		assert.Equal(t, len(decisionInstancePartitionPage.Partitions), 1)
		assert.Equal(t, len(decisionInstancePartitionPage.Partitions[0].Items), 1)
		assert.Equal(t, decisionInstancePartitionPage.Partitions[0].Items[0].DmnResourceDefinitionKey, resDefId1Key)
		assert.Equal(t, *decisionInstancePartitionPage.Partitions[0].Items[0].InputCount, 2)
		assert.Equal(t, *decisionInstancePartitionPage.Partitions[0].Items[0].OutputCount, 1)
		assert.NotEmpty(t, decisionInstancePartitionPage.Partitions[0].Items[0].Key)
		assert.NotEmpty(t, decisionInstancePartitionPage.Partitions[0].Items[0].EvaluatedAt)
	})

	t.Run("GetDecisionInstances get by DmnResourceDefinitionId, evaluatedFrom in past, evaluatedTo in future order by evaluatedAt desc ", func(t *testing.T) {
		result, err := evaluateDecision(
			t,
			zenclient.EvaluateDecisionJSONBodyBindingTypeLatest,
			ptr.To("resDefId2"),
			"resDefId2Rule",
			nil,
			map[string]any{
				"claim.amountOfDamage": 10000,
				"claim.insuranceType":  "MAJ",
			},
		)
		result, err = evaluateDecision(
			t,
			zenclient.EvaluateDecisionJSONBodyBindingTypeLatest,
			ptr.To("resDefId2"),
			"resDefId2Rule",
			nil,
			map[string]any{
				"claim.amountOfDamage": 25000,
				"claim.insuranceType":  "MAJ",
			},
		)
		assert.NoError(t, err)
		assert.NotEmpty(t, result.DecisionOutput)
		assert.NotEmpty(t, result.EvaluatedDecisions)

		past := time.Now().AddDate(0, 0, -1)
		future := time.Now().AddDate(0, 0, 1)
		var decisionInstanceRes *zenclient.GetDecisionInstancesResponse
		decisionInstanceRes, err = app.restClient.GetDecisionInstancesWithResponse(t.Context(), &zenclient.GetDecisionInstancesParams{
			DmnResourceDefinitionId: ptr.To("resDefId2"),
			EvaluatedFrom:           ptr.To(past),
			EvaluatedTo:             ptr.To(future),
			SortBy:                  ptr.To(zenclient.GetDecisionInstancesParamsSortByEvaluatedAt),
			SortOrder:               ptr.To(zenclient.GetDecisionInstancesParamsSortOrderDesc),
		})
		assert.NoError(t, err)
		decisionInstancePartitionPage := decisionInstanceRes.JSON200
		assert.NotEmpty(t, decisionInstancePartitionPage)
		assert.Equal(t, len(decisionInstancePartitionPage.Partitions), 1)
		assert.Equal(t, len(decisionInstancePartitionPage.Partitions[0].Items), 2)

		assert.Equal(t, decisionInstancePartitionPage.Partitions[0].Items[0].DmnResourceDefinitionKey, redDefId2Key)
		assert.Equal(t, *decisionInstancePartitionPage.Partitions[0].Items[0].InputCount, 2)
		assert.Equal(t, *decisionInstancePartitionPage.Partitions[0].Items[0].OutputCount, 1)
		assert.NotEmpty(t, decisionInstancePartitionPage.Partitions[0].Items[0].Key)
		assert.NotEmpty(t, decisionInstancePartitionPage.Partitions[0].Items[0].EvaluatedAt)

		assert.Equal(t, decisionInstancePartitionPage.Partitions[0].Items[1].DmnResourceDefinitionKey, redDefId2Key)
		assert.Equal(t, *decisionInstancePartitionPage.Partitions[0].Items[1].InputCount, 2)
		assert.Equal(t, *decisionInstancePartitionPage.Partitions[0].Items[1].OutputCount, 1)
		assert.NotEmpty(t, decisionInstancePartitionPage.Partitions[0].Items[1].Key)
		assert.NotEmpty(t, decisionInstancePartitionPage.Partitions[0].Items[1].EvaluatedAt)

		// order by evaluatedAt desc check
		assert.True(t, decisionInstancePartitionPage.Partitions[0].Items[0].EvaluatedAt.UnixMilli() > decisionInstancePartitionPage.Partitions[0].Items[1].EvaluatedAt.UnixMilli())
	})
}
