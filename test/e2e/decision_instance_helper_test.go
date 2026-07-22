package e2e

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getDecisionInstance(t testing.TB, key int64) zenclient.DecisionInstanceDetail {
	t.Helper()

	resp, err := app.NewRequest(t).
		WithPath(fmt.Sprintf("/v1/decision-instances?processInstanceKey=%d", key)).
		DoOk()

	require.NoError(t, err)

	decisions := zenclient.DecisionInstancePartitionPage{}

	err = json.Unmarshal(resp, &decisions)
	require.NoError(t, err)
	require.NotEmpty(t, decisions.Partitions)

	for _, partition := range decisions.Partitions {
		for _, item := range partition.Items {

			resp, err := app.NewRequest(t).
				WithPath(fmt.Sprintf("/v1/decision-instances/%d", item.Key)).
				DoOk()

			require.NoError(t, err)

			decision := zenclient.DecisionInstanceDetail{}

			err = json.Unmarshal(resp, &decision)
			require.NoError(t, err)

			return decision
		}
	}

	require.Fail(t, "no decision instance found")

	return zenclient.DecisionInstanceDetail{}
}

type ExpectedDecisionInput struct {
	InputExpression string
	InputId         string
	InputName       string
	InputValue      any
}

func assertDecisionInputs(t testing.TB, inputs *[]zenclient.EvaluatedInput, expectedDecisionInputs []ExpectedDecisionInput) {

	require.NotNil(t, inputs)
	require.Equal(t, len(expectedDecisionInputs), len(*inputs))

	for _, input := range *inputs {
		found := false

		for _, expectedInput := range expectedDecisionInputs {
			if input.InputId == nil ||
				input.InputName == nil ||
				input.InputExpression == nil {
				continue
			}

			var inputValue any
			if input.InputValue != nil {
				inputValue = *input.InputValue
			}

			if *input.InputId == expectedInput.InputId &&
				*input.InputName == expectedInput.InputName &&
				*input.InputExpression == expectedInput.InputExpression &&
				assert.ObjectsAreEqualValues(expectedInput.InputValue, inputValue) {
				found = true
				break
			}
		}
		require.Truef(t, found, "input %+v not found in expected inputs %+v", input, expectedDecisionInputs)
	}
}

func assertDecisionMatchedRule(t testing.TB, rules []zenclient.MatchedRule, expectedRuleId string, expectedDecisionOutputs []ExpectedDecisionOutput) {
	t.Helper()

	for _, rule := range rules {

		if rule.RuleId == nil {
			continue
		}

		if *rule.RuleId == expectedRuleId {
			assertDecisionMatchedRulesOutputs(t, rule.EvaluatedOutputs, expectedDecisionOutputs)
			return
		}
	}

	require.Fail(t, "no matched rule found")
}

type ExpectedDecisionOutput struct {
	OutputId    string
	OutputName  string
	OutputValue any
}

func assertDecisionMatchedRulesOutputs(t testing.TB, outputs *[]zenclient.EvaluatedOutput, expectedDecisionOutputs []ExpectedDecisionOutput) {
	t.Helper()

	require.NotNil(t, outputs)
	require.Equal(t, len(expectedDecisionOutputs), len(*outputs))

	for _, output := range *outputs {
		found := false

		var outputValue any
		if output.OutputValue != nil {
			outputValue = *output.OutputValue
		}

		for _, expectedInput := range expectedDecisionOutputs {
			if output.OutputId == nil ||
				output.OutputName == nil {
				continue
			}

			if *output.OutputId == expectedInput.OutputId &&
				*output.OutputName == expectedInput.OutputName &&
				assert.ObjectsAreEqualValues(expectedInput.OutputValue, outputValue) {
				found = true
				break
			}
		}
		require.Truef(t, found, "output %+v not found in expected outputs %+v", output, expectedDecisionOutputs)
	}
}
