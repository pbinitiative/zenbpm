package dmn

import (
	"testing"

	dmnModel "github.com/pbinitiative/zenbpm/pkg/dmn/model/dmn"
	"github.com/stretchr/testify/require"
)

func TestDecisionTableValidationRegressions(t *testing.T) {
	engine := NewEngine()
	t.Cleanup(engine.Stop)

	t.Run("accepts and evaluates comma-separated variable references", func(t *testing.T) {
		decisionTable := validationTestDecisionTable("low, high", "1")

		err := engine.validateDecisionTable("decision", decisionTable)
		require.NoError(t, err)

		_, matchedRules, _, err := engine.evaluateDecisionTable(
			decisionTable,
			"decision",
			map[string]interface{}{"value": 5, "low": 5, "high": 10},
		)
		require.NoError(t, err)
		require.Len(t, matchedRules, 1)
		require.Equal(t, "rule", matchedRules[0].RuleId)
	})

	t.Run("treats a whitespace-only input entry as a wildcard during validation and evaluation", func(t *testing.T) {
		decisionTable := validationTestDecisionTable(" \t\n", "1")

		err := engine.validateDecisionTable("decision", decisionTable)
		require.NoError(t, err)

		_, matchedRules, _, err := engine.evaluateDecisionTable(
			decisionTable,
			"decision",
			map[string]interface{}{"value": 5},
		)
		require.NoError(t, err)
		require.Len(t, matchedRules, 1)
		require.Equal(t, "rule", matchedRules[0].RuleId)
	})

	t.Run("rejects an output entry with invalid FEEL syntax", func(t *testing.T) {
		decisionTable := validationTestDecisionTable("-", "1 +")

		err := engine.validateDecisionTable("decision", decisionTable)

		require.Error(t, err)
		require.ErrorContains(t, err, `output entry "output-entry" contains invalid or unsupported FEEL expression "1 +"`)
	})

	t.Run("rejects an input entry with invalid FEEL unary-test syntax", func(t *testing.T) {
		decisionTable := validationTestDecisionTable("1 +", "1")

		err := engine.validateDecisionTable("decision", decisionTable)

		require.Error(t, err)
		require.ErrorContains(t, err, `input entry "input-entry" contains invalid or unsupported FEEL unary test "1 +"`)
	})

	t.Run("rejects an input expression with invalid FEEL syntax", func(t *testing.T) {
		decisionTable := validationTestDecisionTable("-", "1")
		decisionTable.Inputs[0].InputExpression.Text = "1 +"

		err := engine.validateDecisionTable("decision", decisionTable)

		require.Error(t, err)
		require.ErrorContains(t, err, `input "input" expression "input-expression" contains invalid or unsupported FEEL expression "1 +"`)
	})

	t.Run("accepts a syntactically valid output variable reference without evaluating it", func(t *testing.T) {
		decisionTable := validationTestDecisionTable("-", "customerCategory")

		err := engine.validateDecisionTable("decision", decisionTable)

		require.NoError(t, err)
	})
}

func validationTestDecisionTable(inputEntry string, outputEntry string) *dmnModel.TDecisionTable {
	return &dmnModel.TDecisionTable{
		HitPolicy: dmnModel.HitPolicyFirst,
		Inputs: []dmnModel.TInput{
			{
				Id: "input",
				InputExpression: dmnModel.TInputExpression{
					Id:      "input-expression",
					TypeRef: "number",
					Text:    "value",
				},
			},
		},
		Outputs: []dmnModel.TOutput{
			{Id: "output", Name: "result", TypeRef: "number"},
		},
		Rules: []dmnModel.TRule{
			{
				Id:          "rule",
				InputEntry:  []dmnModel.TInputEntry{{Id: "input-entry", Text: inputEntry}},
				OutputEntry: []dmnModel.TOutputEntry{{Id: "output-entry", Text: outputEntry}},
			},
		},
	}
}
