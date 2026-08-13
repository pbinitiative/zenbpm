package e2e

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestDmnDeploymentValidation(t *testing.T) {
	validDeployments := []struct {
		name       string
		inputType  string
		expression string
	}{
		{name: "accepts quoted text", inputType: "string", expression: `"VIP"`},
		{name: "accepts quoted text followed by numbers", inputType: "string", expression: `"VIP123"`},
		{name: "accepts quoted numbers followed by text", inputType: "string", expression: `"123VIP"`},
		{name: "accepts quoted text with spaces and numbers", inputType: "string", expression: `"VIP CUSTOMER 123"`},
		{name: "accepts quoted text with punctuation and numbers", inputType: "string", expression: `"VIP-123/PLUS.2"`},
		{name: "accepts quoted unicode text with numbers", inputType: "string", expression: `"ZÁKAZNÍK42"`},
		{name: "accepts a list of quoted values", inputType: "string", expression: `"VIP1", "STANDARD2"`},
		{name: "accepts a variable reference for a string input", inputType: "string", expression: "VIP"},
		{name: "accepts null for a string input", inputType: "string", expression: "null"},
		{name: "accepts true for a string input", inputType: "string", expression: "true"},
		{name: "accepts false for a string input", inputType: "string", expression: "false"},
		{name: "accepts commas inside nested expressions", inputType: "string", expression: `not("INTERNAL, TEST")`},
		{name: "accepts comma-separated variable references", inputType: "number", expression: "low, high"},
		{name: "accepts an empty string input entry as a wildcard", inputType: "string", expression: ""},
		{name: "accepts a whitespace-only input entry as a wildcard", inputType: "string", expression: " \t "},
		{name: "accepts an unquoted number for a number input", inputType: "number", expression: "123"},
	}
	for _, testCase := range validDeployments {
		t.Run(testCase.name, func(t *testing.T) {
			definitionID := uniqueDmnResourceDefinitionTestValue("validDmnInputEntry")
			definition := dmnDeploymentValidationDefinition(t, definitionID, testCase.inputType, "value", testCase.expression)

			response, err := app.restClient.CreateDmnResourceDefinitionWithBodyWithResponse(
				t.Context(),
				"application/xml",
				strings.NewReader(definition),
			)

			require.NoError(t, err)
			require.Equal(t, http.StatusCreated, response.StatusCode())
			require.NotNil(t, response.JSON201)
			require.NotZero(t, response.JSON201.DmnResourceDefinitionKey)

			definitions, err := listDecisionDefinitions(t, &zenclient.GetDmnResourceDefinitionsParams{
				DmnResourceDefinitionId: &definitionID,
			})
			require.NoError(t, err)
			require.Len(t, definitions, 1)
			require.Equal(t, definitionID, definitions[0].DmnResourceDefinitionId)
			require.Equal(t, response.JSON201.DmnResourceDefinitionKey, definitions[0].Key)
		})
	}

	t.Run("evaluates a variable reference in a string input entry", func(t *testing.T) {
		definitionID := uniqueDmnResourceDefinitionTestValue("stringInputVariableReference")
		decisionID := definitionID + "Decision"
		definition := dmnDeploymentValidationDefinition(t, definitionID, "string", "value", "expectedValue")

		response, err := app.restClient.CreateDmnResourceDefinitionWithBodyWithResponse(
			t.Context(),
			"application/xml",
			strings.NewReader(definition),
		)

		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, response.StatusCode())

		result, err := evaluateDecision(
			t,
			zenclient.EvaluateDecisionJSONBodyBindingTypeLatest,
			&definitionID,
			decisionID,
			nil,
			map[string]any{
				"expectedValue": "VIP",
				"value":         "VIP",
			},
		)

		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.EvaluatedDecisions, 1)
		require.Len(t, result.EvaluatedDecisions[0].MatchedRules, 1)
		require.Equal(t, "rule", result.EvaluatedDecisions[0].MatchedRules[0].RuleId)
	})

	t.Run("fails evaluation when an input entry references a missing variable", func(t *testing.T) {
		definitionID := uniqueDmnResourceDefinitionTestValue("missingStringInputVariableReference")
		decisionID := definitionID + "Decision"
		definition := dmnDeploymentValidationMissingVariableReferenceDefinition(definitionID)

		response, err := app.restClient.CreateDmnResourceDefinitionWithBodyWithResponse(
			t.Context(),
			"application/xml",
			strings.NewReader(definition),
		)

		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, response.StatusCode())

		result, err := evaluateDecision(
			t,
			zenclient.EvaluateDecisionJSONBodyBindingTypeLatest,
			&definitionID,
			decisionID,
			nil,
			map[string]any{"amount": 10},
		)

		require.Nil(t, result)
		require.ErrorContains(t, err, "unknown variable(s) in unary test: VIP")
	})

	t.Run("evaluates comma-separated variable references", func(t *testing.T) {
		definitionID := uniqueDmnResourceDefinitionTestValue("inputVariableReferences")
		decisionID := definitionID + "Decision"
		definition := dmnDeploymentValidationDefinition(t, definitionID, "number", "value", "low, high")

		response, err := app.restClient.CreateDmnResourceDefinitionWithBodyWithResponse(
			t.Context(),
			"application/xml",
			strings.NewReader(definition),
		)

		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, response.StatusCode())

		result, err := evaluateDecision(
			t,
			zenclient.EvaluateDecisionJSONBodyBindingTypeLatest,
			&definitionID,
			decisionID,
			nil,
			map[string]any{
				"value": 5,
				"low":   5,
				"high":  10,
			},
		)

		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.EvaluatedDecisions, 1)
		require.Len(t, result.EvaluatedDecisions[0].MatchedRules, 1)
		require.Equal(t, "rule", result.EvaluatedDecisions[0].MatchedRules[0].RuleId)
	})

	t.Run("evaluates a whitespace-only input entry as a wildcard", func(t *testing.T) {
		definitionID := uniqueDmnResourceDefinitionTestValue("whitespaceInputWildcard")
		decisionID := definitionID + "Decision"
		definition := dmnDeploymentValidationDefinition(t, definitionID, "number", "value", " \t ")

		response, err := app.restClient.CreateDmnResourceDefinitionWithBodyWithResponse(
			t.Context(),
			"application/xml",
			strings.NewReader(definition),
		)

		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, response.StatusCode())

		result, err := evaluateDecision(
			t,
			zenclient.EvaluateDecisionJSONBodyBindingTypeLatest,
			&definitionID,
			decisionID,
			nil,
			map[string]any{"value": 5},
		)

		require.NoError(t, err)
		require.NotNil(t, result)
		require.Len(t, result.EvaluatedDecisions, 1)
		require.Len(t, result.EvaluatedDecisions[0].MatchedRules, 1)
		require.Equal(t, "rule", result.EvaluatedDecisions[0].MatchedRules[0].RuleId)
	})

	t.Run("rejects invalid FEEL unary test syntax", func(t *testing.T) {
		definitionID := uniqueDmnResourceDefinitionTestValue("invalidFeelUnaryTest")
		definition := dmnDeploymentValidationDefinition(t, definitionID, "number", "value", "1 +")

		response, err := app.restClient.CreateDmnResourceDefinitionWithBodyWithResponse(
			t.Context(),
			"application/xml",
			strings.NewReader(definition),
		)

		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, response.StatusCode())
		require.NotNil(t, response.JSON400)
		require.Equal(t, "BAD_REQUEST", response.JSON400.Code)
		require.Contains(t, response.JSON400.Message, `input entry "input-entry" contains invalid or unsupported FEEL unary test "1 +"`)

		definitions, err := listDecisionDefinitions(t, &zenclient.GetDmnResourceDefinitionsParams{
			DmnResourceDefinitionId: &definitionID,
		})
		require.NoError(t, err)
		require.Empty(t, definitions)
	})

	t.Run("rejects invalid FEEL input expression syntax", func(t *testing.T) {
		definitionID := uniqueDmnResourceDefinitionTestValue("invalidFeelInputExpression")
		definition := dmnDeploymentValidationDefinition(t, definitionID, "number", "1 +", "-")

		response, err := app.restClient.CreateDmnResourceDefinitionWithBodyWithResponse(
			t.Context(),
			"application/xml",
			strings.NewReader(definition),
		)

		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, response.StatusCode())
		require.NotNil(t, response.JSON400)
		require.Equal(t, "BAD_REQUEST", response.JSON400.Code)
		require.Contains(t, response.JSON400.Message, `input "input" expression "input-expression" contains invalid or unsupported FEEL expression "1 +"`)

		definitions, err := listDecisionDefinitions(t, &zenclient.GetDmnResourceDefinitionsParams{
			DmnResourceDefinitionId: &definitionID,
		})
		require.NoError(t, err)
		require.Empty(t, definitions)
	})

	t.Run("rejects invalid FEEL output expression syntax", func(t *testing.T) {
		definitionID := uniqueDmnResourceDefinitionTestValue("invalidFeelOutputExpression")
		definition := dmnDeploymentValidationStringOutputDefinition(t, definitionID, "1 +")

		response, err := app.restClient.CreateDmnResourceDefinitionWithBodyWithResponse(
			t.Context(),
			"application/xml",
			strings.NewReader(definition),
		)

		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, response.StatusCode())
		require.NotNil(t, response.JSON400)
		require.Equal(t, "BAD_REQUEST", response.JSON400.Code)
		require.Contains(t, response.JSON400.Message, `output entry "output-entry" contains invalid or unsupported FEEL expression "1 +"`)

		definitions, err := listDecisionDefinitions(t, &zenclient.GetDmnResourceDefinitionsParams{
			DmnResourceDefinitionId: &definitionID,
		})
		require.NoError(t, err)
		require.Empty(t, definitions)
	})

	t.Run("rejects a rule with fewer output entries than outputs", func(t *testing.T) {
		definitionID := uniqueDmnResourceDefinitionTestValue("invalidOutputEntryCount")
		definition := dmnDeploymentValidationOutputCountDefinition(definitionID)

		response, err := app.restClient.CreateDmnResourceDefinitionWithBodyWithResponse(
			t.Context(),
			"application/xml",
			strings.NewReader(definition),
		)

		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, response.StatusCode())
		require.NotNil(t, response.JSON400)
		require.Equal(t, "BAD_REQUEST", response.JSON400.Code)
		require.Contains(t, response.JSON400.Message, `rule "rule" has 1 output entries, expected 2`)

		definitions, err := listDecisionDefinitions(t, &zenclient.GetDmnResourceDefinitionsParams{
			DmnResourceDefinitionId: &definitionID,
		})
		require.NoError(t, err)
		require.Empty(t, definitions)
	})

	t.Run("rejects an invalid decision table nested in contexts", func(t *testing.T) {
		definitionID := uniqueDmnResourceDefinitionTestValue("invalidNestedDecisionTable")
		definition := dmnDeploymentValidationNestedContextDefinition(t, definitionID, "1 +")

		response, err := app.restClient.CreateDmnResourceDefinitionWithBodyWithResponse(
			t.Context(),
			"application/xml",
			strings.NewReader(definition),
		)

		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, response.StatusCode())
		require.NotNil(t, response.JSON400)
		require.Equal(t, "BAD_REQUEST", response.JSON400.Code)
		require.Contains(t, response.JSON400.Message, `input entry "nested-input-entry" contains invalid or unsupported FEEL unary test "1 +"`)

		definitions, err := listDecisionDefinitions(t, &zenclient.GetDmnResourceDefinitionsParams{
			DmnResourceDefinitionId: &definitionID,
		})
		require.NoError(t, err)
		require.Empty(t, definitions)
	})

	t.Run("accepts a string output expression that references a variable", func(t *testing.T) {
		definitionID := uniqueDmnResourceDefinitionTestValue("validOutputVariable")
		definition := dmnDeploymentValidationStringOutputDefinition(t, definitionID, "customerCategory")

		response, err := app.restClient.CreateDmnResourceDefinitionWithBodyWithResponse(
			t.Context(),
			"application/xml",
			strings.NewReader(definition),
		)

		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, response.StatusCode())
		require.NotNil(t, response.JSON201)
		require.NotZero(t, response.JSON201.DmnResourceDefinitionKey)

		definitions, err := listDecisionDefinitions(t, &zenclient.GetDmnResourceDefinitionsParams{
			DmnResourceDefinitionId: &definitionID,
		})
		require.NoError(t, err)
		require.Len(t, definitions, 1)
		require.Equal(t, definitionID, definitions[0].DmnResourceDefinitionId)
		require.Equal(t, response.JSON201.DmnResourceDefinitionKey, definitions[0].Key)
	})
}

func dmnDeploymentValidationDefinition(t testing.TB, definitionID string, inputType string, inputExpression string, inputEntry string) string {
	t.Helper()

	var escapedInputExpression strings.Builder
	require.NoError(t, xml.EscapeText(&escapedInputExpression, []byte(inputExpression)))

	var escapedInputEntry strings.Builder
	require.NoError(t, xml.EscapeText(&escapedInputEntry, []byte(inputEntry)))

	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" id="%s" name="DMN deployment validation" namespace="https://pbinitiative.com/zenbpm">
  <decision id="%sDecision" name="DMN deployment validation decision">
    <decisionTable id="decision-table" hitPolicy="FIRST">
      <input id="input" label="Input">
        <inputExpression id="input-expression" typeRef="%s">
          <text>%s</text>
        </inputExpression>
      </input>
      <output id="output" name="result" typeRef="number" />
      <rule id="rule">
        <inputEntry id="input-entry">
          <text>%s</text>
        </inputEntry>
        <outputEntry id="output-entry">
          <text>1</text>
        </outputEntry>
      </rule>
    </decisionTable>
  </decision>
</definitions>`, definitionID, definitionID, inputType, escapedInputExpression.String(), escapedInputEntry.String())
}

func dmnDeploymentValidationOutputCountDefinition(definitionID string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" id="%s" name="DMN deployment validation" namespace="https://pbinitiative.com/zenbpm">
  <decision id="%sDecision" name="DMN deployment validation decision">
    <decisionTable id="decision-table" hitPolicy="FIRST">
      <input id="input" label="Input">
        <inputExpression id="input-expression" typeRef="string">
          <text>value</text>
        </inputExpression>
      </input>
      <output id="output" name="result" typeRef="string" />
      <output id="second-output" name="secondResult" typeRef="string" />
      <rule id="rule">
        <inputEntry id="input-entry">
          <text>"VIP"</text>
        </inputEntry>
        <outputEntry id="output-entry">
          <text>"result"</text>
        </outputEntry>
      </rule>
    </decisionTable>
  </decision>
</definitions>`, definitionID, definitionID)
}

func dmnDeploymentValidationMissingVariableReferenceDefinition(definitionID string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" id="%s" name="DMN deployment validation" namespace="https://pbinitiative.com/zenbpm">
  <decision id="%sDecision" name="DMN missing variable reference decision">
    <decisionTable id="decision-table" hitPolicy="FIRST">
      <input id="customer-type-input" label="Customer type">
        <inputExpression id="customer-type-expression" typeRef="string">
          <text>customer_type</text>
        </inputExpression>
      </input>
      <input id="amount-input" label="Amount">
        <inputExpression id="amount-expression" typeRef="number">
          <text>amount</text>
        </inputExpression>
      </input>
      <output id="discount-output" name="discount" typeRef="number" />
      <rule id="vip-rule">
        <inputEntry id="vip-customer-type-entry">
          <text>VIP</text>
        </inputEntry>
        <inputEntry id="vip-amount-entry">
          <text></text>
        </inputEntry>
        <outputEntry id="vip-discount-entry">
          <text>20</text>
        </outputEntry>
      </rule>
      <rule id="low-amount-rule">
        <inputEntry id="low-amount-customer-type-entry">
          <text></text>
        </inputEntry>
        <inputEntry id="low-amount-entry">
          <text>&lt; 500</text>
        </inputEntry>
        <outputEntry id="low-amount-discount-entry">
          <text>0</text>
        </outputEntry>
      </rule>
    </decisionTable>
  </decision>
</definitions>`, definitionID, definitionID)
}

func dmnDeploymentValidationNestedContextDefinition(t testing.TB, definitionID string, inputEntry string) string {
	t.Helper()

	var escapedInputEntry strings.Builder
	require.NoError(t, xml.EscapeText(&escapedInputEntry, []byte(inputEntry)))

	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" id="%s" name="DMN deployment validation" namespace="https://pbinitiative.com/zenbpm">
  <decision id="%sDecision" name="DMN nested context validation decision">
    <context>
      <contextEntry>
        <context>
          <contextEntry>
            <decisionTable id="nested-decision-table" hitPolicy="FIRST">
              <input id="nested-input" label="Input">
                <inputExpression id="nested-input-expression" typeRef="string">
                  <text>value</text>
                </inputExpression>
              </input>
              <output id="nested-output" name="result" typeRef="number" />
              <rule id="nested-rule">
                <inputEntry id="nested-input-entry">
                  <text>%s</text>
                </inputEntry>
                <outputEntry id="nested-output-entry">
                  <text>1</text>
                </outputEntry>
              </rule>
            </decisionTable>
          </contextEntry>
        </context>
      </contextEntry>
    </context>
  </decision>
</definitions>`, definitionID, definitionID, escapedInputEntry.String())
}

func dmnDeploymentValidationStringOutputDefinition(t testing.TB, definitionID string, outputEntry string) string {
	t.Helper()

	var escapedOutputEntry strings.Builder
	require.NoError(t, xml.EscapeText(&escapedOutputEntry, []byte(outputEntry)))

	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" id="%s" name="DMN deployment validation" namespace="https://pbinitiative.com/zenbpm">
  <decision id="%sDecision" name="DMN output expression validation decision">
    <decisionTable id="decision-table" hitPolicy="FIRST">
      <input id="input" label="Input">
        <inputExpression id="input-expression" typeRef="string">
          <text>value</text>
        </inputExpression>
      </input>
      <output id="output" name="result" typeRef="string" />
      <rule id="rule">
        <inputEntry id="input-entry">
          <text>"VIP"</text>
        </inputEntry>
        <outputEntry id="output-entry">
          <text>%s</text>
        </outputEntry>
      </rule>
    </decisionTable>
  </decision>
</definitions>`, definitionID, definitionID, escapedOutputEntry.String())
}
