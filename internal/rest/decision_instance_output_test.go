package rest

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The engine persists the decision output as raw JSON: an object, an array
// (COLLECT), a bare scalar (single unnamed output) or null (FEEL null, no
// rule matched). Every one of them must be valid against the published contract,
// and so must a detail without output, which omits the field.
func TestDecisionInstanceDetailOutputMatchesSpec(t *testing.T) {
	spec, err := public.GetSpec()
	require.NoError(t, err)
	schema := spec.Components.Schemas["DecisionInstanceDetail"].Value

	marshal := func(t *testing.T, output *json.RawMessage) string {
		t.Helper()
		body, err := json.Marshal(public.DecisionInstanceDetail{
			Key:                      1,
			DmnResourceDefinitionKey: 2,
			EvaluatedAt:              time.UnixMilli(0),
			EvaluatedDecisions:       []public.EvaluatedDecision{},
			DecisionOutput:           output,
		})
		require.NoError(t, err)

		var value any
		require.NoError(t, json.Unmarshal(body, &value))
		assert.NoError(t, schema.VisitJSON(value))
		return string(body)
	}

	for _, output := range []string{`{"sport":"squash"}`, `[{"discount":0.1}]`, `"squash"`, `0`, `1.5`, `false`, `null`} {
		t.Run(output, func(t *testing.T) {
			raw := json.RawMessage(output)
			assert.Contains(t, marshal(t, &raw), `"decisionOutput":`+output)
		})
	}

	t.Run("no output", func(t *testing.T) {
		assert.NotContains(t, marshal(t, nil), `"decisionOutput"`)
	})
}
