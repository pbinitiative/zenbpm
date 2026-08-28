package dmn

import (
	"errors"
	"testing"

	dmnModel "github.com/pbinitiative/zenbpm/pkg/dmn/model/dmn"
	"github.com/stretchr/testify/require"
)

type legacyTrackingFeelRuntime struct {
	evaluateCalls  int
	unaryTestCalls int
}

func (r *legacyTrackingFeelRuntime) UnaryTest(string, map[string]any) (bool, error) {
	r.unaryTestCalls++
	return true, nil
}

func (r *legacyTrackingFeelRuntime) Evaluate(string, map[string]any) (any, error) {
	r.evaluateCalls++
	return nil, nil
}

func (r *legacyTrackingFeelRuntime) Stop() {}

type trackingDmnFeelRuntime struct {
	legacyTrackingFeelRuntime
	strictUnaryTestCalls    int
	validateExpressionCalls int
	validateUnaryTestCalls  int
	strictUnaryTestErr      error
}

func (r *trackingDmnFeelRuntime) UnaryTestStrict(string, map[string]any) (bool, error) {
	r.strictUnaryTestCalls++
	return false, r.strictUnaryTestErr
}

func (r *trackingDmnFeelRuntime) ValidateExpression(string) error {
	r.validateExpressionCalls++
	return nil
}

func (r *trackingDmnFeelRuntime) ValidateUnaryTest(string) error {
	r.validateUnaryTestCalls++
	return nil
}

func customRuntimeTestDefinitions(inputEntry string, outputEntry string) *dmnModel.TDefinitions {
	return &dmnModel.TDefinitions{
		Id: "definitions",
		Decisions: []dmnModel.TDecision{
			{
				Id:            "decision",
				DecisionTable: validationTestDecisionTable(inputEntry, outputEntry),
			},
		},
	}
}

func TestDmnCustomRuntimeContract(t *testing.T) {
	t.Run("rejects an incomplete runtime before deployment validation executes expressions", func(t *testing.T) {
		feelRuntime := &legacyTrackingFeelRuntime{}
		engine := NewEngine(EngineWithFeel(feelRuntime))

		_, _, err := engine.SaveDmnResourceDefinition(
			t.Context(),
			customRuntimeTestDefinitions("VIP", "customerCategory"),
			[]byte("custom runtime test definition"),
			1,
		)

		require.ErrorContains(t, err, "does not support DMN decision tables")
		require.Zero(t, feelRuntime.evaluateCalls)
		require.Zero(t, feelRuntime.unaryTestCalls)
	})

	t.Run("rejects an incomplete runtime before decision evaluation executes expressions", func(t *testing.T) {
		feelRuntime := &legacyTrackingFeelRuntime{}
		engine := NewEngine(EngineWithFeel(feelRuntime))

		_, _, _, err := engine.evaluateDecisionTable(
			validationTestDecisionTable("VIP", "1"),
			"decision",
			map[string]interface{}{"value": nil},
		)

		require.ErrorContains(t, err, "does not support DMN decision tables")
		require.Zero(t, feelRuntime.evaluateCalls)
		require.Zero(t, feelRuntime.unaryTestCalls)
	})

	t.Run("deployment validation uses only parse-only capability methods", func(t *testing.T) {
		feelRuntime := &trackingDmnFeelRuntime{}
		engine := NewEngine(EngineWithFeel(feelRuntime))

		_, _, err := engine.SaveDmnResourceDefinition(
			t.Context(),
			customRuntimeTestDefinitions("VIP", "customerCategory"),
			[]byte("custom runtime test definition"),
			1,
		)

		require.NoError(t, err)
		require.Equal(t, 2, feelRuntime.validateExpressionCalls)
		require.Equal(t, 1, feelRuntime.validateUnaryTestCalls)
		require.Zero(t, feelRuntime.evaluateCalls)
		require.Zero(t, feelRuntime.unaryTestCalls)
		require.Zero(t, feelRuntime.strictUnaryTestCalls)
	})

	t.Run("decision evaluation always uses strict unary tests", func(t *testing.T) {
		feelRuntime := &trackingDmnFeelRuntime{
			strictUnaryTestErr: errors.New("unknown variable(s) in unary test: VIP"),
		}
		engine := NewEngine(EngineWithFeel(feelRuntime))

		_, _, _, err := engine.evaluateDecisionTable(
			validationTestDecisionTable("VIP", "1"),
			"decision",
			map[string]interface{}{"value": nil},
		)

		require.ErrorContains(t, err, "unknown variable(s) in unary test: VIP")
		require.Equal(t, 1, feelRuntime.evaluateCalls)
		require.Zero(t, feelRuntime.unaryTestCalls)
		require.Equal(t, 1, feelRuntime.strictUnaryTestCalls)
	})
}
