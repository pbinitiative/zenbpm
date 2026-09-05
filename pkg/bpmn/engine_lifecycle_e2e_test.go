package bpmn

import (
	"path/filepath"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// TestEngineLifecycleEndToEnd exercises a complete engine lifecycle:
// construction with default (engine-owned) script runtimes, FEEL evaluation
// through gateway conditions, DMN decision evaluation through the embedded DMN
// engine (which reuses the BPMN engine's FEEL runtime), and shutdown. After
// Stop, goleak verifies that no engine-owned script-pool cleanup goroutines are left running.
func TestEngineLifecycleEndToEnd(t *testing.T) {
	defer goleak.VerifyNone(t, sharedEngineGoleakOptions()...)

	engine := NewEngine(EngineWithStorage(inmemory.NewStorage()))
	defer engine.Stop()

	// FEEL evaluation: exclusive gateway with a FEEL condition.
	gatewayProcess, err := engine.LoadFromFile(t.Context(), "./test-cases/exclusive-gateway-with-condition.bpmn")
	require.NoError(t, err)

	cp := CallPath{}
	gatewayHandler := engine.NewTaskHandler().Type("task-b").Handler(cp.TaskHandler)
	defer engine.RemoveHandler(gatewayHandler)

	gatewayInstance, err := engine.CreateInstanceByKey(t.Context(), gatewayProcess.Key, map[string]interface{}{"price": -50})
	require.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, gatewayInstance.ProcessInstance().State)
	assert.Equal(t, "task-b", cp.CallPath)

	// DMN evaluation: business rule task evaluated by the embedded DMN engine,
	// which shares the BPMN engine's FEEL runtime.
	dmnDefinition, dmnXML, err := engine.dmnEngine.ParseDmnFromFile(filepath.Join("..", "dmn", "test-data", "bulk-evaluation-test", "can-autoliquidate-rule.dmn"))
	require.NoError(t, err)
	_, _, err = engine.dmnEngine.SaveDmnResourceDefinition(t.Context(), dmnDefinition, dmnXML, engine.generateKey())
	require.NoError(t, err)

	ruleProcess, err := engine.LoadFromFile(t.Context(), filepath.Join(".", "test-cases", "business_rule", "simple-business-rule-task-local.bpmn"))
	require.NoError(t, err)

	ruleInstance, err := engine.CreateInstanceByKey(t.Context(), ruleProcess.Key, nil)
	require.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, ruleInstance.ProcessInstance().State)
	assert.Equal(t, true, ruleInstance.ProcessInstance().VariableHolder.LocalVariables()["OutputTestResultVariable"])

	// Shutdown: Stop must terminate the engine-owned FEEL and JS pools exactly
	// once; the deferred goleak verification proves their cleanup goroutines exit.
	engine.Stop()
	engine.Stop()
}
