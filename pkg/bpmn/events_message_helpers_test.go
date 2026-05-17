package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildProcessDefinitionWithMessage constructs a minimal ProcessDefinition
// containing one TMessage identified by messageId.
func buildProcessDefinitionWithMessage(messageId, messageName, correlationKey string) runtime.ProcessDefinition {
	return runtime.ProcessDefinition{
		Definitions: bpmn20.TDefinitions{
			TRootElementsContainer: bpmn20.TRootElementsContainer{
				Messages: []bpmn20.TMessage{
					{
						Id:   messageId,
						Name: messageName,
						Extension: bpmn20.TSubscription{
							CorrelationKey: correlationKey,
						},
					},
				},
			},
		},
	}
}

func messageDef(ref string) bpmn20.TMessageEventDefinition {
	return bpmn20.TMessageEventDefinition{MessageRef: ref}
}

// newIsolatedEngine returns a fresh, isolated Engine backed by its own inmemory storage
// so these unit tests do not depend on the package-global bpmnEngine.
func newIsolatedEngine() Engine {
	return NewEngine(EngineWithStorage(inmemory.NewStorage()))
}

// newProcessInstanceWithVars wraps a runtime.DefaultProcessInstance with the given local
// variables, so the correlation-key expression evaluator can look them up.
func newProcessInstanceWithVars(vars map[string]any) runtime.ProcessInstance {
	return &runtime.DefaultProcessInstance{
		ProcessInstanceData: runtime.ProcessInstanceData{
			VariableHolder: runtime.NewVariableHolder(nil, vars),
		},
	}
}

// ---------------------------------------------------------------------------
// getMessageName
// ---------------------------------------------------------------------------

func TestGetMessageName_ReturnsNameForKnownRef(t *testing.T) {
	engine := newIsolatedEngine()
	pd := buildProcessDefinitionWithMessage("msg-1", "OrderPlaced", "")
	name, err := engine.getMessageName(pd, messageDef("msg-1"))
	require.NoError(t, err)
	assert.Equal(t, "OrderPlaced", name)
}

func TestGetMessageName_ErrorForUnknownRef(t *testing.T) {
	engine := newIsolatedEngine()
	pd := buildProcessDefinitionWithMessage("msg-1", "OrderPlaced", "")
	_, err := engine.getMessageName(pd, messageDef("does-not-exist"))
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// getMessageCorrelationKey — static (no expression prefix)
// ---------------------------------------------------------------------------

func TestGetMessageCorrelationKey_StaticKey_NoInstance(t *testing.T) {
	engine := newIsolatedEngine()
	pd := buildProcessDefinitionWithMessage("msg-1", "OrderPlaced", "order-42")
	key, err := engine.getMessageCorrelationKey(pd, nil, messageDef("msg-1"))
	require.NoError(t, err)
	assert.Equal(t, "order-42", key)
}

func TestGetMessageCorrelationKey_StaticKey_WithInstance(t *testing.T) {
	engine := newIsolatedEngine()
	pd := buildProcessDefinitionWithMessage("msg-1", "OrderPlaced", "order-42")
	pi := newProcessInstanceWithVars(nil)
	key, err := engine.getMessageCorrelationKey(pd, &pi, messageDef("msg-1"))
	require.NoError(t, err)
	assert.Equal(t, "order-42", key)
}

// ---------------------------------------------------------------------------
// getMessageCorrelationKey — expression (starts with "=")
// ---------------------------------------------------------------------------

func TestGetMessageCorrelationKey_ExpressionKey_EvaluatedFromInstanceVariables(t *testing.T) {
	engine := newIsolatedEngine()
	pd := buildProcessDefinitionWithMessage("msg-1", "OrderPlaced", `=orderId`)
	pi := newProcessInstanceWithVars(map[string]any{"orderId": "order-99"})
	key, err := engine.getMessageCorrelationKey(pd, &pi, messageDef("msg-1"))
	require.NoError(t, err)
	assert.Equal(t, "order-99", key)
}

func TestGetMessageCorrelationKey_ExpressionKey_NilInstance_UsesEmptyVars(t *testing.T) {
	engine := newIsolatedEngine()
	pd := buildProcessDefinitionWithMessage("msg-1", "OrderPlaced", `="static-key"`)
	key, err := engine.getMessageCorrelationKey(pd, nil, messageDef("msg-1"))
	require.NoError(t, err)
	assert.Equal(t, "static-key", key)
}

func TestGetMessageCorrelationKey_ErrorForUnknownRef(t *testing.T) {
	engine := newIsolatedEngine()
	pd := buildProcessDefinitionWithMessage("msg-1", "OrderPlaced", "order-42")
	_, err := engine.getMessageCorrelationKey(pd, nil, messageDef("unknown-ref"))
	require.Error(t, err)
}
