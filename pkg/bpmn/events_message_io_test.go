package bpmn

import (
	"os"
	"strings"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/require"
)

func TestIntermediateMessageCatchEventPreservesInputScopeAndHistoryMetadata(t *testing.T) {
	const fixture = "./test-cases/simple-intermediate-message-catch-event.bpmn"
	const originalOutputMapping = `<zenbpm:output source="= foo" target="mappedFoo" />`
	const inputScopedOutputMapping = `<zenbpm:output source="= mappedInputFoo" target="mappedFoo" />`

	bpmnData, err := os.ReadFile(fixture)
	require.NoError(t, err)
	modifiedBPMN := strings.Replace(string(bpmnData), originalOutputMapping, inputScopedOutputMapping, 1)
	require.NotEqual(t, string(bpmnData), modifiedBPMN, "test fixture output mapping was not replaced")

	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	engine.Start(t.Context())
	t.Cleanup(engine.Stop)

	definition, err := engine.LoadFromBytes(t.Context(), []byte(modifiedBPMN), engine.generateKey())
	require.NoError(t, err)
	instance, err := engine.CreateInstanceByKey(t.Context(), definition.Key, map[string]any{
		"inputFoo": "input-value",
	})
	require.NoError(t, err)

	correlationKey := "key"
	require.NoError(t, engine.PublishMessageByName(t.Context(), "msg", &correlationKey, map[string]any{
		"foo": "message-value",
	}))

	persistedInstance, err := store.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
	require.NoError(t, err)
	require.Equal(t, "input-value", persistedInstance.ProcessInstance().GetVariable("mappedFoo"))

	flowElements, err := store.GetFlowElementInstancesByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key, false)
	require.NoError(t, err)
	for _, flowElement := range flowElements {
		if flowElement.ElementId != "msg" {
			continue
		}
		require.Equal(t, string(bpmn20.ElementTypeIntermediateCatchEvent), flowElement.ElementType)
		require.Equal(t, map[string]any{
			"inputFoo":       "input-value",
			"mappedInputFoo": "input-value",
		}, flowElement.InputVariables)
		require.Equal(t, map[string]any{"mappedFoo": "input-value"}, flowElement.OutputVariables)
		return
	}
	t.Fatal("intermediate message catch event history was not found")
}
