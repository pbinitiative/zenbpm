package bpmn

import (
	"context"
	"fmt"
	"strings"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

func (engine *Engine) handleIntermediateThrowLinkEvent(_ context.Context, instance runtime.ProcessInstance, ite *bpmn20.TIntermediateThrowEvent, currentToken runtime.ExecutionToken) (runtime.ExecutionToken, map[string]any, error) {
	linkDef := ite.EventDefinition.(bpmn20.TLinkEventDefinition)
	linkName := linkDef.Name
	elementVarHolder := runtime.NewVariableHolder(&instance.ProcessInstance().VariableHolder, nil)
	outputVariables, err := elementVarHolder.PropagateMappedOutputsOrAll(ite.Output, nil, engine.evaluateExpression)
	if err != nil {
		msg := fmt.Sprintf("Can't evaluate expression in element id=%s name=%s", ite.Id, ite.Name)
		currentToken.State = runtime.TokenStateFailed
		return currentToken, nil, &ExpressionEvaluationError{Msg: msg, Err: err}
	}
	if len(strings.TrimSpace(linkName)) == 0 {
		currentToken.State = runtime.TokenStateFailed
		return currentToken, nil, fmt.Errorf("missing link name in link intermediate throw event element id=%s name=%s", ite.Id, ite.Name)
	}
	for _, ice := range instance.ProcessInstance().Definition.Definitions.Process.IntermediateCatchEvent {
		link, ok := ice.EventDefinition.(bpmn20.TLinkEventDefinition)
		if !ok || link.Name != linkName {
			continue
		}

		currentToken.ElementId = ice.Id
		currentToken.ElementInstanceKey = engine.generateKey()
		currentToken.State = runtime.TokenStateRunning
		return currentToken, outputVariables, nil
	}
	currentToken.State = runtime.TokenStateFailed
	return currentToken, nil, fmt.Errorf("failed to find link %v target in process instance %d", ite, instance.ProcessInstance().Key)
}
