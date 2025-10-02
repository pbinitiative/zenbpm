package bpmn

import (
	"context"
	"fmt"
	"strings"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

func (engine *Engine) handleIntermediateThrowLinkEvent(ctx context.Context, instance *runtime.ProcessInstance, ite *bpmn20.TIntermediateThrowEvent, currentToken runtime.ExecutionToken) (runtime.ExecutionToken, error) {
	linkDef := ite.EventDefinition.(bpmn20.TLinkEventDefinition)
	linkName := linkDef.Name
	elementVarHolder := runtime.NewVariableHolder(&instance.VariableHolder, nil)
	if err := propagateProcessInstanceVariables(&elementVarHolder, ite.Output); err != nil {
		msg := fmt.Sprintf("Can't evaluate expression in element id=%s name=%s", ite.Id, ite.Name)
		currentToken.State = runtime.TokenStateFailed
		return currentToken, &ExpressionEvaluationError{Msg: msg, Err: err}
	}
	if len(strings.TrimSpace(linkName)) == 0 {
		currentToken.State = runtime.TokenStateFailed
		return currentToken, fmt.Errorf("missing link name in link intermediate throw event element id=%s name=%s", ite.Id, ite.Name)
	}
	for _, ice := range instance.Definition.Definitions.Process.IntermediateCatchEvent {
		link := ice.EventDefinition.(bpmn20.TLinkEventDefinition)
		if link.Name != linkName {
			continue
		}
		elementVarHolder := runtime.NewVariableHolder(&instance.VariableHolder, nil)
		if err := propagateProcessInstanceVariables(&elementVarHolder, ice.Output); err != nil {
			msg := fmt.Sprintf("Can't evaluate expression in element id=%s name=%s", ice.Id, ice.Name)
			currentToken.State = runtime.TokenStateFailed
			return currentToken, &ExpressionEvaluationError{Msg: msg, Err: err}
		}

		currentToken.ElementId = ice.Id
		currentToken.ElementInstanceKey = engine.generateKey()
		currentToken.State = runtime.TokenStateRunning
		return currentToken, nil
	}
	currentToken.State = runtime.TokenStateFailed
	return currentToken, fmt.Errorf("failed to find link %v target in process instance %d", ite, instance.Key)
}
