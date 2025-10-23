package dmn

import (
	"github.com/pbinitiative/zenbpm/pkg/dmn/model/dmn"
	"github.com/pbinitiative/zenbpm/pkg/dmn/runtime"
)

func findDecisionDefinition(dmnDefinition *runtime.DmnResourceDefinition, decisionId string) *dmn.TDecision {
	for _, decision := range dmnDefinition.Definitions.Decisions {
		if decision.Id == decisionId {
			return &decision
		}
	}
	return nil
}
