package dmn

import (
	"github.com/pbinitiative/zenbpm/pkg/dmn/model/dmn"
	"github.com/pbinitiative/zenbpm/pkg/dmn/runtime"
)

func findDecision(dmnDefinition *runtime.DecisionDefinition, decisionId string) *dmn.TDecision {
	for _, decision := range dmnDefinition.Definitions.Decisions {
		if decision.Id == decisionId {
			return &decision
		}
	}
	return nil
}
