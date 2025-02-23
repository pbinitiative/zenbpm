package dmn

import (
	"github.com/pbinitiative/zenbpm/pkg/storage/dmn"
)

func findDecision(dmnDefinition *DmnDefinition, decisionId string) *dmn.TDecision {
	for _, decision := range dmnDefinition.definitions.Decisions {
		if decision.Id == decisionId {
			return &decision
		}
	}
	return nil
}
