// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

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
