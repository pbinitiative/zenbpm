// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package bpmn20

import "testing"

// tests to get quick compiler warnings, when interface is not correctly implemented

func Test_all_interfaces_implemented(t *testing.T) {
	var _ InternalTask = &TServiceTask{}
	var _ InternalTask = &TUserTask{}
	var _ InternalTask = &TBusinessRuleTask{}

	var _ BaseElement = &TStartEvent{}
	var _ BaseElement = &TEndEvent{}
	var _ BaseElement = &TServiceTask{}
	var _ BaseElement = &TUserTask{}
	var _ BaseElement = &TParallelGateway{}
	var _ BaseElement = &TExclusiveGateway{}
	var _ BaseElement = &TIntermediateCatchEvent{}
	var _ BaseElement = &TIntermediateThrowEvent{}
	var _ BaseElement = &TEventBasedGateway{}
	var _ BaseElement = &TInclusiveGateway{}
	var _ BaseElement = &TCallActivity{}
	var _ BaseElement = &TBusinessRuleTask{}
}
