// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

func Test_Activity_interfaces_implemented(t *testing.T) {
	var _ runtime.Activity = &elementActivity{}
}

func Test_GatewayActivity_interfaces_implemented(t *testing.T) {
	var _ runtime.Activity = &gatewayActivity{}
}

func Test_EventBaseGatewayActivity_interfaces_implemented(t *testing.T) {
	var _ runtime.Activity = &gatewayActivity{}
}
