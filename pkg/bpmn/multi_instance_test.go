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
	"github.com/stretchr/testify/assert"
)

func Test_multi_instance_service_task(t *testing.T) {
	// given
	process, _ := bpmnEngine.LoadFromFile("./test-cases/multi-instance-service-task.bpmn")

	// when
	pi, err := bpmnEngine.CreateInstance(t.Context(), process, nil)
	assert.NoError(t, err)

	// then
	// TODO
	assert.Equal(t, runtime.ActivityStateActive, pi.GetState(),
		"fail message...")
}
