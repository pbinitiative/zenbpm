// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package bpmn20

import (
	"encoding/xml"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_BoundaryEvent_Parses(t *testing.T) {
	var definitions TDefinitions
	var xmlData, err = os.ReadFile("../../test-cases/message-boundary-event-interrupting.bpmn")

	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}
	err1 := xml.Unmarshal(xmlData, &definitions)
	if err1 != nil {
		t.Fatalf("failed to unmarshal XML: %v", err)
	}
	assert.Equal(t, 1, len(definitions.Process.BoundaryEvent))

	be := definitions.Process.BoundaryEvent[0]
	assert.Equal(t, 1, len(be.Output))
	assert.Equal(t, "service-task-id", be.AttachedToRef)
	assert.Equal(t, true, be.CancellActivity)

	assert.Equal(t, ElementTypeBoundaryEvent, be.GetType())

	var m TMessageEventDefinition
	if messageEventDefinition, ok := be.EventDefinition.(TMessageEventDefinition); ok {
		m = messageEventDefinition
	} else {
		assert.Fail(t, "Expected TMessageEventDefinition")
	}

	assert.Equal(t, "simple-boundary-id", m.MessageRef)

}
