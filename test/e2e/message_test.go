// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package e2e

import (
	"fmt"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/stretchr/testify/assert"
)

// TODO: Test with multiple partitions/nodes
func TestRestApiMessage(t *testing.T) {
	var instance public.ProcessInstance
	var definition public.ProcessDefinitionSimple
	err := deployDefinition(t, "message-intermediate-catch-event.bpmn")
	assert.NoError(t, err)
	defintitions, err := listProcessDefinitions(t)
	assert.NoError(t, err)
	for _, def := range defintitions {
		if def.BpmnProcessId == "message-intermediate-catch-event" {
			definition = def
			break
		}
	}
	instance, err = createProcessInstance(t, definition.Key, map[string]any{
		"testVar": 123,
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, instance.Key)

	_, err = createProcessInstance(t, definition.Key, map[string]any{
		"testVar": 123,
	})
	assert.Error(t, err)

	t.Run("publish message", func(t *testing.T) {
		err := publishMessage(t, "globalMsgRef", "correlation-key-one", &map[string]any{
			"test-var": "test",
		})
		assert.NoError(t, err)
		processInstance, err := getProcessInstance(t, instance.Key)
		assert.NoError(t, err)
		assert.NotEmpty(t, processInstance.Variables)
		assert.NotEmpty(t, processInstance.Variables["test-var"])
		assert.Equal(t, "test", processInstance.Variables["test-var"])
		assert.Equal(t, float64(123), processInstance.Variables["testVar"])

		err = publishMessage(t, "globalMsgRef", "correlation-key-one", &map[string]any{
			"test-var": "test",
		})
		assert.Error(t, err)
	})

	_, err = createProcessInstance(t, definition.Key, map[string]any{
		"testVar": 123,
	})
	assert.NoError(t, err)
}

func publishMessage(t testing.TB, name string, correlationKey string, vars *map[string]any) error {
	_, status, _, err := app.NewRequest(t).
		WithPath("/v1/messages").
		WithMethod("POST").
		WithBody(public.PublishMessageJSONBody{
			CorrelationKey: correlationKey,
			MessageName:    name,
			Variables:      vars,
		}).
		Do()
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}
	if status != 201 {
		return fmt.Errorf("failed to publish message expected status 201")
	}
	return nil
}
