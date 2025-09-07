// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package bpmn

import (
	"os"
	"strings"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
)

func Test_FindProcessInstance_ComfortFunction_ReturnsNilIfNoInstanceFound(t *testing.T) {
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	_, err := bpmnEngine.FindProcessInstance(1234)

	assert.NotNil(t, err)
	assert.ErrorIs(t, err, storage.ErrNotFound, "expected ErrNotFound for not existing process")
}

func Test_FindProcessesById_ComfortFunction_ReturnsEmptyArrayIfNoInstanceFound(t *testing.T) {
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))
	instanceInfos, err := bpmnEngine.FindProcessesById("unknown-id")

	assert.Empty(t, instanceInfos)
	assert.Nil(t, err)
}

func Test_FindProcessesById_result_is_ordered_by_version(t *testing.T) {
	store := inmemory.NewStorage()
	bpmnEngine := NewEngine(EngineWithStorage(store))

	// setup
	dataV1, err := os.ReadFile("./test-cases/simple_task.bpmn")
	assert.Nil(t, err)
	_, err = bpmnEngine.LoadFromBytes(dataV1, bpmnEngine.generateKey())
	assert.Nil(t, err)

	// given
	dataV2 := strings.Replace(string(dataV1), "StartEvent_1", "StartEvent_2", -1)
	assert.NotEqual(t, dataV2, string(dataV1))
	_, err = bpmnEngine.LoadFromBytes([]byte(dataV2), bpmnEngine.generateKey())
	assert.Nil(t, err)

	// when
	infos, err := bpmnEngine.FindProcessesById("Simple_Task_Process")
	assert.Nil(t, err)

	// then
	for i := range len(infos) - 1 {
		assert.Less(t, infos[i].Version, infos[i+1].Version)
	}
}
