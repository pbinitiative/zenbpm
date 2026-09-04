package bpmn

import (
	"strings"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
)

func TestLoadFromBytesStoresProcessVersionTag(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))

	const processXML = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zenbpm="http://zenbpm.pbinitiative.org/1.0">
  <bpmn:process id="process-with-version-tag" isExecutable="true">
    <bpmn:extensionElements><zenbpm:versionTag value="stable-release" /></bpmn:extensionElements>
    <bpmn:startEvent id="start"><bpmn:outgoing>to-task</bpmn:outgoing></bpmn:startEvent>
    <bpmn:serviceTask id="task">
      <bpmn:extensionElements><zenbpm:taskDefinition type="test-type" /></bpmn:extensionElements>
      <bpmn:incoming>to-task</bpmn:incoming>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="to-task" sourceRef="start" targetRef="task" />
  </bpmn:process>
</bpmn:definitions>`

	definition, err := engine.LoadFromBytes(t.Context(), []byte(processXML), engine.generateKey())
	assert.NoError(t, err)
	if !assert.NotNil(t, definition) {
		return
	}
	assert.Equal(t, "stable-release", definition.VersionTag)

	stored, err := store.FindProcessDefinitionByKey(t.Context(), definition.Key)
	assert.NoError(t, err)
	assert.Equal(t, "stable-release", stored.VersionTag)
}

func TestLoadFromBytesRejectsDuplicateProcessVersionTag(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))

	const firstProcessXML = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zenbpm="http://zenbpm.pbinitiative.org/1.0">
  <bpmn:process id="version-tag-must-be-unique" name="first definition" isExecutable="true">
    <bpmn:extensionElements><zenbpm:versionTag value="stable-release" /></bpmn:extensionElements>
    <bpmn:startEvent id="start" />
  </bpmn:process>
</bpmn:definitions>`

	first, err := engine.LoadFromBytes(t.Context(), []byte(firstProcessXML), engine.generateKey())
	if !assert.NoError(t, err) {
		return
	}
	assert.NotNil(t, first)

	// An identical deployment is idempotent even though a different definition
	// with this (process ID, versionTag) is rejected below.
	idempotent, err := engine.LoadFromBytes(t.Context(), []byte(firstProcessXML), engine.generateKey())
	assert.NoError(t, err)
	if assert.NotNil(t, idempotent) {
		assert.Equal(t, first.Key, idempotent.Key)
	}

	secondProcessXML := strings.Replace(firstProcessXML, "first definition", "second definition", 1)
	second, err := engine.LoadFromBytes(t.Context(), []byte(secondProcessXML), engine.generateKey())
	assert.Nil(t, second)
	assert.ErrorIs(t, err, storage.ErrUniqueConstraint)

	definitions, err := store.FindProcessDefinitionsById(t.Context(), "version-tag-must-be-unique")
	assert.NoError(t, err)
	assert.Len(t, definitions, 1)
}
