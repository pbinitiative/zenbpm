package bpmn

import (
	"crypto/md5"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadFromBytes_FormattingOnlyRedeployReusesDefinition(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	require.NoError(t, engine.Start(t.Context()))
	defer engine.Stop()

	original := []byte(`<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" id="definitions" targetNamespace="urn:test">
  <bpmn:process id="formatting-test" name="Formatting test" isExecutable="true">
    <bpmn:startEvent id="start"/>
    <bpmn:endEvent id="end"/>
    <bpmn:sequenceFlow id="flow" sourceRef="start" targetRef="end"/>
  </bpmn:process>
</bpmn:definitions>`)
	formatted := []byte(`<?xml version="1.0" encoding="UTF-8"?>

<bpmn:definitions targetNamespace = 'urn:test' id = 'definitions' xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">

  <bpmn:process isExecutable = 'true' name = 'Formatting test' id = 'formatting-test'>
    <bpmn:startEvent id = 'start'></bpmn:startEvent>

    <bpmn:endEvent id = 'end'></bpmn:endEvent>
    <bpmn:sequenceFlow targetRef = 'end' sourceRef = 'start' id = 'flow'></bpmn:sequenceFlow>
  </bpmn:process>

</bpmn:definitions>
`)
	require.NotEqual(t, md5.Sum(original), md5.Sum(formatted), "test inputs must exercise the fallback")

	first, err := engine.LoadFromBytes(t.Context(), original, engine.generateKey())
	require.NoError(t, err)
	second, err := engine.LoadFromBytes(t.Context(), formatted, engine.generateKey())
	require.NoError(t, err)

	assert.Equal(t, first.Key, second.Key)
	assert.Equal(t, int32(1), second.Version)
	assert.Equal(t, string(original), second.BpmnData)
	assert.Equal(t, md5.Sum(original), second.BpmnChecksum, "the stored checksum must remain the raw MD5")

	definitions, err := store.FindProcessDefinitionsById(t.Context(), "formatting-test")
	require.NoError(t, err)
	assert.Len(t, definitions, 1)
}

func TestLoadFromBytes_ContentChangeCreatesNewVersion(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	require.NoError(t, engine.Start(t.Context()))
	defer engine.Stop()

	original := []byte(`<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" id="definitions" targetNamespace="urn:test"><bpmn:process id="content-change" name="Original" isExecutable="true"><bpmn:startEvent id="start"/></bpmn:process></bpmn:definitions>`)
	changed := []byte(`<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" id="definitions" targetNamespace="urn:test"><bpmn:process id="content-change" name="Changed" isExecutable="true"><bpmn:startEvent id="start"/></bpmn:process></bpmn:definitions>`)

	first, err := engine.LoadFromBytes(t.Context(), original, engine.generateKey())
	require.NoError(t, err)
	second, err := engine.LoadFromBytes(t.Context(), changed, engine.generateKey())
	require.NoError(t, err)

	assert.NotEqual(t, first.Key, second.Key)
	assert.Equal(t, int32(2), second.Version)
	assert.Equal(t, md5.Sum(changed), second.BpmnChecksum)
}
