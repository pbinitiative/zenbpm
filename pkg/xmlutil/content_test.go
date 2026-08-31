package xmlutil

import (
	"crypto/md5"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSameContent_RawChecksumFastPath(t *testing.T) {
	data := []byte(`not XML, but the raw bytes are identical`)
	checksum := md5.Sum(data)

	same, err := SameContent(checksum[:], checksum[:], nil, data)

	require.NoError(t, err)
	assert.True(t, same)
}

func TestSameContent_IgnoresXMLFormatting(t *testing.T) {
	stored := []byte(`<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:ext="urn:extension" id="definitions">
  <bpmn:process id="process" name="Example">
    <ext:value enabled="true">a&lt;b</ext:value>
  </bpmn:process>
</bpmn:definitions>`)
	formatted := []byte(`<?xml version="1.0"?>

<bpmn:definitions id = 'definitions' xmlns:ext="urn:extension" xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">

  <bpmn:process name='Example' id='process'>
    <ext:value enabled = 'true'><![CDATA[a<b]]></ext:value>
  </bpmn:process>

</bpmn:definitions>
`)
	checksum := md5.Sum(stored)
	formattedChecksum := md5.Sum(formatted)
	require.NotEqual(t, checksum, formattedChecksum, "test inputs must exercise the fallback")

	same, err := SameContent(checksum[:], formattedChecksum[:], stored, formatted)

	require.NoError(t, err)
	assert.True(t, same)
}

func TestSameContent_WhitespacePolicy(t *testing.T) {
	t.Run("BPMN structural indentation", func(t *testing.T) {
		stored := []byte(`<process xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL">

  <startEvent/>
  <endEvent/>
</process>`)
		formatted := []byte(`<process xmlns="http://www.omg.org/spec/BPMN/20100524/MODEL"><startEvent/><endEvent/></process>`)
		storedChecksum := md5.Sum(stored)
		formattedChecksum := md5.Sum(formatted)

		same, err := SameContent(storedChecksum[:], formattedChecksum[:], stored, formatted)

		require.NoError(t, err)
		assert.True(t, same)
	})

	t.Run("DMN structural indentation and line endings", func(t *testing.T) {
		stored := []byte("<definitions xmlns=\"https://www.omg.org/spec/DMN/20191111/MODEL/\">\r\n  <decision/>\r\n  <decisionService/>\r\n</definitions>")
		formatted := []byte(`<definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/"><decision/><decisionService/></definitions>`)
		storedChecksum := md5.Sum(stored)
		formattedChecksum := md5.Sum(formatted)

		same, err := SameContent(storedChecksum[:], formattedChecksum[:], stored, formatted)

		require.NoError(t, err)
		assert.True(t, same)
	})

	t.Run("mixed-content whitespace in an extension namespace", func(t *testing.T) {
		stored := []byte(`<definitions xmlns:e="urn:extension"><e:value><e:a/> <e:b/></e:value></definitions>`)
		formatted := []byte(`<definitions xmlns:e="urn:extension"><e:value><e:a/><e:b/></e:value></definitions>`)
		storedChecksum := md5.Sum(stored)
		formattedChecksum := md5.Sum(formatted)

		same, err := SameContent(storedChecksum[:], formattedChecksum[:], stored, formatted)

		require.NoError(t, err)
		assert.False(t, same)
	})

	t.Run("mixed-content whitespace in BPMN documentation", func(t *testing.T) {
		stored := []byte(`<bpmn:documentation xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:e="urn:extension"><e:a/> <e:b/></bpmn:documentation>`)
		formatted := []byte(`<bpmn:documentation xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:e="urn:extension"><e:a/><e:b/></bpmn:documentation>`)
		storedChecksum := md5.Sum(stored)
		formattedChecksum := md5.Sum(formatted)

		same, err := SameContent(storedChecksum[:], formattedChecksum[:], stored, formatted)

		require.NoError(t, err)
		assert.False(t, same)
	})

	t.Run("BPMN DI structural indentation", func(t *testing.T) {
		stored := []byte(`<bpmndi:BPMNDiagram xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI" xmlns:dc="http://www.omg.org/spec/DD/20100524/DC">
  <bpmndi:BPMNPlane>
    <bpmndi:BPMNShape><dc:Bounds x="10" y="20"/></bpmndi:BPMNShape>
  </bpmndi:BPMNPlane>
</bpmndi:BPMNDiagram>`)
		formatted := []byte(`<bpmndi:BPMNDiagram xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI" xmlns:dc="http://www.omg.org/spec/DD/20100524/DC"><bpmndi:BPMNPlane><bpmndi:BPMNShape><dc:Bounds x="10" y="20"/></bpmndi:BPMNShape></bpmndi:BPMNPlane></bpmndi:BPMNDiagram>`)
		storedChecksum := md5.Sum(stored)
		formattedChecksum := md5.Sum(formatted)

		same, err := SameContent(storedChecksum[:], formattedChecksum[:], stored, formatted)

		require.NoError(t, err)
		assert.True(t, same)
	})

	t.Run("zenbpm:ioMapping structural indentation", func(t *testing.T) {
		stored := []byte(`<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zenbpm="http://zenbpm.pbinitiative.org/1.0">
  <bpmn:process id="p">
    <zenbpm:ioMapping>
      <zenbpm:input source="=a" target="x"/>
      <zenbpm:input source="=b" target="y"/>
    </zenbpm:ioMapping>
  </bpmn:process>
</bpmn:definitions>`)
		formatted := []byte(`<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zenbpm="http://zenbpm.pbinitiative.org/1.0"><bpmn:process id="p"><zenbpm:ioMapping><zenbpm:input source="=a" target="x"/><zenbpm:input source="=b" target="y"/></zenbpm:ioMapping></bpmn:process></bpmn:definitions>`)
		storedChecksum := md5.Sum(stored)
		formattedChecksum := md5.Sum(formatted)

		same, err := SameContent(storedChecksum[:], formattedChecksum[:], stored, formatted)

		require.NoError(t, err)
		assert.True(t, same)
	})

	t.Run("zeebe:ioMapping structural indentation (legacy namespace)", func(t *testing.T) {
		stored := []byte(`<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="p">
    <zeebe:ioMapping>
      <zeebe:input source="=a" target="x"/>
      <zeebe:output source="=x" target="y"/>
    </zeebe:ioMapping>
  </bpmn:process>
</bpmn:definitions>`)
		formatted := []byte(`<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"><bpmn:process id="p"><zeebe:ioMapping><zeebe:input source="=a" target="x"/><zeebe:output source="=x" target="y"/></zeebe:ioMapping></bpmn:process></bpmn:definitions>`)
		storedChecksum := md5.Sum(stored)
		formattedChecksum := md5.Sum(formatted)

		same, err := SameContent(storedChecksum[:], formattedChecksum[:], stored, formatted)

		require.NoError(t, err)
		assert.True(t, same)
	})

	t.Run("zenbpm:taskHeaders and assignmentDefinition structural indentation", func(t *testing.T) {
		stored := []byte(`<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zenbpm="http://zenbpm.pbinitiative.org/1.0">
  <bpmn:process id="p">
    <bpmn:userTask id="u">
      <bpmn:extensionElements>
        <zenbpm:taskHeaders>
          <zenbpm:header key="a" value="1"/>
          <zenbpm:header key="b" value="2"/>
        </zenbpm:taskHeaders>
        <zenbpm:assignmentDefinition assignee="x" candidateGroups="g1, g2"/>
      </bpmn:extensionElements>
    </bpmn:userTask>
  </bpmn:process>
</bpmn:definitions>`)
		formatted := []byte(`<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zenbpm="http://zenbpm.pbinitiative.org/1.0"><bpmn:process id="p"><bpmn:userTask id="u"><bpmn:extensionElements><zenbpm:taskHeaders><zenbpm:header key="a" value="1"/><zenbpm:header key="b" value="2"/></zenbpm:taskHeaders><zenbpm:assignmentDefinition assignee="x" candidateGroups="g1, g2"/></bpmn:extensionElements></bpmn:userTask></bpmn:process></bpmn:definitions>`)
		storedChecksum := md5.Sum(stored)
		formattedChecksum := md5.Sum(formatted)

		same, err := SameContent(storedChecksum[:], formattedChecksum[:], stored, formatted)

		require.NoError(t, err)
		assert.True(t, same)
	})
}

func TestSameContent_IsConservative(t *testing.T) {
	tests := []struct {
		name   string
		stored string
		new    string
	}{
		{
			name:   "element text whitespace",
			stored: `<definitions><condition>${x > 10}</condition></definitions>`,
			new:    `<definitions><condition>${x >  10}</condition></definitions>`,
		},
		{
			name:   "whitespace-only BPMN expression",
			stored: `<bpmn:conditionExpression xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"> </bpmn:conditionExpression>`,
			new:    `<bpmn:conditionExpression xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"></bpmn:conditionExpression>`,
		},
		{
			name:   "attribute value",
			stored: `<definitions><extension value="one"/></definitions>`,
			new:    `<definitions><extension value="two"/></definitions>`,
		},
		{
			name:   "unknown extension content",
			stored: `<definitions xmlns:e="urn:extension"><e:value>one</e:value></definitions>`,
			new:    `<definitions xmlns:e="urn:extension"><e:value>two</e:value></definitions>`,
		},
		{
			name:   "comment",
			stored: `<definitions><!-- deployment note --><process/></definitions>`,
			new:    `<definitions><process/></definitions>`,
		},
		{
			name:   "namespace declaration",
			stored: `<b:definitions xmlns:b="urn:bpmn"><b:process/></b:definitions>`,
			new:    `<x:definitions xmlns:x="urn:bpmn"><x:process/></x:definitions>`,
		},
		{
			name:   "XML declaration",
			stored: `<?xml version="1.0"?><definitions/>`,
			new:    `<definitions/>`,
		},
		{
			name:   "non-XML whitespace text",
			stored: "<definitions>\u00a0</definitions>",
			new:    `<definitions/>`,
		},
		{
			name:   "whitespace-only extension value",
			stored: `<definitions xmlns:e="urn:extension"><e:value> </e:value></definitions>`,
			new:    `<definitions xmlns:e="urn:extension"><e:value></e:value></definitions>`,
		},
		{
			name:   "xml space preserve",
			stored: `<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xml:space="preserve"> <bpmn:process/></bpmn:definitions>`,
			new:    `<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xml:space="preserve"><bpmn:process/></bpmn:definitions>`,
		},
		{
			name:   "zenbpm:ioMapping attribute value change",
			stored: `<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zenbpm="http://zenbpm.pbinitiative.org/1.0"><zenbpm:ioMapping><zenbpm:input source="=a" target="x"/></zenbpm:ioMapping></bpmn:definitions>`,
			new:    `<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zenbpm="http://zenbpm.pbinitiative.org/1.0"><zenbpm:ioMapping><zenbpm:input source="=b" target="x"/></zenbpm:ioMapping></bpmn:definitions>`,
		},
		{
			name:   "zenbpm:ioMapping child added",
			stored: `<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zenbpm="http://zenbpm.pbinitiative.org/1.0"><zenbpm:ioMapping><zenbpm:input source="=a" target="x"/></zenbpm:ioMapping></bpmn:definitions>`,
			new:    `<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zenbpm="http://zenbpm.pbinitiative.org/1.0"><zenbpm:ioMapping><zenbpm:input source="=a" target="x"/><zenbpm:output source="=x" target="y"/></zenbpm:ioMapping></bpmn:definitions>`,
		},
		{
			name:   "zeebe:ioMapping attribute value change (legacy namespace)",
			stored: `<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"><zeebe:ioMapping><zeebe:input source="=a" target="x"/></zeebe:ioMapping></bpmn:definitions>`,
			new:    `<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"><zeebe:ioMapping><zeebe:input source="=b" target="x"/></zeebe:ioMapping></bpmn:definitions>`,
		},
		{
			name:   "BPMN DI coordinate",
			stored: `<bpmndi:BPMNDiagram xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI" xmlns:dc="http://www.omg.org/spec/DD/20100524/DC"><bpmndi:BPMNShape><dc:Bounds x="10" y="20"/></bpmndi:BPMNShape></bpmndi:BPMNDiagram>`,
			new:    `<bpmndi:BPMNDiagram xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI" xmlns:dc="http://www.omg.org/spec/DD/20100524/DC"><bpmndi:BPMNShape><dc:Bounds x="11" y="20"/></bpmndi:BPMNShape></bpmndi:BPMNDiagram>`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			checksum := md5.Sum([]byte(test.stored))
			newChecksum := md5.Sum([]byte(test.new))
			same, err := SameContent(checksum[:], newChecksum[:], []byte(test.stored), []byte(test.new))
			require.NoError(t, err)
			assert.False(t, same)
		})
	}
}

func TestSameContent_ReturnsXMLParsingErrors(t *testing.T) {
	t.Run("stored XML", func(t *testing.T) {
		stored := []byte(`<definitions>`)
		newData := []byte(`<definitions/>`)
		newChecksum := md5.Sum(newData)
		_, err := SameContent(make([]byte, md5.Size), newChecksum[:], stored, newData)

		require.Error(t, err)
		assert.ErrorContains(t, err, "failed to normalize stored XML")
	})

	t.Run("new XML", func(t *testing.T) {
		stored := []byte(`<definitions><process/></definitions>`)
		checksum := md5.Sum(stored)
		newData := []byte(`<definitions>`)
		newChecksum := md5.Sum(newData)
		_, err := SameContent(checksum[:], newChecksum[:], stored, newData)

		require.Error(t, err)
		assert.ErrorContains(t, err, "failed to normalize new XML")
	})

	t.Run("duplicate attributes", func(t *testing.T) {
		stored := []byte(`<definitions><process name="A"/></definitions>`)
		checksum := md5.Sum(stored)
		newData := []byte(`<definitions><process name="A" name="B"/></definitions>`)
		newChecksum := md5.Sum(newData)
		_, err := SameContent(checksum[:], newChecksum[:], stored, newData)

		require.Error(t, err)
		assert.ErrorContains(t, err, "duplicate attribute")
	})
}
