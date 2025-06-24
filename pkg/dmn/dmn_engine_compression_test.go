package dmn

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

const XmlTestString = `<?xml version="1.0" encoding="UTF-8"?><definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" xmlns:dmndi="https://www.omg.org/spec/DMN/20191111/DMNDI/" xmlns:dc="http://www.omg.org/spec/DMN/20180521/DC/" xmlns:modeler="http://camunda.org/schema/modeler/1.0" id="example_canAutoliqudate" name="DRD" namespace="http://camunda.org/schema/1.0/dmn" exporter="Camunda Modeler" exporterVersion="5.35.0" modeler:executionPlatform="Camunda Cloud" modeler:executionPlatformVersion="8.5.0">  <decision id="Decision_0v8xeu8"><decisionTable id="DecisionTable_1r7lo3q"><input id="InputClause_0vo3n89"><inputExpression id="LiteralExpression_1tui1z2" typeRef="string" /></input><output id="OutputClause_1y3a0c8" typeRef="string" /></decisionTable></decision><dmndi:DMNDI><dmndi:DMNDiagram><dmndi:DMNShape dmnElementRef="example_canAutoliqudateRule"><dc:Bounds height="80" width="180" x="160" y="100" /></dmndi:DMNShape><dmndi:DMNShape id="DMNShape_0qa4v5i" dmnElementRef="Decision_0v8xeu8"><dc:Bounds height="80" width="180" x="440" y="100" /></dmndi:DMNShape></dmndi:DMNDiagram></dmndi:DMNDI></definitions>`

func Test_compress_and_encode_produces_ascii_chars(t *testing.T) {
	str := compressAndEncode([]byte(XmlTestString))
	encodedBytes := []byte(str)
	for i := 0; i < len(encodedBytes); i++ {
		b := encodedBytes[i]
		t.Run(fmt.Sprintf("string, index=%d", i), func(t *testing.T) {
			assert.GreaterOrEqual(t, b, byte(33), "every encoded byte shall have an ordinary value of 33 <= x <= 117")
			assert.LessOrEqual(t, b, byte(117), "every encoded byte shall have an ordinary value of 33 <= x <= 117")
		})

	}
}

func Test_compress_and_decompress_roundtrip(t *testing.T) {
	encoded := compressAndEncode([]byte(XmlTestString))
	decoded, err := decodeAndDecompress(encoded)

	assert.Nil(t, err)
	assert.Equal(t, []byte(XmlTestString), decoded)
}
