package bpmn

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

const XmlTestString = `<?xml version="1.0" encoding="UTF-8"?><bpmn:process id="Simple_Task_Process" name="aName" isExecutable="true"></bpmn:process></xml>`

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
