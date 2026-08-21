// Package xmlutil provides XML content comparison helpers.
package xmlutil

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"hash"
	"io"
	"sort"
)

const (
	xmlNamespace = "http://www.w3.org/XML/1998/namespace"

	bpmnModelNamespace = "http://www.omg.org/spec/BPMN/20100524/MODEL"
	bpmnDINamespace    = "http://www.omg.org/spec/BPMN/20100524/DI"
	ddDCNamespace      = "http://www.omg.org/spec/DD/20100524/DC"
	ddDINamespace      = "http://www.omg.org/spec/DD/20100524/DI"

	dmnModelNamespace   = "https://www.omg.org/spec/DMN/20191111/MODEL/"
	dmnDINamespace      = "https://www.omg.org/spec/DMN/20191111/DMNDI/"
	dmnDCNamespace      = "http://www.omg.org/spec/DMN/20180521/DC/"
	dmnDiagramNamespace = "http://www.omg.org/spec/DMN/20180521/DI/"
)

// SameContent reports whether newData is the same XML document as storedData
// ignoring XML formatting differences. Both checksums must be md5(raw).
func SameContent(storedChecksum, newChecksum, storedData, newData []byte) (bool, error) {
	if bytes.Equal(storedChecksum, newChecksum) {
		return true, nil
	}

	storedNormalized, err := normalizedXMLHash(storedData)
	if err != nil {
		return false, fmt.Errorf("failed to normalize stored XML: %w", err)
	}
	newNormalized, err := normalizedXMLHash(newData)
	if err != nil {
		return false, fmt.Errorf("failed to normalize new XML: %w", err)
	}
	return storedNormalized == newNormalized, nil
}

// normalizedXMLHash hashes a length-prefixed token stream so attribute order,
// quoting, and namespace-prefix spellings are normalized.
func normalizedXMLHash(data []byte) ([sha256.Size]byte, error) {
	decoder := xml.NewDecoder(bytes.NewReader(data))
	digest := sha256.New()
	writer := tokenHashWriter{Hash: digest}
	writer.writeBytes([]byte("zenbpm:xml-format-v2"))

	// Stack entries: xml:space=preserve?, has child element?, structural namespace?
	preserveWhitespace := []bool{false}
	elementHasChild := []bool{true}
	structuralWhitespace := []bool{true}
	var text []byte
	flushText := func() {
		if len(text) == 0 {
			return
		}
		if preserveWhitespace[len(preserveWhitespace)-1] ||
			!structuralWhitespace[len(structuralWhitespace)-1] ||
			!elementHasChild[len(elementHasChild)-1] ||
			!isXMLWhitespace(text) {
			writer.writeToken('T', text)
		}
		text = text[:0]
	}

	for {
		token, err := decoder.Token()
		if err == io.EOF {
			flushText()
			break
		}
		if err != nil {
			return [sha256.Size]byte{}, err
		}

		if charData, ok := token.(xml.CharData); ok {
			// Accumulate consecutive CharData tokens; CDATA and entity spellings
			// decode to the same bytes, so coalescing is intentional.
			text = append(text, charData...)
			continue
		}
		if _, isStart := token.(xml.StartElement); isStart {
			elementHasChild[len(elementHasChild)-1] = true
		}
		flushText()

		switch typed := token.(type) {
		case xml.StartElement:
			attributes := append([]xml.Attr(nil), typed.Attr...)
			sort.Slice(attributes, func(i, j int) bool {
				left, right := attributes[i], attributes[j]
				if left.Name.Space != right.Name.Space {
					return left.Name.Space < right.Name.Space
				}
				if left.Name.Local != right.Name.Local {
					return left.Name.Local < right.Name.Local
				}
				return left.Value < right.Value
			})
			for i := 1; i < len(attributes); i++ {
				if attributes[i-1].Name == attributes[i].Name {
					return [sha256.Size]byte{}, fmt.Errorf(
						"duplicate attribute {%s}%s",
						attributes[i].Name.Space,
						attributes[i].Name.Local,
					)
				}
			}

			writer.writeByte('S')
			writer.writeName(typed.Name)
			writer.writeUint64(uint64(len(attributes)))
			for _, attribute := range attributes {
				writer.writeName(attribute.Name)
				writer.writeBytes([]byte(attribute.Value))
			}

			preserve := preserveWhitespace[len(preserveWhitespace)-1]
			for _, attribute := range attributes {
				if attribute.Name.Space == xmlNamespace && attribute.Name.Local == "space" {
					switch attribute.Value {
					case "preserve":
						preserve = true
					case "default":
						preserve = false
					}
				}
			}
			preserveWhitespace = append(preserveWhitespace, preserve)
			elementHasChild = append(elementHasChild, false)
			structuralWhitespace = append(structuralWhitespace, isStructuralElement(typed.Name))

		case xml.EndElement:
			writer.writeByte('E')
			writer.writeName(typed.Name)
			preserveWhitespace = preserveWhitespace[:len(preserveWhitespace)-1]
			elementHasChild = elementHasChild[:len(elementHasChild)-1]
			structuralWhitespace = structuralWhitespace[:len(structuralWhitespace)-1]
		case xml.Comment:
			writer.writeToken('C', typed)
		case xml.ProcInst:
			writer.writeToken('P', []byte(typed.Target), typed.Inst)
		case xml.Directive:
			writer.writeToken('D', typed)
		default:
			return [sha256.Size]byte{}, fmt.Errorf("unsupported XML token %T", token)
		}
	}

	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result, nil
}

type tokenHashWriter struct {
	hash.Hash
}

func (writer tokenHashWriter) writeToken(kind byte, fields ...[]byte) {
	writer.writeByte(kind)
	writer.writeUint64(uint64(len(fields)))
	for _, field := range fields {
		writer.writeBytes(field)
	}
}

func (writer tokenHashWriter) writeName(name xml.Name) {
	writer.writeBytes([]byte(name.Space))
	writer.writeBytes([]byte(name.Local))
}

func (writer tokenHashWriter) writeBytes(value []byte) {
	writer.writeUint64(uint64(len(value)))
	_, _ = writer.Write(value)
}

func (writer tokenHashWriter) writeUint64(value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = writer.Write(encoded[:])
}

func (writer tokenHashWriter) writeByte(value byte) {
	_, _ = writer.Write([]byte{value})
}

func isStructuralElement(name xml.Name) bool {
	if name.Space == bpmnModelNamespace {
		switch name.Local {
		case "documentation",
			"script",
			"text",
			"expression",
			"formalExpression",
			"completionCondition",
			"from",
			"to",
			"activationCondition",
			"condition",
			"loopCardinality",
			"conditionExpression",
			"loopCondition",
			"timeDate",
			"timeDuration",
			"timeCycle",
			"transformation",
			"baseElementWithMixedContent":
			return false
		default:
			return true
		}
	}

	switch name.Space {
	case bpmnDINamespace,
		ddDCNamespace,
		ddDINamespace,
		dmnModelNamespace,
		dmnDINamespace,
		dmnDCNamespace,
		dmnDiagramNamespace:
		return true
	default:
		return false
	}
}

func isXMLWhitespace(value []byte) bool {
	for _, character := range value {
		switch character {
		case ' ', '\t', '\r', '\n':
		default:
			return false
		}
	}
	return true
}
