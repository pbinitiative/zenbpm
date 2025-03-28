package bpmn

import (
	"bytes"
	"compress/flate"
	"context"
	"crypto/md5"
	"encoding/ascii85"
	"encoding/hex"
	"encoding/xml"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"io"
	"os"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

// LoadFromFile loads a given BPMN file by filename into the engine
// and returns ProcessInfo details for the deployed workflow
func (state *Engine) LoadFromFile(filename string) (*runtime.ProcessDefinition, error) {
	xmlData, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return state.load(xmlData, filename)
}

// LoadFromBytes loads a given BPMN file by xmlData byte array into the engine
// and returns ProcessInfo details for the deployed workflow
func (state *Engine) LoadFromBytes(xmlData []byte) (*runtime.ProcessDefinition, error) {
	return state.load(xmlData, "")
}

func (state *Engine) load(xmlData []byte, resourceName string) (*runtime.ProcessDefinition, error) {
	md5sum := md5.Sum(xmlData)
	var definitions bpmn20.TDefinitions
	err := xml.Unmarshal(xmlData, &definitions)
	if err != nil {
		return nil, err
	}

	processInfo := runtime.ProcessDefinition{
		Version:          1,
		BpmnProcessId:    definitions.Process.Id,
		ProcessKey:       state.generateKey(),
		Definitions:      definitions,
		BpmnData:         compressAndEncode(xmlData),
		BpmnResourceName: resourceName,
		BpmnChecksum:     md5sum,
	}
	processes := state.FindProcessesById(definitions.Process.Id)
	if len(processes) > 0 {
		if areEqual(processes[0].BpmnChecksum, md5sum) {
			return processes[0], nil
		}
		processInfo.Version = processes[0].Version + 1
	}
	state.persistence.PersistNewProcess(context.TODO(), &processInfo)

	state.exportNewProcessEvent(processInfo, xmlData, resourceName, hex.EncodeToString(md5sum[:]))
	return &processInfo, nil
}

func compressAndEncode(data []byte) string {
	buffer := bytes.Buffer{}
	ascii85Writer := ascii85.NewEncoder(&buffer)
	flateWriter, err := flate.NewWriter(ascii85Writer, flate.BestCompression)
	if err != nil {
		panic("can't initialize flate.Writer, error=" + err.Error())
	}
	_, err = flateWriter.Write(data)
	if err != nil {
		panic("can't write to flate.Writer, error=" + err.Error())
	}
	_ = flateWriter.Flush()
	_ = flateWriter.Close()
	_ = ascii85Writer.Close()
	return buffer.String()
}

func decodeAndDecompress(data string) ([]byte, error) {
	ascii85Reader := ascii85.NewDecoder(bytes.NewBuffer([]byte(data)))
	deflateReader := flate.NewReader(ascii85Reader)
	buffer := bytes.Buffer{}
	_, err := io.Copy(&buffer, deflateReader)
	if err != nil {
		return []byte{}, &BpmnEngineUnmarshallingError{Err: err}
	}
	return buffer.Bytes(), nil
}
