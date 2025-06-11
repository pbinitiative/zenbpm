package bpmn

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"os"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

// LoadFromFile loads a given BPMN file by filename into the engine
// and returns ProcessInfo details for the deployed workflow
func (engine *Engine) LoadFromFile(filename string) (*runtime.ProcessDefinition, error) {
	xmlData, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to load from file: %w", err)
	}
	return engine.load(xmlData, filename, engine.generateKey())
}

// LoadFromBytes loads a given BPMN file by xmlData byte array into the engine
// and returns ProcessInfo details for the deployed workflow
func (engine *Engine) LoadFromBytes(xmlData []byte, key int64) (*runtime.ProcessDefinition, error) {
	def, err := engine.load(xmlData, "", key)
	if err != nil {
		return nil, fmt.Errorf("failed to load from bytes: %w", err)
	}
	return def, nil
}

func (engine *Engine) load(xmlData []byte, resourceName string, key int64) (*runtime.ProcessDefinition, error) {
	md5sum := md5.Sum(xmlData)
	var definitions bpmn20.TDefinitions
	err := xml.Unmarshal(xmlData, &definitions)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal xml data: %w", err)
	}

	processInfo := runtime.ProcessDefinition{
		Version:          1,
		BpmnProcessId:    definitions.Process.Id,
		Key:              key,
		Definitions:      definitions,
		BpmnData:         string(xmlData),
		BpmnResourceName: resourceName,
		BpmnChecksum:     md5sum,
	}
	processes, err := engine.FindProcessesById(definitions.Process.Id)
	if err != nil {
		return nil, fmt.Errorf("failed to load processes by id %s: %w", definitions.Process.Id, err)
	}
	if len(processes) > 0 {
		latestIndex := len(processes) - 1
		if processes[latestIndex].BpmnChecksum == md5sum {
			return &processes[latestIndex], nil
		}
		processInfo.Version = processes[latestIndex].Version + 1
	}
	err = engine.persistence.SaveProcessDefinition(context.TODO(), processInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to save process definition: %w", err)
	}

	engine.exportNewProcessEvent(processInfo, xmlData, resourceName, hex.EncodeToString(md5sum[:]))
	return &processInfo, nil
}
