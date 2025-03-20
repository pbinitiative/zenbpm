package inmemory

import (
	"encoding/hex"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

type processDefinition struct {
	bpmnProcessId    string
	version          int32
	processKey       int64
	definitions      bpmn20.TDefinitions
	bpmnData         string
	bpmnResourceName string
	bpmnChecksum     [16]byte
}

func (p *processDefinition) BpmnProcessId() string {
	return p.bpmnProcessId
}

func (p *processDefinition) Version() int32 {
	return p.version
}

func (p *processDefinition) ProcessKey() int64 {
	return p.processKey
}

func (p *processDefinition) BpmnData() string {
	return p.bpmnData
}

func (p *processDefinition) BpmnChecksum() string {
	return hex.EncodeToString(p.bpmnChecksum[:])
}

func (p *processDefinition) BpmnResourceName() string {
	return p.bpmnResourceName
}
