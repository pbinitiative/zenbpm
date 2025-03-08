package storage

import (
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/var_holder"
	"time"
)

type ProcessDefinition struct {
	BpmnProcessId    string // The ID as defined in the BPMN file
	Version          int32  // A version of the process, default=1, incremented, when another process with the same ID is loaded
	ProcessKey       int64  // The engines key for this given BpmnProcessId with Version
	BpmnData         string // the raw source data, compressed and encoded via ascii85
	BpmnChecksum     string // internal checksum to identify different versions; using sha1 as string, all lower case; similar as git-hashes
	BpmnResourceName string // some name for the resource; optional, can be empty
}

type ProcessInstance struct {
	ProcessDefinition *ProcessDefinition        `json:"-"`
	InstanceKey       int64                     `json:"ik"`
	VariableHolder    var_holder.VariableHolder `json:"vh,omitempty"`
	CreatedAt         time.Time                 `json:"c"`
	State             bpmn20.ActivityState      `json:"s"`
	//CaughtEvents      []catchEvent              `json:"ce,omitempty"`
	//activities        []activity
}
