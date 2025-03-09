package storage

import (
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"time"
)

type ProcessDefinition interface {
	// BpmnProcessId the ID as defined in the BPMN file
	BpmnProcessId() string

	// Version of the process, default=1, incremented, when another process with the same ID is loaded
	Version() int32

	// ProcessKey the engines key for this given BpmnProcessId with Version
	ProcessKey() int64

	// BpmnData the raw source data, compressed and encoded via ascii85
	BpmnData() string

	// BpmnChecksum internal checksum to identify different versions; using sha1 as string, all lower case; similar as git-hashes
	BpmnChecksum() string

	// BpmnResourceName some name for the resource; optional, can be empty
	BpmnResourceName() string
}

type ProcessInstance interface {
	ProcessDefinition() ProcessDefinition
	InstanceKey() int64
	VariableHolder() VariableHolder // TODO: change type and prevent circular dependencies
	CreatedAt() time.Time
	State() bpmn20.ActivityState
	//CaughtEvents      []catchEvent  // TODO: check if needed
}

type VariableHolder interface {
	Parent() VariableHolder
	Variables() map[string]interface{}
}
