package storage

import (
	"time"
)

type ProcessDefinition interface {
	// BpmnProcessId the ID as defined in the BPMN file
	BpmnProcessId() string
	// Version of the process, default=1, incremented, when another process with the same ID is loaded
	Version() int32
	// ProcessKey the engines key for this given BpmnProcessId with Version
	ProcessKey() int64 // TODO: use SnowflakeId
	// BpmnData the raw source data, compressed and encoded via ascii85
	BpmnData() string
	// BpmnChecksum internal checksum to identify different versions; using sha1 as string, all lower case; similar as git-hashes
	BpmnChecksum() string
	// BpmnResourceName some name for the resource; optional, can be empty
	BpmnResourceName() string
}

type ProcessInstance interface {
	InstanceKey() int64 // TODO: use SnowflakeId
	VariableHolder() VariableHolder
	CreatedAt() time.Time
	State() string // TODO think of type change bpmn.ActivityState
	//caughtEvents      []catchEvent  // TODO: check if needed
}

type VariableHolder interface {
	Parent() VariableHolder
	Variables() map[string]interface{}
}

type MessageSubscription interface {
	ElementId() string
	ElementInstanceKey() int64
	ProcessKey() int64
	ProcessInstanceKey() int64
	Name() string
	MessageState() string // see bpmn.ActivityState
	CreatedAt() time.Time
	//originActivity() activity
	//baseElement() bpmn20.FlowNode
}

type Activity interface {
}

type Timer interface {
	ElementId() string
	ElementInstanceKey() int64
	ProcessKey() int64
	ProcessInstanceKey() int64
	TimerState() string // see bpmn.TimerState
	CreatedAt() time.Time
	DueAt() time.Time
	Duration() time.Duration
	//originActivity     activity
	//baseElement        bpmn20.FlowNode
}

type Job interface {
	ElementId() string
	ElementInstanceKey() int64
	ProcessInstanceKey() int64
	JobKey() int64
	JobState() string // see bpmn.ActivityState
	CreatedAt() time.Time
	//baseElement        bpmn20.FlowNode
}

type TimeState interface {
}

type JobState interface {
}
