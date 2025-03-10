package storage_inmemory

import (
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"time"
)

type processInstance struct {
	processDefinition *processDefinition
	instanceKey       int64
	variableHolder    *variableHolder
	createdAt         time.Time
	state             bpmn20.ActivityState
	//CaughtEvents   []catchEvent
	//activities     []activity
}

type variableHolder struct {
	parent    *variableHolder
	variables map[string]interface{}
}

func (p *processInstance) ProcessDefinition() storage.ProcessDefinition {
	return p.processDefinition
}

func (p *processInstance) InstanceKey() int64 {
	return p.instanceKey
}

func (p *processInstance) VariableHolder() storage.VariableHolder {
	return p.variableHolder
}

func (p *processInstance) CreatedAt() time.Time {
	return p.createdAt
}

func (p *processInstance) State() bpmn20.ActivityState {
	return p.state
}

func (v *variableHolder) Parent() storage.VariableHolder {
	return v.parent
}

func (v *variableHolder) Variables() map[string]interface{} {
	return v.variables
}
