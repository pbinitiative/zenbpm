package inmemory

import (
	"context"
	"github.com/corbym/gocrest/has"
	"github.com/corbym/gocrest/is"
	"github.com/corbym/gocrest/then"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"testing"
	"time"
)

func Test_InMemoryStorage_implements_PersistentStorageApi_interface(t *testing.T) {
	var _ storage.PersistentStorageNew = &InMemoryStorage{}
}

func TestInMemoryStorage_SaveProcessDefinition_and_FindById(t *testing.T) {
	ctx := context.TODO()

	def := runtime.ProcessDefinition{
		BpmnProcessId:    "id-1",
		Version:          1,
		ProcessKey:       1234567890,
		BpmnData:         "data",
		BpmnChecksum:     [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6},
		BpmnResourceName: "aResource",
	}

	inMemory := NewInMemory()

	for i := 0; i < 3; i++ {
		// hint: we can repeat SaveProcessDefinition and FindProcessDefinitionsById and will always get same results.
		err := inMemory.SaveProcessDefinition(ctx, def)
		then.AssertThat(t, err, is.Nil())

		definitions, err := inMemory.FindProcessDefinitionsById(ctx, "id-1")
		then.AssertThat(t, err, is.Nil())
		then.AssertThat(t, definitions, has.Length(1))
	}
}

func TestInMemoryStorage_SaveProcessInstance_and_FindByKey(t *testing.T) {
	ctx := context.TODO()

	instance := testProcessInstance{
		processDefinition: nil,
		instanceKey:       123456,
		variableHolder:    runtime.VariableHolder{},
		createdAt:         time.Time{},
		state:             runtime.Completed,
	}

	inMemory := NewInMemory()

	for i := 0; i < 3; i++ {
		// hint: we can repeat SaveProcessInstance and FindProcessInstancesByKey and will always get same results.
		err := inMemory.SaveProcessInstance(ctx, &instance)
		then.AssertThat(t, err, is.Nil())

		definitions, err := inMemory.FindProcessInstancesByKey(ctx, 123456)
		then.AssertThat(t, err, is.Nil())
		then.AssertThat(t, definitions, has.Length(1))
	}
}

type testProcessInstance struct {
	processDefinition *runtime.ProcessDefinition
	instanceKey       int64
	variableHolder    runtime.VariableHolder
	createdAt         time.Time
	state             runtime.ActivityState
	//CaughtEvents   []catchEvent               `
	//activities     []runtime.Activity
}

func (t testProcessInstance) GetProcessInfo() *runtime.ProcessDefinition {
	return t.processDefinition
}

func (t testProcessInstance) GetInstanceKey() int64 {
	return t.instanceKey
}

func (t testProcessInstance) GetVariable(key string) interface{} {
	//TODO implement me
	panic("implement me")
}

func (t testProcessInstance) SetVariable(key string, value interface{}) {
	//TODO implement me
	panic("implement me")
}

func (t testProcessInstance) GetCreatedAt() time.Time {
	return t.createdAt
}

func (t testProcessInstance) GetState() runtime.ActivityState {
	return t.state
}
