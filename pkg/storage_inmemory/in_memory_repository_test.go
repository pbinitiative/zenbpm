package storage_inmemory

import (
	"context"
	"github.com/corbym/gocrest/has"
	"github.com/corbym/gocrest/is"
	"github.com/corbym/gocrest/then"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/var_holder"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"testing"
	"time"
)

func Test_InMemoryStorage_implements_PersistentStorageApi_interface(t *testing.T) {
	var _ storage.PersistentStorageApi = &InMemoryStorage{}
}

func TestInMemoryStorage_SaveProcessDefinition_and_FindById(t *testing.T) {
	ctx := context.TODO()

	def := storage.ProcessDefinition{
		BpmnProcessId:    "id-1",
		Version:          1,
		ProcessKey:       1234567890,
		BpmnData:         "data",
		BpmnChecksum:     "crc1234",
		BpmnResourceName: "aResource",
	}

	inMemory := InMemoryStorage{}

	for i := 0; i < 3; i++ {
		// hint: we can repeat SaveProcessDefinition and FindProcessDefinitionsById and will always get same results.
		err := inMemory.SaveProcessDefinition(ctx, &def)
		then.AssertThat(t, err, is.Nil())

		definitions, err := inMemory.FindProcessDefinitionsById(ctx, "id-1")
		then.AssertThat(t, err, is.Nil())
		then.AssertThat(t, definitions, has.Length(1))
	}
}

func TestInMemoryStorage_SaveProcessInstance_and_FindByKey(t *testing.T) {
	ctx := context.TODO()

	instance := storage.ProcessInstance{
		ProcessDefinition: nil,
		InstanceKey:       123456,
		VariableHolder:    var_holder.VariableHolder{},
		CreatedAt:         time.Time{},
		State:             bpmn20.Completed,
	}

	inMemory := InMemoryStorage{}

	for i := 0; i < 3; i++ {
		// hint: we can repeat SaveProcessInstance and FindProcessInstancesByKey and will always get same results.
		err := inMemory.SaveProcessInstance(ctx, &instance)
		then.AssertThat(t, err, is.Nil())

		definitions, err := inMemory.FindProcessInstancesByKey(ctx, 123456)
		then.AssertThat(t, err, is.Nil())
		then.AssertThat(t, definitions, has.Length(1))
	}
}

func Test_contains(t *testing.T) {
	containsB := contains([]string{"a", "b", "c"}, "b")
	then.AssertThat(t, containsB, is.True())

	containsX := contains([]string{"a", "b", "c"}, "x")
	then.AssertThat(t, containsX, is.False())
}
