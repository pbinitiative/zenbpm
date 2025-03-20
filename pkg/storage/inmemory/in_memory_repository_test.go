package inmemory

import (
	"context"
	"github.com/corbym/gocrest/has"
	"github.com/corbym/gocrest/is"
	"github.com/corbym/gocrest/then"
	"github.com/pbinitiative/zenbpm/pkg/bpmn"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"testing"
	"time"
)

func Test_InMemoryStorage_implements_PersistentStorageApi_interface(t *testing.T) {
	var _ storage.PersistentStorageApi = &InMemoryStorage{}
}

func TestInMemoryStorage_SaveProcessDefinition_and_FindById(t *testing.T) {
	ctx := context.TODO()

	def := processDefinition{
		bpmnProcessId:    "id-1",
		version:          1,
		processKey:       1234567890,
		bpmnData:         "data",
		bpmnChecksum:     [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6},
		bpmnResourceName: "aResource",
	}

	inMemory := New()

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

	instance := processInstance{
		processDefinition: nil,
		instanceKey:       123456,
		variableHolder:    &variableHolder{},
		createdAt:         time.Time{},
		state:             string(bpmn.Completed), // TODO: fix me, should not type convert here
	}

	inMemory := New()

	for i := 0; i < 3; i++ {
		// hint: we can repeat SaveProcessInstance and FindProcessInstancesByKey and will always get same results.
		err := inMemory.SaveProcessInstance(ctx, &instance)
		then.AssertThat(t, err, is.Nil())

		definitions, err := inMemory.FindProcessInstancesByKey(ctx, 123456)
		then.AssertThat(t, err, is.Nil())
		then.AssertThat(t, definitions, has.Length(1))
	}
}
