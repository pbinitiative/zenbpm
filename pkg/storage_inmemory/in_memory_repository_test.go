package storage_inmemory

import (
	"context"
	"github.com/corbym/gocrest/has"
	"github.com/corbym/gocrest/is"
	"github.com/corbym/gocrest/then"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"testing"
)

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
		err := inMemory.SaveProcessDefinition(ctx, def)
		then.AssertThat(t, err, is.Nil())

		definitions, err := inMemory.FindProcessDefinitionsById(ctx, "id-1")
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
