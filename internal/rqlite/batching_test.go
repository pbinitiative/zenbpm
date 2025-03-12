package rqlite

import (
	"context"
	"reflect"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/appcontext"
	"github.com/pbinitiative/zenbpm/internal/rqlite/sql"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
)

func Test_NormalExecution_shouldExecuteRightAway(t *testing.T) {
	// given

	ProcessDefinition := sql.ProcessDefinition{
		Key:              1,
		Version:          1,
		BpmnProcessID:    "1",
		BpmnData:         "",
		BpmnChecksum:     []byte{12, 32},
		BpmnResourceName: "",
	}

	err := rqlitePersistence.SaveNewProcess(t.Context(), ProcessDefinition)

	if err != nil {
		t.Fatalf("Failed inserting the record: %s", err)
	}

	processDefinitions, err := rqlitePersistence.FindProcesses(t.Context(), ptr.To("1"), nil)

	if err != nil {
		t.Fatalf("Failed finding the record: %s", err)
	}

	// Assert
	if len(processDefinitions) != 1 {
		t.Errorf("Wrong number of process definitions: %d", len(processDefinitions))
	}

	for _, pd := range processDefinitions {
		if !reflect.DeepEqual(pd, ProcessDefinition) {
			t.Errorf("Wrong process definition data got: %+v expected: %+v", pd, ProcessDefinition)
		}
	}
}

func Test_BatchExecution_shouldExecuteAfterFlush(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), appcontext.ExecutionKey, gen.Generate().Int64())
	key := gen.Generate().Int64()

	ProcessDefinition := sql.ProcessDefinition{
		Key:              key,
		Version:          1,
		BpmnProcessID:    "1",
		BpmnData:         "",
		BpmnChecksum:     []byte{12, 32},
		BpmnResourceName: "",
	}

	err := rqlitePersistence.SaveNewProcess(ctx, ProcessDefinition)

	if err != nil {
		t.Fatalf("Failed inserting the record: %s", err)
	}

	processDefinitions, err := rqlitePersistence.FindProcesses(ctx, nil, &key)

	if err != nil {
		t.Fatalf("Failed finding the record: %s", err)
	}

	// Assert
	if len(processDefinitions) != 0 {
		t.Errorf("Wrong number of process definitions: %d", len(processDefinitions))
	}

	for _, pd := range processDefinitions {
		if !reflect.DeepEqual(pd, ProcessDefinition) {
			t.Errorf("Wrong process definition data got: %+v expected: %+v", pd, ProcessDefinition)
		}
	}

	// when
	err = rqlitePersistence.FlushTransaction(ctx)

	if err != nil {
		t.Fatalf("Failed flushing the transaction: %s", err)
	}

	// then
	processDefinitions, err = rqlitePersistence.FindProcesses(ctx, nil, &key)

	if err != nil {
		t.Fatalf("Failed finding the record: %s", err)
	}

	if len(processDefinitions) != 1 {
		t.Errorf("Wrong number of process instances: %d", len(processDefinitions))
	}

}
