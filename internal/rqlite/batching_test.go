package rqlite

import (
	"context"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rqlite/sql"
)

func Test_NormalExecution_shouldExecuteRightAway(t *testing.T) {
	// given

	processInstanceKey := gen.Generate().Int64()
	processInstance := sql.ProcessInstance{
		Key:                  processInstanceKey,
		ProcessDefinitionKey: 1,
		CreatedAt:            time.Now().Unix(),
		State:                1,
		VariableHolder:       "",
		CaughtEvents:         "",
		Activities:           "",
	}

	// when
	err := rqlitePersistence.SaveProcessInstance(t.Context(), processInstance)

	if err != nil {
		t.Fatalf("Failed inserting the record: %s", err)
	}

	processInstances, err := rqlitePersistence.FindProcessInstances(t.Context(), &processInstanceKey, nil)

	if err != nil {
		t.Fatalf("Failed finding the record: %s", err)
	}

	// then

	if len(processInstances) != 1 {
		t.Errorf("Wrong number of process instances: %d", len(processInstances))
	}

	for _, pi := range processInstances {
		if pi != processInstance {
			t.Errorf("Wrong process instance data got: %+v expected: %+v", pi, processInstance)
		}
	}
}

func Test_BatchExecution_shouldExecuteAfterFlush(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "executionKey", gen.Generate().Int64())
	processInstanceKey := gen.Generate().Int64()
	processInstance := sql.ProcessInstance{
		Key:                  processInstanceKey,
		ProcessDefinitionKey: 1,
		CreatedAt:            time.Now().Unix(),
		State:                1,
		VariableHolder:       "",
		CaughtEvents:         "",
		Activities:           "",
	}
	// when
	err := rqlitePersistence.SaveProcessInstance(ctx, processInstance)

	if err != nil {
		t.Fatalf("Failed inserting the record: %s", err)
	}

	processInstances, err := rqlitePersistence.FindProcessInstances(t.Context(), &processInstanceKey, nil)

	if err != nil {
		t.Fatalf("Failed finding the record: %s", err)
	}

	// then

	if len(processInstances) != 0 {
		t.Errorf("Wrong number of process instances: %d", len(processInstances))
	}

	for _, pi := range processInstances {
		if pi != processInstance {
			t.Errorf("Wrong process instance data got: %+v expected: %+v", pi, processInstance)
		}
	}

	// when
	err = rqlitePersistence.FlushTransaction(ctx)

	if err != nil {
		t.Fatalf("Failed flushing the transaction: %s", err)
	}

	// then
	processInstances, err = rqlitePersistence.FindProcessInstances(t.Context(), &processInstanceKey, nil)

	if err != nil {
		t.Fatalf("Failed finding the record: %s", err)
	}

	if len(processInstances) != 1 {
		t.Errorf("Wrong number of process instances: %d", len(processInstances))
	}

}
