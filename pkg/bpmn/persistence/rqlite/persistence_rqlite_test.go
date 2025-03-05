package rqlite

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/internal/log"
)

var rqlitePersistence *BpmnEnginePersistenceRqlite

func TestMain(m *testing.M) {
	// Setup
	appContext, _ := context.WithCancel(context.Background())

	conf := config.InitConfig()

	zenNode, err := cluster.StartZenNode(appContext, conf)
	if err != nil {
		log.Error("Failed to start Zen node: %s", err)
		os.Exit(1)
	}
	// give time to start
	time.Sleep(time.Second * 2)

	rqlitePersistence = NewBpmnEnginePersistenceRqlite(
		zenNode,
	)
	if err != nil {
		log.Errorf(appContext, "Error while initing persistence: %s", err.Error())
	}

	// Run tests
	exitCode := m.Run()

	// Cleanup: Perform any additional cleanup if needed

	// Cleanup: Remove the partition-1 folder
	if err := os.RemoveAll("./partition-1"); err != nil {
		log.Errorf(context.Background(), "Error while removing partition-1 folder: %s", err.Error())
	}

	// Exit with the appropriate code
	os.Exit(exitCode)
}

func Test_RqlitePersistence_ConnectionWorks(t *testing.T) {
	_, err := rqlitePersistence.ExecContext(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
}

func Test_ParseSimleResult_works(t *testing.T) {
	rows, err := rqlitePersistence.QueryContext(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}

	if rows.Next() {
		i := 0
		err := rows.Scan(&i)
		if err != nil {
			t.Fatal(err)
		}
		if i != 1 {
			t.Errorf("Wrong result: %d", i)
		}
	} else {
		t.Fatal("No result found")
	}
}

func Test_ProcessInstanceWrite_works(t *testing.T) {

	processInstance := ProcessInstance{
		Key:                  1,
		ProcessDefinitionKey: 1,
		CreatedAt:            time.Now().Unix(),
		State:                1,
		VariableHolder:       "",
		CaughtEvents:         "",
		Activities:           "",
	}
	err := rqlitePersistence.SaveProcessInstance(t.Context(), &processInstance)

	if err != nil {
		t.Fatalf("Failed inserting the record: %s", err)
	}

	processInstances, err := rqlitePersistence.FindProcessInstances(t.Context(), 1, -1)

	if err != nil {
		t.Fatalf("Failed finding the record: %s", err)
	}

	// Assert

	if len(processInstances) != 1 {
		t.Errorf("Wrong number of process instances: %d", len(processInstances))
	}

	for _, pi := range processInstances {
		if pi.Key != 1 {
			t.Errorf("Wrong key: %d", pi.Key)
		}
	}
}

func Test_ProcessInstanceUpdate_works(t *testing.T) {

	processInstance := ProcessInstance{
		Key:                  1,
		ProcessDefinitionKey: 1,
		CreatedAt:            time.Now().Unix(),
		State:                1,
		VariableHolder:       "",
		CaughtEvents:         "",
		Activities:           "",
	}
	err := rqlitePersistence.SaveProcessInstance(t.Context(), &processInstance)

	if err != nil {
		t.Fatalf("Failed inserting the record: %s", err)
	}

	processInstance.State = 4
	processInstance.Activities = "[]"
	processInstance.VariableHolder = "[]"
	processInstance.CaughtEvents = "[]"

	err = rqlitePersistence.SaveProcessInstance(t.Context(), &processInstance)

	processInstances, err := rqlitePersistence.FindProcessInstances(t.Context(), 1, -1)

	if err != nil {
		t.Fatalf("Failed finding the record: %s", err)
	}

	// Assert

	if len(processInstances) != 1 {
		t.Errorf("Wrong number of process instances: %d", len(processInstances))
	}

	for _, pi := range processInstances {
		if pi.Key != 1 {
			t.Errorf("Wrong key: %d", pi.Key)
		}
		if pi.State != 4 {
			t.Errorf("Wrong state: %d", pi.State)
		}
		if pi.Activities != "[]" {
			t.Errorf("Wrong activities: %s", pi.Activities)
		}
		if pi.VariableHolder != "[]" {
			t.Errorf("Wrong variable holder: %s", pi.VariableHolder)
		}
		if pi.CaughtEvents != "[]" {
			t.Errorf("Wrong caught events: %s", pi.CaughtEvents)
		}
	}
}

func Test_ProcessDefinitionWrite_works(t *testing.T) {
	ProcessDefinition := ProcessDefinition{
		Key:              1,
		Version:          1,
		BpmnProcessID:    "1",
		BpmnData:         "",
		BpmnChecksum:     []byte{},
		BpmnResourceName: "",
	}

	err := rqlitePersistence.SaveNewProcess(t.Context(), &ProcessDefinition)

	if err != nil {
		t.Fatalf("Failed inserting the record: %s", err)
	}

	processDefinitions, err := rqlitePersistence.FindProcesses(t.Context(), "1", -1)

	if err != nil {
		t.Fatalf("Failed finding the record: %s", err)
	}

	// Assert
	if len(processDefinitions) != 1 {
		t.Errorf("Wrong number of process definitions: %d", len(processDefinitions))
	}

	for _, pd := range processDefinitions {
		if pd.Key != 1 {
			t.Errorf("Wrong key: %d", pd.Key)
		}
	}

}
