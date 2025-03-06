package rqlite

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/internal/log"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/persistence/rqlite/sql"
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

	processInstance := sql.ProcessInstance{
		Key:                  1,
		ProcessDefinitionKey: 1,
		CreatedAt:            time.Now().Unix(),
		State:                1,
		VariableHolder:       "",
		CaughtEvents:         "",
		Activities:           "",
	}
	err := rqlitePersistence.SaveProcessInstance(t.Context(), processInstance)

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
		if pi != processInstance {
			t.Errorf("Wrong process instance data got: %+v expected: %+v", pi, processInstance)
		}
	}
}

func Test_ProcessInstanceUpdate_works(t *testing.T) {

	processInstance := sql.ProcessInstance{
		Key:                  1,
		ProcessDefinitionKey: 1,
		CreatedAt:            time.Now().Unix(),
		State:                1,
		VariableHolder:       "",
		CaughtEvents:         "",
		Activities:           "",
	}
	err := rqlitePersistence.SaveProcessInstance(t.Context(), processInstance)

	if err != nil {
		t.Fatalf("Failed inserting the record: %s", err)
	}

	processInstance.State = 4
	processInstance.Activities = "[]"
	processInstance.VariableHolder = "[]"
	processInstance.CaughtEvents = "[]"

	err = rqlitePersistence.SaveProcessInstance(t.Context(), processInstance)

	processInstances, err := rqlitePersistence.FindProcessInstances(t.Context(), 1, -1)

	if err != nil {
		t.Fatalf("Failed finding the record: %s", err)
	}

	// Assert

	if len(processInstances) != 1 {
		t.Errorf("Wrong number of process instances: %d", len(processInstances))
	}

	for _, pi := range processInstances {
		if pi != processInstance {
			t.Errorf("Wrong process instance data got: %+v expected: %+v", pi, processInstance)
		}
	}
}

func Test_ProcessDefinitionWrite_works(t *testing.T) {
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

	processDefinitions, err := rqlitePersistence.FindProcesses(t.Context(), "1", -1)

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

func Test_JobFindByKey_failsProperlyWhenKeyNotFound(t *testing.T) {
	_, err := rqlitePersistence.queries.FindJobByKey(t.Context(), 1)

	if err == nil {
		t.Errorf("Expected error when key not found")
	} else if err.Error() != sql.ErrNoRows {
		t.Errorf("Unexpected error: %s", err)
	}

}

func Test_JobWrite_works(t *testing.T) {
	job := sql.Job{
		Key:                1,
		ElementID:          "id",
		ElementInstanceKey: 1,
		ProcessInstanceKey: 1,
		State:              1,
		CreatedAt:          1,
	}

	err := rqlitePersistence.SaveJob(t.Context(), job)

	if err != nil {
		t.Fatalf("Failed inserting the record: %s", err)
	}

	jobs, err := rqlitePersistence.FindJobs(t.Context(), "", -1, 1, nil)

	if err != nil {
		t.Fatalf("Failed finding the record: %s", err)
	}

	// Assert
	if len(jobs) != 1 {
		t.Errorf("Wrong number of jobs: %d", len(jobs))
	}

	for _, j := range jobs {
		if j != job {
			t.Errorf("Wrong job data got: %+v expected: %+v", j, job)
		}
	}
}

func Test_JobStateFilter_works(t *testing.T) {
	jobs, err := rqlitePersistence.FindJobs(t.Context(), "", -1, 1, []string{"ACTIVE", "COMPLETED"})

	if err != nil {
		t.Fatalf("Failed finding the record: %s", err)
	}

	// Assert
	if len(jobs) != 1 {
		t.Errorf("Wrong number of jobs: %d", len(jobs))
	}
}

func Test_TimerWrite_works(t *testing.T) {
	timer := sql.Timer{
		Key:                  1,
		ElementID:            "id",
		ElementInstanceKey:   1,
		ProcessInstanceKey:   1,
		State:                1,
		CreatedAt:            1,
		ProcessDefinitionKey: 1,
		DueAt:                1,
		Duration:             1,
	}

	err := rqlitePersistence.SaveTimer(t.Context(), timer)

	if err != nil {
		t.Fatalf("Failed inserting the record: %s", err)
	}

	timers, err := rqlitePersistence.FindTimers(t.Context(), -1, 1, nil)

	if err != nil {
		t.Fatalf("Failed finding the record: %s", err)
	}

	// Assert
	if len(timers) != 1 {
		t.Errorf("Wrong number of timers: %d", len(timers))
	}

	for _, timer := range timers {
		if timer != timer {
			t.Errorf("Wrong timer data got: %+v expected: %+v", timer, timer)
		}
	}
}

func Test_MessageSubscriptionWrite_works(t *testing.T) {
	messageSubscription := sql.MessageSubscription{
		Key:                  1,
		ElementID:            "id",
		ElementInstanceKey:   1,
		ProcessInstanceKey:   1,
		ProcessDefinitionKey: 1,
		OriginActivityKey:    1,
		Name:                 "name",
		State:                1,
		CreatedAt:            time.Now().Unix(),
		OriginActivityState:  1,
		OriginActivityID:     "id",
	}

	err := rqlitePersistence.SaveMessageSubscription(t.Context(), messageSubscription)

	if err != nil {
		t.Fatalf("Failed inserting the record: %s", err)
	}

	messageSubscriptions, err := rqlitePersistence.FindMessageSubscriptions(t.Context(), 1, 1, "", nil)

	if err != nil {
		t.Fatalf("Failed finding the record: %s", err)
	}

	// Assert
	if len(messageSubscriptions) != 1 {
		t.Errorf("Wrong number of message subscriptions: %d", len(messageSubscriptions))
	}

	for _, ms := range messageSubscriptions {
		if ms != messageSubscription {
			t.Errorf("Wrong message subscription data got: %+v expected: %+v", ms, messageSubscription)
		}
	}

}

func Test_ActivityInstanceWrite_works(t *testing.T) {
	activityInstance := sql.ActivityInstance{
		Key:                  1,
		ElementID:            "id",
		ProcessInstanceKey:   1,
		ProcessDefinitionKey: 1,
		CreatedAt:            time.Now().Unix(),
		State:                "ACTIVATED",
		BpmnElementType:      "Task",
	}

	err := rqlitePersistence.SaveActivity(t.Context(), activityInstance)

	if err != nil {
		t.Fatalf("Failed inserting the record: %s", err)
	}

	activityInstances, err := rqlitePersistence.FindActivitiesByProcessInstanceKey(t.Context(), 1)

	if err != nil {
		t.Fatalf("Failed finding the record: %s", err)
	}

	// Assert
	if len(activityInstances) != 1 {
		t.Errorf("Wrong number of activity instances: %d", len(activityInstances))
	}

	for _, ai := range activityInstances {
		if ai != activityInstance {
			t.Errorf("Wrong activity instance data got: %+v expected: %+v", ai, activityInstance)
		}
	}
}
