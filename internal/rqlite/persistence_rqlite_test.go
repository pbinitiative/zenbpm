package rqlite

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/pbinitiative/zenbpm/internal/rqlite/sql"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
)

var rqlitePersistence PersistenceRqlite
var gen *snowflake.Node

func TestMain(m *testing.M) {
	// setup
	testStore := TestStorage{}
	testStore.SetupTestEnvironment(m)

	if testStore.rqlitePersistence == nil {
		os.Exit(1)
	}
	rqlitePersistence = ptr.Deref(testStore.rqlitePersistence, PersistenceRqlite{})
	gen = testStore.gen

	// run
	exitCode := m.Run()

	// teardown
	testStore.TeardownTestEnvironment(m)
	os.Exit(exitCode)
}

func Test_RqlitePersistence_ConnectionWorks(t *testing.T) {
	// when
	_, err := rqlitePersistence.ExecContext(context.Background(), "SELECT 1")

	//then
	if err != nil {
		t.Fatal(err)
	}
}

func Test_ParseSimleResult_works(t *testing.T) {
	// when
	rows, err := rqlitePersistence.QueryContext(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}

	// then
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

func Test_ProcessDefinitionWrite_works(t *testing.T) {
	// given
	ProcessDefinition := sql.ProcessDefinition{
		Key:              1,
		Version:          1,
		BpmnProcessID:    "test-definition",
		BpmnData:         "",
		BpmnChecksum:     []byte{12, 32},
		BpmnResourceName: "",
	}

	// when
	err := rqlitePersistence.SaveNewProcess(t.Context(), ProcessDefinition)

	if err != nil {
		t.Fatalf("Failed inserting the record: %s", err)
	}

	processDefinitions, err := rqlitePersistence.FindProcesses(t.Context(), nil, ptr.To(int64(1)))

	if err != nil {
		t.Fatalf("Failed finding the record: %s", err)
	}

	// then
	if len(processDefinitions) != 1 {
		t.Errorf("Wrong number of process definitions: %d", len(processDefinitions))
	}

	for _, pd := range processDefinitions {
		if !reflect.DeepEqual(pd, ProcessDefinition) {
			t.Errorf("Wrong process definition data got: %+v expected: %+v", pd, ProcessDefinition)
		}
	}

}

func setupProcessDefinition(t *testing.T) {
	processes, err := rqlitePersistence.FindProcesses(t.Context(), nil, ptr.To(int64(1)))
	if err != nil {
		t.Fatalf("Failed finding the record: %s", err)
	}

	if len(processes) == 0 {
		Test_ProcessDefinitionWrite_works(t)
		if err != nil {
			t.Fatalf("Failed finding the record: %s", err)
		}
	}
}

func setupProcessInstance(t *testing.T) {
	Test_ProcessInstanceWrite_works(t)
}

func Test_ProcessInstanceWrite_works(t *testing.T) {
	// setup
	setupProcessDefinition(t)

	// given
	processInstance := sql.ProcessInstance{
		Key:                  1,
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

	processInstances, err := rqlitePersistence.FindProcessInstances(t.Context(), ptr.To(int64(1)), nil)

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

func Test_ProcessInstanceUpdate_works(t *testing.T) {
	// setup
	setupProcessDefinition(t)

	// given
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

	// when
	err = rqlitePersistence.SaveProcessInstance(t.Context(), processInstance)

	processInstances, err := rqlitePersistence.FindProcessInstances(t.Context(), ptr.To(int64(1)), nil)

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

func Test_JobFindByKey_failsProperlyWhenKeyNotFound(t *testing.T) {
	// when
	_, err := rqlitePersistence.queries.FindJobByKey(t.Context(), 1)

	// then
	if err == nil {
		t.Errorf("Expected error when key not found")
	} else if err.Error() != sql.ErrNoRows {
		t.Errorf("Unexpected error: %s", err)
	}

}

func Test_JobWrite_works(t *testing.T) { // We need the process definition in db
	//setup
	setupProcessInstance(t)

	// given
	job := sql.Job{
		Key:                1,
		ElementID:          "id",
		ElementInstanceKey: 1,
		ProcessInstanceKey: 1,
		State:              1,
		CreatedAt:          1,
	}

	// when
	err := rqlitePersistence.SaveJob(t.Context(), job)

	if err != nil {
		t.Fatalf("Failed inserting the record: %s", err)
	}

	jobs, err := rqlitePersistence.FindJobs(t.Context(), nil, nil, ptr.To(int64(1)), nil)

	if err != nil {
		t.Fatalf("Failed finding the record: %s", err)
	}

	// then
	if len(jobs) != 1 {
		t.Errorf("Wrong number of jobs: %d", len(jobs))
	}

	for _, j := range jobs {
		if j != job {
			t.Errorf("Wrong job data got: %+v expected: %+v", j, job)
		}
	}
}

func setupJob(t *testing.T) {
	Test_JobWrite_works(t)
}

func Test_JobStateFilter_works(t *testing.T) {
	//setup
	setupJob(t)
	// when
	jobs, err := rqlitePersistence.FindJobs(t.Context(), nil, nil, ptr.To(int64(1)), []string{"ACTIVE", "COMPLETED"})

	if err != nil {
		t.Fatalf("Failed finding the record: %s", err)
	}

	// then
	if len(jobs) != 1 {
		t.Errorf("Wrong number of jobs: %d", len(jobs))
	}
}

func Test_TimerWrite_works(t *testing.T) {
	// setup
	setupProcessInstance(t)
	// given
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
	// when
	err := rqlitePersistence.SaveTimer(t.Context(), timer)

	if err != nil {
		t.Fatalf("Failed inserting the record: %s", err)
	}

	timers, err := rqlitePersistence.FindTimers(t.Context(), nil, ptr.To(int64(1)), nil)

	if err != nil {
		t.Fatalf("Failed finding the record: %s", err)
	}

	// then
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
	// setup
	setupProcessInstance(t)
	// given
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

	// when
	err := rqlitePersistence.SaveMessageSubscription(t.Context(), messageSubscription)

	if err != nil {
		t.Fatalf("Failed inserting the record: %s", err)
	}

	messageSubscriptions, err := rqlitePersistence.FindMessageSubscriptions(t.Context(), ptr.To(int64(1)), ptr.To(int64(1)), nil, nil)

	if err != nil {
		t.Fatalf("Failed finding the record: %s", err)
	}

	// then
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
	// setup
	setupProcessInstance(t)

	// given
	activityInstance := sql.ActivityInstance{
		Key:                  1,
		ElementID:            "id",
		ProcessInstanceKey:   1,
		ProcessDefinitionKey: 1,
		CreatedAt:            time.Now().Unix(),
		State:                "ACTIVATED",
		BpmnElementType:      "Task",
	}

	// when
	err := rqlitePersistence.SaveActivity(t.Context(), activityInstance)

	if err != nil {
		t.Fatalf("Failed inserting the record: %s", err)
	}

	activityInstances, err := rqlitePersistence.FindActivitiesByProcessInstanceKey(t.Context(), ptr.To(int64(1)))

	if err != nil {
		t.Fatalf("Failed finding the record: %s", err)
	}

	// then
	if len(activityInstances) != 1 {
		t.Errorf("Wrong number of activity instances: %d", len(activityInstances))
	}

	for _, ai := range activityInstances {
		if ai != activityInstance {
			t.Errorf("Wrong activity instance data got: %+v expected: %+v", ai, activityInstance)
		}
	}
}
