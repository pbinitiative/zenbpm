package storagetest

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	stdruntime "runtime"

	"slices"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/stretchr/testify/assert"
)

type StorageTestFunc func(s storage.Storage, t *testing.T) func(t *testing.T)

type StorageTester struct {
	processDefinition runtime.ProcessDefinition
	processInstance   runtime.ProcessInstance
}

func (st *StorageTester) GetTests() map[string]StorageTestFunc {
	tests := map[string]StorageTestFunc{}

	// all test functions need to be registered here
	functions := []StorageTestFunc{
		st.TestProcessDefinitionStorageWriter,
		st.TestProcessDefinitionStorageReader,
		st.TestProcessInstanceStorageWriter,
		st.TestProcessInstanceStorageReader,
		st.TestTimerStorageWriter,
		st.TestTimerStorageReader,
		st.TestJobStorageWriter,
		st.TestJobStorageReader,
		st.TestMessageStorageReader,
		st.TestMessageStorageWriter,
	}

	for _, function := range functions {
		funcName := getFunctionName(function)
		stripedName := funcName[strings.LastIndex(funcName, ".")+1:]
		tests[stripedName] = function
	}
	return tests
}

func getFunctionName(i any) string {
	return stdruntime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

func getProcessDefinition(r int64) runtime.ProcessDefinition {
	return runtime.ProcessDefinition{
		BpmnProcessId:    fmt.Sprintf("id-%d", r),
		Version:          1,
		Key:              r,
		BpmnData:         fmt.Sprintf("data-%d", r),
		BpmnChecksum:     [16]byte{1},
		BpmnResourceName: fmt.Sprintf("resource-%d", r),
	}
}

// prepareTestData will prepare common data for the tests
func (st *StorageTester) PrepareTestData(s storage.Storage, t *testing.T) {
	ctx := context.Background()
	r := rand.Int63()

	st.processDefinition = getProcessDefinition(r)
	err := s.SaveProcessDefinition(ctx, st.processDefinition)
	assert.Nil(t, err)

	st.processInstance = getProcessInstance(r, st.processDefinition)
	err = s.SaveProcessInstance(ctx, st.processInstance)
	assert.Nil(t, err)
}

func (st *StorageTester) TestProcessDefinitionStorageWriter(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.TODO()

		r := rand.Int63()

		def := getProcessDefinition(r)

		err := s.SaveProcessDefinition(ctx, def)
		assert.Nil(t, err)

		definition, err := s.FindProcessDefinitionByKey(ctx, r)
		assert.Nil(t, err)
		assert.Equal(t, r, definition.Key)
	}
}

func (st *StorageTester) TestProcessDefinitionStorageReader(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.TODO()

		r := rand.Int63()

		def := getProcessDefinition(r)

		err := s.SaveProcessDefinition(ctx, def)
		assert.Nil(t, err)

		definition, err := s.FindLatestProcessDefinitionById(ctx, def.BpmnProcessId)
		assert.Nil(t, err)
		assert.Equal(t, r, definition.Key)

		definition, err = s.FindProcessDefinitionByKey(ctx, def.Key)
		assert.Nil(t, err)
		assert.Equal(t, r, definition.Key)

		definitions, err := s.FindProcessDefinitionsById(ctx, def.BpmnProcessId)
		assert.Nil(t, err)
		assert.Len(t, definitions, 1)
		assert.Equal(t, definitions[0].Key, definition.Key)
	}
}

func getProcessInstance(r int64, d runtime.ProcessDefinition, activities ...runtime.Activity) runtime.ProcessInstance {
	return runtime.ProcessInstance{
		Definition: &d,
		Key:        r,
		VariableHolder: runtime.NewVariableHolder(nil, map[string]interface{}{
			"v1":   float64(123),
			"var2": "val2",
		}),
		CreatedAt: time.Now().Truncate(time.Millisecond),
		State:     runtime.ActivityStateActive,
		CaughtEvents: []runtime.CatchEvent{
			{
				Name:       "TestEvent",
				CaughtAt:   time.Now().Truncate(time.Millisecond),
				IsConsumed: false,
				Variables: map[string]interface{}{
					"v1": float64(123),
					"v2": "v2",
				},
			},
		},
		Activities: activities,
	}
}

func getJob(key, piKey int64) runtime.Job {
	return runtime.Job{
		ElementId:          fmt.Sprintf("job-%d", key),
		ElementInstanceKey: key + 200,
		ProcessInstanceKey: piKey,
		Key:                key,
		State:              runtime.ActivityStateActive,
		CreatedAt:          time.Now().Truncate(time.Millisecond),
		// BaseElement:        ,
	}
}

func (st *StorageTester) TestProcessInstanceStorageWriter(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.TODO()

		r := rand.Int63()

		inst := getProcessInstance(r, st.processDefinition, getJob(r, st.processInstance.Key))

		err := s.SaveProcessInstance(ctx, inst)
		assert.Nil(t, err)
	}
}

func (st *StorageTester) TestProcessInstanceStorageReader(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.TODO()

		r := rand.Int63()

		inst := getProcessInstance(r, st.processDefinition, getJob(r, st.processInstance.Key))

		err := s.SaveProcessInstance(ctx, inst)
		assert.Nil(t, err)

		instance, err := s.FindProcessInstanceByKey(ctx, inst.Key)
		assert.Nil(t, err)
		assert.Equal(t, inst.Key, instance.Key)
		assert.Equal(t, inst.CreatedAt.Truncate(time.Millisecond), instance.CreatedAt.Truncate(time.Millisecond))
		assert.Equal(t, inst.VariableHolder, instance.VariableHolder)

		// TODO: uncomment once its implemented
		// assert.Equal(t, len(inst.Activities), len(instance.Activities))
		// assert.Equal(t, inst.Activities[0], instance.Activities[0])
		//
		// assert.Equal(t, len(inst.CaughtEvents), len(instance.CaughtEvents))
		// assert.Equal(t, inst.CaughtEvents[0], instance.CaughtEvents[0])
	}
}

func getTimer(key, pdKey, piKey int64, originActivity runtime.Activity) runtime.Timer {
	return runtime.Timer{
		ElementId:            fmt.Sprintf("timer-%d", key),
		Key:                  key,
		ProcessDefinitionKey: pdKey,
		ProcessInstanceKey:   piKey,
		TimerState:           runtime.TimerStateCreated,
		CreatedAt:            time.Now().Truncate(time.Millisecond),
		DueAt:                time.Now().Add(1 * time.Hour).Truncate(time.Millisecond),
		Duration:             1 * time.Hour,
		OriginActivity:       originActivity,
		// BaseElement:          nil,
	}
}

func (st *StorageTester) TestTimerStorageWriter(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.TODO()

		r := rand.Int63()

		job := getJob(r, st.processInstance.Key)
		err := s.SaveJob(ctx, job)
		assert.Nil(t, err)

		timer := getTimer(r, st.processDefinition.Key, st.processInstance.Key, job)

		err = s.SaveTimer(ctx, timer)
		assert.Nil(t, err)
	}
}

func (st *StorageTester) TestTimerStorageReader(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.TODO()

		r := rand.Int63()

		job := getJob(r, st.processInstance.Key)
		err := s.SaveJob(ctx, job)
		assert.Nil(t, err)

		timer := getTimer(r, st.processDefinition.Key, st.processInstance.Key, job)

		err = s.SaveTimer(ctx, timer)
		assert.Nil(t, err)

		timers, err := s.FindTimersByState(ctx, st.processInstance.Key, runtime.TimerStateCreated)
		assert.Nil(t, err)
		assert.Truef(t, slices.ContainsFunc(timers, timer.EqualTo), "expected to find timer in timers array: %+v", timers)

		timers, err = s.FindActivityTimers(ctx, job.Key, runtime.TimerStateCreated)
		assert.Nil(t, err)
		assert.Truef(t, slices.ContainsFunc(timers, timer.EqualTo), "expected to find timer in timers array: %+v", timers)
	}
}

func (st *StorageTester) TestJobStorageWriter(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.TODO()

		r := rand.Int63()

		job := getJob(r, st.processInstance.Key)

		err := s.SaveJob(ctx, job)
		assert.Nil(t, err)
	}
}

func (st *StorageTester) TestJobStorageReader(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.TODO()

		r := rand.Int63()

		job := getJob(r, st.processInstance.Key)
		err := s.SaveJob(ctx, job)
		assert.Nil(t, err)

		jobs, err := s.FindPendingProcessInstanceJobs(ctx, st.processInstance.Key)
		assert.Nil(t, err)
		assert.Contains(t, jobs, job)

		storeJob, err := s.FindJobByJobKey(ctx, job.Key)
		assert.Nil(t, err)
		assert.Equal(t, job, storeJob)
	}
}

func getMessage(r int64, piKey int64, pdKey int64, activity runtime.Activity) runtime.MessageSubscription {
	return runtime.MessageSubscription{
		ElementId:            fmt.Sprintf("message-%d", r),
		ElementInstanceKey:   r + 400,
		ProcessDefinitionKey: pdKey,
		ProcessInstanceKey:   piKey,
		Name:                 fmt.Sprintf("message-%d", r),
		MessageState:         runtime.ActivityStateActive,
		CreatedAt:            time.Now().Truncate(time.Millisecond),
		OriginActivity:       activity,
		BaseElement:          nil,
	}
}

func (st *StorageTester) TestMessageStorageWriter(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.TODO()

		r := rand.Int63()

		job := getJob(r, st.processInstance.Key)
		err := s.SaveJob(ctx, job)
		assert.Nil(t, err)

		message := getMessage(r, st.processDefinition.Key, st.processInstance.Key, job)

		err = s.SaveMessageSubscription(ctx, message)
		assert.Nil(t, err)
	}
}

func (st *StorageTester) TestMessageStorageReader(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.TODO()

		r := rand.Int63()

		job := getJob(r, st.processInstance.Key)
		err := s.SaveJob(ctx, job)
		assert.Nil(t, err)

		messageSub := getMessage(r, st.processDefinition.Key, st.processInstance.Key, job)
		err = s.SaveMessageSubscription(ctx, messageSub)
		assert.Nil(t, err)

		messageSubs, err := s.FindProcessInstanceMessageSubscriptions(ctx, st.processInstance.Key, runtime.ActivityStateActive)
		assert.Nil(t, err)
		assert.Truef(t, slices.ContainsFunc(messageSubs, messageSub.EqualTo), "expected to find message subscription in message subscriptions array: %+v", messageSubs)

		messageSubs, err = s.FindActivityMessageSubscriptions(ctx, job.Key, runtime.ActivityStateActive)
		assert.Nil(t, err)
		assert.Truef(t, slices.ContainsFunc(messageSubs, messageSub.EqualTo), "expected to find message subscription in message subscriptions array: %+v", messageSubs)
	}
}
