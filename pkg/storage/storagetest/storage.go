package storagetest

import (
	"context"
	"fmt"
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
		st.TestTokenStorageReader,
		st.TestTokenStorageWriter,
		st.TestIncidentStorageWriter,
		st.TestIncidentStorageReader,
	}

	for _, function := range functions {
		funcName := getFunctionName(function)
		strippedName := funcName[strings.LastIndex(funcName, ".")+1:]
		tests[strippedName] = function
	}
	return tests
}

func getFunctionName(i any) string {
	return stdruntime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

func getProcessDefinition(r int64) runtime.ProcessDefinition {
	data := `<?xml version="1.0" encoding="UTF-8"?><bpmn:process id="Simple_Task_Process%d" name="aName" isExecutable="true"></bpmn:process></xml>`
	return runtime.ProcessDefinition{
		BpmnProcessId:    fmt.Sprintf("id-%d", r),
		Version:          1,
		Key:              r,
		BpmnData:         fmt.Sprintf(data, r),
		BpmnChecksum:     [16]byte{1},
		BpmnResourceName: fmt.Sprintf("resource-%d", r),
	}
}

// prepareTestData will prepare common data for the tests
func (st *StorageTester) PrepareTestData(s storage.Storage, t *testing.T) {
	ctx := context.Background()

	r := s.GenerateId()

	st.processDefinition = getProcessDefinition(r)
	err := s.SaveProcessDefinition(ctx, st.processDefinition)
	assert.NoError(t, err)

	st.processInstance = getProcessInstance(r, st.processDefinition)
	err = s.SaveProcessInstance(ctx, st.processInstance)
	assert.NoError(t, err)
}

func (st *StorageTester) TestProcessDefinitionStorageWriter(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.TODO()

		r := s.GenerateId()

		def := getProcessDefinition(r)

		err := s.SaveProcessDefinition(ctx, def)
		assert.NoError(t, err)

		definition, err := s.FindProcessDefinitionByKey(ctx, r)
		assert.NoError(t, err)
		assert.Equal(t, r, definition.Key)
	}
}

func (st *StorageTester) TestProcessDefinitionStorageReader(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.TODO()

		r := s.GenerateId()

		def := getProcessDefinition(r)

		err := s.SaveProcessDefinition(ctx, def)
		assert.NoError(t, err)

		definition, err := s.FindLatestProcessDefinitionById(ctx, def.BpmnProcessId)
		assert.NoError(t, err)
		assert.Equal(t, r, definition.Key)

		definition, err = s.FindProcessDefinitionByKey(ctx, def.Key)
		assert.NoError(t, err)
		assert.Equal(t, r, definition.Key)

		definitions, err := s.FindProcessDefinitionsById(ctx, def.BpmnProcessId)
		assert.NoError(t, err)
		assert.Len(t, definitions, 1)
		assert.Equal(t, definitions[0].Key, definition.Key)
	}
}

func getProcessInstance(r int64, d runtime.ProcessDefinition, jobs ...runtime.Job) runtime.ProcessInstance {
	return runtime.ProcessInstance{
		Definition: &d,
		Key:        r,
		VariableHolder: runtime.NewVariableHolder(nil, map[string]interface{}{
			"v1":   float64(123),
			"var2": "val2",
		}),
		CreatedAt: time.Now().Truncate(time.Millisecond),
		State:     runtime.ActivityStateActive,
	}
}

func getJob(key, piKey int64, token runtime.ExecutionToken) runtime.Job {
	return runtime.Job{
		ElementId:          fmt.Sprintf("job-%d", key),
		ElementInstanceKey: key + 200,
		ProcessInstanceKey: piKey,
		Key:                key,
		Type:               "test-job",
		State:              runtime.ActivityStateActive,
		CreatedAt:          time.Now().Truncate(time.Millisecond),
		Token:              token,
	}
}

func (st *StorageTester) TestProcessInstanceStorageWriter(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.TODO()

		r := s.GenerateId()
		token := runtime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ProcessInstanceKey: st.processInstance.Key,
			State:              runtime.TokenStateWaiting,
		}
		s.SaveToken(ctx, token)

		inst := getProcessInstance(r, st.processDefinition, getJob(r, st.processInstance.Key, token))

		err := s.SaveProcessInstance(ctx, inst)
		assert.NoError(t, err)
	}
}

func (st *StorageTester) TestProcessInstanceStorageReader(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.TODO()

		r := s.GenerateId()
		token := runtime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ProcessInstanceKey: st.processInstance.Key,
			State:              runtime.TokenStateWaiting,
		}
		s.SaveToken(ctx, token)

		inst := getProcessInstance(r, st.processDefinition, getJob(r, st.processInstance.Key, token))

		err := s.SaveProcessInstance(ctx, inst)
		assert.NoError(t, err)

		instance, err := s.FindProcessInstanceByKey(ctx, inst.Key)
		assert.NoError(t, err)
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

func getTimer(key, pdKey, piKey int64, originActivity runtime.Job) runtime.Timer {
	return runtime.Timer{
		ElementId:            fmt.Sprintf("timer-%d", key),
		Key:                  key,
		ProcessDefinitionKey: pdKey,
		ProcessInstanceKey:   piKey,
		TimerState:           runtime.TimerStateCreated,
		CreatedAt:            time.Now().Truncate(time.Millisecond),
		DueAt:                time.Now().Add(1 * time.Hour).Truncate(time.Millisecond),
		Token: runtime.ExecutionToken{
			Key:                key,
			ElementInstanceKey: key,
			ElementId:          "",
			ProcessInstanceKey: piKey,
			State:              runtime.TokenStateWaiting,
		},
		Duration: 1 * time.Hour,
	}
}

func (st *StorageTester) TestTimerStorageWriter(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.TODO()

		r := s.GenerateId()
		token := runtime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ProcessInstanceKey: st.processInstance.Key,
			State:              runtime.TokenStateWaiting,
		}
		s.SaveToken(ctx, token)

		job := getJob(r, st.processInstance.Key, token)
		err := s.SaveJob(ctx, job)
		assert.NoError(t, err)

		timer := getTimer(r, st.processDefinition.Key, st.processInstance.Key, job)

		err = s.SaveTimer(ctx, timer)
		assert.NoError(t, err)
	}
}

func (st *StorageTester) TestTimerStorageReader(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.TODO()

		r := s.GenerateId()
		token := runtime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ProcessInstanceKey: st.processInstance.Key,
			State:              runtime.TokenStateWaiting,
		}
		s.SaveToken(ctx, token)

		job := getJob(r, st.processInstance.Key, token)
		err := s.SaveJob(ctx, job)
		assert.NoError(t, err)

		timer := getTimer(r, st.processDefinition.Key, st.processInstance.Key, job)

		err = s.SaveTimer(ctx, timer)
		assert.NoError(t, err)

		timers, err := s.FindTimersByState(ctx, st.processInstance.Key, runtime.TimerStateCreated)
		assert.NoError(t, err)
		assert.Truef(t, slices.ContainsFunc(timers, timer.EqualTo), "expected to find timer in timers array: %+v", timers)
	}
}

func (st *StorageTester) TestJobStorageWriter(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.TODO()

		r := s.GenerateId()
		token := runtime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ProcessInstanceKey: st.processInstance.Key,
			State:              runtime.TokenStateWaiting,
		}
		s.SaveToken(ctx, token)

		job := getJob(r, st.processInstance.Key, token)

		err := s.SaveJob(ctx, job)
		assert.Nil(t, err)
	}
}

func (st *StorageTester) TestJobStorageReader(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.TODO()

		r := s.GenerateId()
		token := runtime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ProcessInstanceKey: st.processInstance.Key,
			State:              runtime.TokenStateWaiting,
		}
		s.SaveToken(ctx, token)

		job := getJob(r, st.processInstance.Key, token)
		err := s.SaveJob(ctx, job)
		assert.NoError(t, err)

		jobs, err := s.FindPendingProcessInstanceJobs(ctx, st.processInstance.Key)
		assert.NoError(t, err)
		assert.Contains(t, jobs, job)

		storeJob, err := s.FindJobByJobKey(ctx, job.Key)
		assert.NoError(t, err)
		assert.Equal(t, job, storeJob)
		assert.NotEmpty(t, job.Type)
	}
}

func getMessage(r int64, piKey int64, pdKey int64, token runtime.ExecutionToken) runtime.MessageSubscription {
	return runtime.MessageSubscription{
		ElementId:            fmt.Sprintf("message-%d", r),
		ElementInstanceKey:   r + 400,
		ProcessDefinitionKey: pdKey,
		ProcessInstanceKey:   piKey,
		Name:                 fmt.Sprintf("message-%d", r),
		MessageState:         runtime.ActivityStateActive,
		CreatedAt:            time.Now().Truncate(time.Millisecond),
		Token:                token,
	}
}

func (st *StorageTester) TestMessageStorageWriter(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.TODO()

		r := s.GenerateId()
		token := runtime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ProcessInstanceKey: st.processInstance.Key,
			State:              runtime.TokenStateWaiting,
		}
		s.SaveToken(ctx, token)

		job := getJob(r, st.processInstance.Key, token)
		err := s.SaveJob(ctx, job)
		assert.NoError(t, err)

		message := getMessage(r, st.processDefinition.Key, st.processInstance.Key, runtime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ElementId:          "messageElementId",
			ProcessInstanceKey: st.processInstance.Key,
			State:              runtime.TokenStateWaiting,
		})

		err = s.SaveMessageSubscription(ctx, message)
		assert.NoError(t, err)
	}
}

func (st *StorageTester) TestMessageStorageReader(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.TODO()

		r := s.GenerateId()

		token := runtime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ElementId:          "47b623dd-54ab-407e-86dc-847b62d22318",
			ProcessInstanceKey: st.processInstance.Key,
			State:              runtime.TokenStateWaiting,
		}
		s.SaveToken(ctx, token)

		messageSub := getMessage(r, st.processDefinition.Key, st.processInstance.Key, token)
		err := s.SaveMessageSubscription(ctx, messageSub)
		assert.NoError(t, err)

		messageSubs, err := s.FindProcessInstanceMessageSubscriptions(ctx, st.processInstance.Key, runtime.ActivityStateActive)
		assert.NoError(t, err)
		assert.Truef(t, slices.ContainsFunc(messageSubs, messageSub.EqualTo), "expected to find message subscription in message subscriptions array: %+v", messageSubs)

		messageSubs, err = s.FindTokenMessageSubscriptions(ctx, token.Key, runtime.ActivityStateActive)
		assert.NoError(t, err)
		assert.Truef(t, slices.ContainsFunc(messageSubs, messageSub.EqualTo), "expected to find message subscription in message subscriptions array: %+v", messageSubs)
	}
}

func (st *StorageTester) TestTokenStorageWriter(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.TODO()
		r := s.GenerateId()

		token1 := runtime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ElementId:          "test-elem",
			ProcessInstanceKey: st.processInstance.Key,
			State:              runtime.TokenStateWaiting,
		}

		err := s.SaveToken(ctx, token1)
		assert.Nil(t, err)
	}
}

func (st *StorageTester) TestTokenStorageReader(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.TODO()
		r := s.GenerateId()

		token1 := runtime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ElementId:          "test-elem",
			ProcessInstanceKey: st.processInstance.Key,
			State:              runtime.TokenStateRunning,
		}

		err := s.SaveToken(ctx, token1)
		assert.Nil(t, err)

		tokens, err := s.GetRunningTokens(ctx)
		assert.Nil(t, err)

		matched := false
		for _, tok := range tokens {
			if tok.ElementInstanceKey == token1.ElementInstanceKey {
				matched = true
				break
			}
		}
		assert.True(t, matched, "expected to find created token among active tokens for partition")
	}
}

func (st *StorageTester) TestIncidentStorageWriter(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.TODO()
		r := s.GenerateId()
		tok := s.GenerateId()

		incident := runtime.Incident{
			Key:                r,
			ElementInstanceKey: r,
			ElementId:          "test-elem",
			ProcessInstanceKey: st.processInstance.Key,
			Message:            "test-message",
			Token: runtime.ExecutionToken{
				Key:                tok,
				ElementInstanceKey: tok,
				ElementId:          "test-elem",
				ProcessInstanceKey: st.processInstance.Key,
				State:              runtime.TokenStateWaiting,
			},
		}

		err := s.SaveIncident(ctx, incident)
		assert.Nil(t, err)

	}
}

func (st *StorageTester) TestIncidentStorageReader(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.TODO()
		r := s.GenerateId()
		tok := s.GenerateId()

		token := runtime.ExecutionToken{
			Key:                tok,
			ElementInstanceKey: tok,
			ElementId:          "test-elem",
			ProcessInstanceKey: st.processInstance.Key,
			State:              runtime.TokenStateWaiting,
		}

		incident := runtime.Incident{
			Key:                r,
			ElementInstanceKey: r,
			ElementId:          "test-elem",
			ProcessInstanceKey: st.processInstance.Key,
			Message:            "test-message",
			Token:              token,
		}

		err := s.SaveIncident(ctx, incident)
		assert.Nil(t, err)

		err = s.SaveToken(ctx, token)
		assert.Nil(t, err)

		incident, err = s.FindIncidentByKey(ctx, r)
		assert.Nil(t, err)
		assert.Equal(t, incident, incident)
	}
}
