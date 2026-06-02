package storagetest

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	stdruntime "runtime"

	"slices"

	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	dmnruntime "github.com/pbinitiative/zenbpm/pkg/dmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/stretchr/testify/assert"
)

type StorageTestFunc func(s storage.Storage, t *testing.T) func(t *testing.T)

type StorageTester struct {
	processDefinition bpmnruntime.ProcessDefinition
	processInstance   bpmnruntime.ProcessInstance
}

func (st *StorageTester) GetTests() map[string]StorageTestFunc {
	tests := map[string]StorageTestFunc{}

	// all test functions need to be registered here
	functions := []StorageTestFunc{
		st.TestProcessDefinitionStorageWriter,
		st.TestProcessDefinitionStorageReaderBasic,
		// st.TestProcessDefinitionStorageReaderFind,
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
		st.TestDecisionDefinitionStorageWriter,
		st.TestDecisionDefinitionStorageReaderGetSingle,
		st.TestDecisionDefinitionStorageReaderGetMultiple,
		st.TestDecisionStorageWriter,
		st.TestDecisionStorageReaderGetSingle,
		st.TestDecisionStorageReaderGetMultiple,
		st.TestSaveFlowElementInstanceWriter,
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

func getProcessDefinition(r int64) bpmnruntime.ProcessDefinition {
	data := `<?xml version="1.0" encoding="UTF-8"?><bpmn:process id="Simple_Task_Process%d" name="aName" isExecutable="true"></bpmn:process></xml>`
	return bpmnruntime.ProcessDefinition{
		BpmnProcessId: fmt.Sprintf("id-%d", r),
		Version:       1,
		Key:           r,
		BpmnData:      fmt.Sprintf(data, r),
		BpmnChecksum:  [16]byte{1},
	}
}

func getDmnResourceDefinition(r int64) dmnruntime.DmnResourceDefinition {
	data := `<?xml version="1.0" encoding="UTF-8"?><definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" xmlns:dmndi="https://www.omg.org/spec/DMN/20191111/DMNDI/" xmlns:dc="http://www.omg.org/spec/DMN/20180521/DC/" id="Definitions_1e04521" name="DRD" namespace="http://camunda.org/schema/1.0/dmn" xmlns:modeler="http://camunda.org/schema/modeler/1.0" exporter="ZenBPM Modeler" exporterVersion="1.0.0"><decision id="Decision_0xcqx00" name="Decision 1"><decisionTable id="DecisionTable_0q2yhyz"><input id="Input_1"><inputExpression id="InputExpression_1" typeRef="string"><text></text></inputExpression></input><output id="Output_1" typeRef="string" /></decisionTable></decision><dmndi:DMNDI><dmndi:DMNDiagram><dmndi:DMNShape dmnElementRef="Decision_0xcqx00"><dc:Bounds height="80" width="180" x="160" y="100" /></dmndi:DMNShape></dmndi:DMNDiagram></dmndi:DMNDI></definitions>`
	return dmnruntime.DmnResourceDefinition{
		Id:                fmt.Sprintf("id-%d", r),
		Version:           1,
		Key:               r,
		DmnData:           []byte(fmt.Sprintf(data, r)),
		DmnChecksum:       [16]byte{1},
		DmnDefinitionName: fmt.Sprintf("resource-%d", r),
	}
}

func getDecisionDefinition(r int64, dmnResourceDefinitionKey int64) dmnruntime.DecisionDefinition {
	return dmnruntime.DecisionDefinition{
		Version:                  1,
		Id:                       fmt.Sprintf("id-%d", r),
		Key:                      r,
		VersionTag:               "123",
		DmnResourceDefinitionId:  fmt.Sprintf("id-%d", dmnResourceDefinitionKey),
		DmnResourceDefinitionKey: dmnResourceDefinitionKey,
	}
}

// prepareTestData will prepare common data for the tests
func (st *StorageTester) PrepareTestData(s storage.Storage, t *testing.T) {
	r := s.GenerateId()

	st.processDefinition = getProcessDefinition(r)
	err := s.SaveProcessDefinition(t.Context(), st.processDefinition)
	assert.NoError(t, err)

	st.processInstance = getProcessInstance(r, st.processDefinition)
	err = s.SaveProcessInstance(t.Context(), st.processInstance)
	assert.NoError(t, err)
}

func (st *StorageTester) TestProcessDefinitionStorageWriter(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		r := s.GenerateId()

		def := getProcessDefinition(r)

		err := s.SaveProcessDefinition(t.Context(), def)
		assert.NoError(t, err)

		definition, err := s.FindProcessDefinitionByKey(t.Context(), r)
		assert.NoError(t, err)
		assert.Equal(t, r, definition.Key)
	}
}

func (st *StorageTester) TestProcessDefinitionStorageReaderBasic(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {

		r := s.GenerateId()

		def := getProcessDefinition(r)

		err := s.SaveProcessDefinition(t.Context(), def)
		assert.NoError(t, err)

		definition, err := s.FindLatestProcessDefinitionById(t.Context(), def.BpmnProcessId)
		assert.NoError(t, err)
		assert.Equal(t, r, definition.Key)

		definition, err = s.FindProcessDefinitionByKey(t.Context(), def.Key)
		assert.NoError(t, err)
		assert.Equal(t, r, definition.Key)

		definitions, err := s.FindProcessDefinitionsById(t.Context(), def.BpmnProcessId)
		assert.NoError(t, err)
		assert.Len(t, definitions, 1)
		assert.Equal(t, definitions[0].Key, definition.Key)

	}
}

func getProcessInstance(r int64, d bpmnruntime.ProcessDefinition, jobs ...bpmnruntime.Job) bpmnruntime.ProcessInstance {
	return &bpmnruntime.DefaultProcessInstance{
		ProcessInstanceData: bpmnruntime.ProcessInstanceData{
			Definition: &d,
			Key:        r,
			VariableHolder: bpmnruntime.NewVariableHolder(nil, map[string]interface{}{
				"v1":   float64(123),
				"var2": "val2",
			}),
			CreatedAt: time.Now().Truncate(time.Millisecond),
			State:     bpmnruntime.ActivityStateActive,
		},
	}
}

func getJob(key, piKey int64, token bpmnruntime.ExecutionToken) bpmnruntime.Job {
	return bpmnruntime.Job{
		ElementId:          fmt.Sprintf("job-%d", key),
		ElementInstanceKey: key + 200,
		ProcessInstanceKey: piKey,
		Key:                key,
		Type:               "test-job",
		State:              bpmnruntime.ActivityStateActive,
		CreatedAt:          time.Now().Truncate(time.Millisecond),
		Token:              token,
		InputVariables:     map[string]any{"foo": "bar"},
	}
}

func (st *StorageTester) TestProcessInstanceStorageWriter(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {

		r := s.GenerateId()
		token := bpmnruntime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ProcessInstanceKey: st.processInstance.ProcessInstance().Key,
			State:              bpmnruntime.TokenStateWaiting,
		}
		s.SaveToken(t.Context(), token)

		inst := getProcessInstance(r, st.processDefinition, getJob(r, st.processInstance.ProcessInstance().Key, token))

		err := s.SaveProcessInstance(t.Context(), inst)
		assert.NoError(t, err)
	}
}

func (st *StorageTester) TestProcessInstanceStorageReader(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {

		r := s.GenerateId()
		token := bpmnruntime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ProcessInstanceKey: st.processInstance.ProcessInstance().Key,
			State:              bpmnruntime.TokenStateWaiting,
		}
		s.SaveToken(t.Context(), token)

		inst := getProcessInstance(r, st.processDefinition, getJob(r, st.processInstance.ProcessInstance().Key, token))

		err := s.SaveProcessInstance(t.Context(), inst)
		assert.NoError(t, err)

		instance, err := s.FindProcessInstanceByKey(t.Context(), inst.ProcessInstance().Key)
		assert.NoError(t, err)
		assert.Equal(t, inst.ProcessInstance().Key, instance.ProcessInstance().Key)
		assert.Equal(t, inst.ProcessInstance().CreatedAt.Truncate(time.Millisecond), instance.ProcessInstance().CreatedAt.Truncate(time.Millisecond))
		assert.Equal(t, inst.ProcessInstance().VariableHolder, instance.ProcessInstance().VariableHolder)

		//testRefresh
		instance.ProcessInstance().State = bpmnruntime.ActivityStateTerminated
		err = s.SaveProcessInstance(t.Context(), instance)
		assert.NoError(t, err)
		err = s.RefreshProcessInstance(t.Context(), inst)
		assert.NoError(t, err)
		assert.Equal(t, bpmnruntime.ActivityStateTerminated, inst.ProcessInstance().State)

		// TODO: uncomment once its implemented
		// assert.Equal(t, len(inst.Activities), len(instance.Activities))
		// assert.Equal(t, inst.Activities[0], instance.Activities[0])
	}
}

func getTimer(key, pdKey, piKey int64, originActivity bpmnruntime.Job) bpmnruntime.Timer {
	return bpmnruntime.Timer{
		ElementId:            fmt.Sprintf("timer-%d", key),
		Key:                  key,
		ElementInstanceKey:   &key,
		ProcessDefinitionKey: pdKey,
		ProcessInstanceKey:   &piKey,
		TimerState:           bpmnruntime.TimerStateCreated,
		CreatedAt:            time.Now().Truncate(time.Millisecond),
		DueAt:                time.Now().Add(1 * time.Hour).Truncate(time.Millisecond),
		Token: &bpmnruntime.ExecutionToken{
			Key:                key,
			ElementInstanceKey: key,
			ElementId:          "",
			ProcessInstanceKey: piKey,
			State:              bpmnruntime.TokenStateWaiting,
		},
		Duration: 1 * time.Hour,
	}
}

func (st *StorageTester) TestTimerStorageWriter(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {

		r := s.GenerateId()
		token := bpmnruntime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ProcessInstanceKey: st.processInstance.ProcessInstance().Key,
			State:              bpmnruntime.TokenStateWaiting,
		}
		s.SaveToken(t.Context(), token)

		job := getJob(r, st.processInstance.ProcessInstance().Key, token)
		err := s.SaveJob(t.Context(), job)
		assert.NoError(t, err)

		timer := getTimer(r, st.processDefinition.Key, st.processInstance.ProcessInstance().Key, job)

		err = s.SaveTimer(t.Context(), timer)
		assert.NoError(t, err)

		t.Run("Create and delete process definitions timers", func(t *testing.T) {
			pdTimerKey := s.GenerateId()
			pdTimer := bpmnruntime.Timer{
				ElementId:            fmt.Sprintf("timer-start-%d", pdTimerKey),
				Key:                  pdTimerKey,
				ElementInstanceKey:   nil,
				ProcessDefinitionKey: st.processDefinition.Key,
				ProcessInstanceKey:   nil,
				TimerState:           bpmnruntime.TimerStateCreated,
				CreatedAt:            time.Now().Truncate(time.Millisecond),
				DueAt:                time.Now().Add(1 * time.Hour).Truncate(time.Millisecond),
				Token:                nil,
			}
			err = s.SaveTimer(t.Context(), pdTimer)
			assert.NoError(t, err)

			err = s.DeleteProcessDefinitionsTimers(t.Context(), []int64{st.processDefinition.Key})
			assert.NoError(t, err)

			timers, err := s.FindTimersTo(t.Context(), pdTimer.DueAt.Add(1*time.Second))
			assert.NoError(t, err)
			foundProcessInstanceTimer := false
			for _, found := range timers {
				assert.NotEqual(t, pdTimerKey, found.Key, "process-definition-level timer should have been deleted")
				if found.Key == timer.Key {
					foundProcessInstanceTimer = true
				}
			}
			assert.True(t, foundProcessInstanceTimer, "process-instance timer should not have been deleted")
		})
	}
}

func (st *StorageTester) TestTimerStorageReader(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {

		r := s.GenerateId()
		token := bpmnruntime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ProcessInstanceKey: st.processInstance.ProcessInstance().Key,
			State:              bpmnruntime.TokenStateWaiting,
		}
		s.SaveToken(t.Context(), token)

		job := getJob(r, st.processInstance.ProcessInstance().Key, token)
		err := s.SaveJob(t.Context(), job)
		assert.NoError(t, err)

		timer := getTimer(r, st.processDefinition.Key, st.processInstance.ProcessInstance().Key, job)

		err = s.SaveToken(t.Context(), *timer.Token)
		assert.NoError(t, err)

		err = s.SaveTimer(t.Context(), timer)
		assert.NoError(t, err)

		timers, err := s.FindTimersTo(t.Context(), timer.DueAt.Add(1*time.Second))
		assert.NoError(t, err)
		assert.Truef(t, slices.ContainsFunc(timers, timer.EqualTo), "expected to find timer %v in timers array: %+v", timer, timers)

		timers, err = s.FindTokenActiveTimerSubscriptions(t.Context(), timer.Token.Key)
		assert.NoError(t, err)
		assert.Truef(t, slices.ContainsFunc(timers, timer.EqualTo), "expected to find timer %v in timers array: %+v", timer, timers)

		timers, err = s.FindProcessDefinitionTimers(t.Context(), st.processDefinition.Key, bpmnruntime.TimerStateCreated)
		assert.NoError(t, err)
		assert.Truef(t, slices.ContainsFunc(timers, timer.EqualTo), "expected to find timer %v in FindProcessDefinitionTimers result: %+v", timer, timers)

		timers, err = s.FindProcessDefinitionTimers(t.Context(), st.processDefinition.Key, bpmnruntime.TimerStateTriggered)
		assert.NoError(t, err)
		assert.Falsef(t, slices.ContainsFunc(timers, timer.EqualTo), "timer in TimerStateCreated should not appear in TimerStateTriggered results: %+v", timers)

		// Element-aware instance query returns the timer for its element only.
		timers, err = s.FindProcessInstanceTimersByElement(t.Context(), *timer.ProcessInstanceKey, timer.ElementId, bpmnruntime.TimerStateCreated)
		assert.NoError(t, err)
		assert.Truef(t, slices.ContainsFunc(timers, timer.EqualTo), "expected to find timer %v in FindProcessInstanceTimersByElement result: %+v", timer, timers)

		timers, err = s.FindProcessInstanceTimersByElement(t.Context(), *timer.ProcessInstanceKey, timer.ElementId+"-other", bpmnruntime.TimerStateCreated)
		assert.NoError(t, err)
		assert.Falsef(t, slices.ContainsFunc(timers, timer.EqualTo), "timer should not be returned for a different element id: %+v", timers)

		// Definition-level (process_instance_key IS NULL) element-aware query.
		defTimerKey := s.GenerateId()
		defTimer := bpmnruntime.Timer{
			ElementId:            fmt.Sprintf("def-timer-%d", defTimerKey),
			Key:                  defTimerKey,
			ProcessDefinitionKey: st.processDefinition.Key,
			TimerState:           bpmnruntime.TimerStateCreated,
			CreatedAt:            time.Now().Truncate(time.Millisecond),
			DueAt:                time.Now().Add(1 * time.Hour).Truncate(time.Millisecond),
			Duration:             1 * time.Hour,
		}
		err = s.SaveTimer(t.Context(), defTimer)
		assert.NoError(t, err)

		timers, err = s.FindProcessDefinitionTimersByElement(t.Context(), st.processDefinition.Key, defTimer.ElementId, bpmnruntime.TimerStateCreated)
		assert.NoError(t, err)
		assert.Truef(t, slices.ContainsFunc(timers, defTimer.EqualTo), "expected to find definition timer %v in FindProcessDefinitionTimersByElement result: %+v", defTimer, timers)
		assert.Falsef(t, slices.ContainsFunc(timers, timer.EqualTo), "instance-scoped timer must not appear in definition-level element results: %+v", timers)

		timerTest, err := s.GetTimer(t.Context(), timer.Token.Key)
		assert.NoError(t, err)
		assert.Equal(t, timer.Key, timerTest.Key)
	}
}

func (st *StorageTester) TestJobStorageWriter(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {

		r := s.GenerateId()
		token := bpmnruntime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ProcessInstanceKey: st.processInstance.ProcessInstance().Key,
			State:              bpmnruntime.TokenStateWaiting,
		}
		s.SaveToken(t.Context(), token)

		job := getJob(r, st.processInstance.ProcessInstance().Key, token)

		err := s.SaveJob(t.Context(), job)
		assert.Nil(t, err)
	}
}

func (st *StorageTester) TestJobStorageReader(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {

		r := s.GenerateId()
		token := bpmnruntime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ProcessInstanceKey: st.processInstance.ProcessInstance().Key,
			State:              bpmnruntime.TokenStateWaiting,
		}
		s.SaveToken(t.Context(), token)

		job := getJob(r, st.processInstance.ProcessInstance().Key, token)
		err := s.SaveJob(t.Context(), job)
		assert.NoError(t, err)

		jobs, err := s.FindPendingProcessInstanceJobs(t.Context(), st.processInstance.ProcessInstance().Key)
		assert.NoError(t, err)
		assert.Contains(t, jobs, job)

		storeJob, err := s.FindJobByJobKey(t.Context(), job.Key)
		assert.NoError(t, err)
		assert.Equal(t, job, storeJob)
		assert.NotEmpty(t, job.Type)

		storeJobs, err := s.GetJobsInStateByTokenKey(t.Context(), token.Key, []bpmnruntime.ActivityState{bpmnruntime.ActivityStateActive})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(storeJobs))
		assert.Equal(t, job, storeJobs[0])
		assert.NotEmpty(t, storeJobs[0].Type)
	}
}

func getMessage(r int64, piKey int64, pdKey int64, token bpmnruntime.ExecutionToken) bpmnruntime.MessageSubscription {
	return &bpmnruntime.TokenMessageSubscription{
		Token:              token,
		ProcessInstanceKey: piKey,
		CorrelationKey:     fmt.Sprintf("correlation-%d", r),
		MessageSubscriptionData: bpmnruntime.MessageSubscriptionData{
			ElementId:            fmt.Sprintf("message-%d", r),
			Key:                  r + 400,
			ProcessDefinitionKey: pdKey,
			Name:                 fmt.Sprintf("message-%d", r),
			State:                bpmnruntime.ActivityStateActive,
			CreatedAt:            time.Now().Truncate(time.Millisecond),
		},
	}
}

func (st *StorageTester) TestMessageStorageWriter(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {

		r := s.GenerateId()
		token := bpmnruntime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ProcessInstanceKey: st.processInstance.ProcessInstance().Key,
			State:              bpmnruntime.TokenStateWaiting,
		}
		s.SaveToken(t.Context(), token)

		job := getJob(r, st.processInstance.ProcessInstance().Key, token)
		err := s.SaveJob(t.Context(), job)
		assert.NoError(t, err)

		message := getMessage(r, st.processInstance.ProcessInstance().Key, st.processDefinition.Key, bpmnruntime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ElementId:          "messageElementId",
			ProcessInstanceKey: st.processInstance.ProcessInstance().Key,
			State:              bpmnruntime.TokenStateWaiting,
		})

		err = s.SaveMessageSubscription(t.Context(), message)
		assert.NoError(t, err)

		t.Run("Create and delete process definition message subscriptions", func(t *testing.T) {
			defSubKey := s.GenerateId()
			defSub := &bpmnruntime.DefinitionMessageSubscription{
				MessageSubscriptionData: bpmnruntime.MessageSubscriptionData{
					ElementId:            fmt.Sprintf("message-start-%d", defSubKey),
					Key:                  defSubKey,
					ProcessDefinitionKey: st.processDefinition.Key,
					Name:                 fmt.Sprintf("message-start-%d", defSubKey),
					State:                bpmnruntime.ActivityStateActive,
					CreatedAt:            time.Now().Truncate(time.Millisecond),
				},
			}
			err = s.SaveMessageSubscription(t.Context(), defSub)
			assert.NoError(t, err)

			err = s.DeleteProcessDefinitionsMessageSubscriptions(t.Context(), []int64{st.processDefinition.Key})
			assert.NoError(t, err)

			// definition-level subscription should be gone
			_, err = s.FindMessageSubscriptionByKey(t.Context(), defSubKey, bpmnruntime.ActivityStateActive)
			assert.ErrorIs(t, err, storage.ErrNotFound, "definition-level message subscription should have been deleted")

			// token/process-instance level subscriptions for the same process definition
			// must be preserved (only subscriptions without a process_instance_key and
			// execution_token are deleted).
			gotMsg, err := s.FindMessageSubscriptionByKey(t.Context(), message.MessageSubscription().Key, bpmnruntime.ActivityStateActive)
			assert.NoError(t, err, "token-level message subscription should not have been deleted")
			assert.Equal(t, message.MessageSubscription().Key, gotMsg.MessageSubscription().Key)
		})
	}
}

func (st *StorageTester) TestMessageStorageReader(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {

		r := s.GenerateId()

		token := bpmnruntime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ElementId:          "47b623dd-54ab-407e-86dc-847b62d22318",
			ProcessInstanceKey: st.processInstance.ProcessInstance().Key,
			State:              bpmnruntime.TokenStateWaiting,
		}
		s.SaveToken(t.Context(), token)

		messageSub := getMessage(r, st.processInstance.ProcessInstance().Key, st.processDefinition.Key, token)
		err := s.SaveMessageSubscription(t.Context(), messageSub)
		assert.NoError(t, err)

		messageSubs, err := s.FindProcessInstanceMessageSubscriptions(t.Context(), st.processInstance.ProcessInstance().Key, bpmnruntime.ActivityStateActive)
		assert.NoError(t, err)
		assert.Truef(t, slices.ContainsFunc(messageSubs, func(sub bpmnruntime.MessageSubscription) bool {
			return bpmnruntime.EqualTo(messageSub, sub)
		}), "expected to find message subscription in message subscriptions array: %+v", messageSubs)

		messageSubs, err = s.FindTokenMessageSubscriptions(t.Context(), token.Key, bpmnruntime.ActivityStateActive)
		assert.NoError(t, err)
		assert.Truef(t, slices.ContainsFunc(messageSubs, func(sub bpmnruntime.MessageSubscription) bool {
			return bpmnruntime.EqualTo(messageSub, sub)
		}), "expected to find message subscription in message subscriptions array: %+v", messageSubs)

		// Backend-parity coverage for the message-start-event hot path:
		// FindMessageSubscriptionByName(name, nil, Active) must locate a definition-level
		// subscription (correlation_key IS NULL) for inbound messages without a correlation key.
		defSubKey := s.GenerateId()
		defSubName := fmt.Sprintf("msg-start-no-correlation-%d", defSubKey)
		defSub := &bpmnruntime.DefinitionMessageSubscription{
			MessageSubscriptionData: bpmnruntime.MessageSubscriptionData{
				ElementId:            fmt.Sprintf("startEvt-%d", defSubKey),
				Key:                  defSubKey,
				ProcessDefinitionKey: st.processDefinition.Key,
				Name:                 defSubName,
				State:                bpmnruntime.ActivityStateActive,
				CreatedAt:            time.Now().Truncate(time.Millisecond),
			},
		}
		assert.NoError(t, s.SaveMessageSubscription(t.Context(), defSub))

		foundDefSub, err := s.FindMessageSubscriptionByName(t.Context(), defSubName, nil, bpmnruntime.ActivityStateActive)
		assert.NoError(t, err, "FindMessageSubscriptionByName(name, nil, Active) must locate a definition-level subscription")
		if assert.NotNil(t, foundDefSub) {
			assert.Equal(t, defSubKey, foundDefSub.MessageSubscription().Key)
			_, isDef := foundDefSub.(*bpmnruntime.DefinitionMessageSubscription)
			assert.True(t, isDef, "expected returned subscription to be a DefinitionMessageSubscription, got %T", foundDefSub)
		}
	}
}

func (st *StorageTester) TestTokenStorageWriter(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {

		r := s.GenerateId()

		token1 := bpmnruntime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ElementId:          "test-elem",
			ProcessInstanceKey: st.processInstance.ProcessInstance().Key,
			State:              bpmnruntime.TokenStateWaiting,
		}

		err := s.SaveToken(t.Context(), token1)
		assert.Nil(t, err)
	}
}

func (st *StorageTester) TestTokenStorageReader(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {

		r := s.GenerateId()
		token1 := bpmnruntime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ElementId:          "test-elem",
			ProcessInstanceKey: st.processInstance.ProcessInstance().Key,
			State:              bpmnruntime.TokenStateRunning,
		}
		err := s.SaveToken(t.Context(), token1)
		assert.NoError(t, err)

		r = s.GenerateId()
		token2 := bpmnruntime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ElementId:          "test-elem",
			ProcessInstanceKey: st.processInstance.ProcessInstance().Key,
			State:              bpmnruntime.TokenStateCompleted,
		}
		err = s.SaveToken(t.Context(), token2)
		assert.NoError(t, err)

		r = s.GenerateId()
		token3 := bpmnruntime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ElementId:          "test-elem",
			ProcessInstanceKey: st.processInstance.ProcessInstance().Key,
			State:              bpmnruntime.TokenStateWaiting,
		}
		err = s.SaveToken(t.Context(), token3)
		assert.NoError(t, err)

		tokens, err := s.GetRunningTokens(t.Context())
		assert.NoError(t, err)
		matched := false
		for _, tok := range tokens {
			if tok.ElementInstanceKey == token1.ElementInstanceKey {
				matched = true
				break
			}
		}
		assert.True(t, matched, "expected to find created token among active tokens for partition")

		tokens, err = s.GetAllTokensForProcessInstance(t.Context(), st.processInstance.ProcessInstance().Key)
		assert.NoError(t, err)
		matchedTwice := 0
		for _, tok := range tokens {
			if tok.ElementInstanceKey == token1.ElementInstanceKey || tok.ElementInstanceKey == token2.ElementInstanceKey {
				matchedTwice++
			}
		}
		assert.Equal(t, 2, matchedTwice, "expected to find created tokens among tokens for partition")

		tokens, err = s.GetActiveTokensForProcessInstance(t.Context(), st.processInstance.ProcessInstance().Key)
		assert.NoError(t, err)
		matchedTwice = 0
		for _, tok := range tokens {
			if tok.ElementInstanceKey == token1.ElementInstanceKey || tok.ElementInstanceKey == token3.ElementInstanceKey {
				matchedTwice++
			}
		}
		assert.Equal(t, 2, matchedTwice, "expected to find created tokens among tokens for partition")
	}
}

func (st *StorageTester) TestSaveFlowElementInstanceWriter(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		r := s.GenerateId()

		historyItem := bpmnruntime.FlowElementInstance{
			Key:                r,
			ProcessInstanceKey: st.processInstance.ProcessInstance().Key,
			ElementId:          "test-elem",
			CreatedAt:          time.Now().Truncate(time.Millisecond),
			ExecutionTokenKey:  r,
			InputVariables:     map[string]any{"test": "test"},
			OutputVariables:    map[string]any{"test": "test"},
		}
		err := s.SaveFlowElementInstance(t.Context(), historyItem)
		assert.Nil(t, err)
	}
}

func (st *StorageTester) TestIncidentStorageWriter(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		r := s.GenerateId()
		tok := s.GenerateId()

		incident := bpmnruntime.Incident{
			Key:                r,
			ElementInstanceKey: r,
			ElementId:          "test-elem",
			ProcessInstanceKey: st.processInstance.ProcessInstance().Key,
			Message:            "test-message",
			Token: bpmnruntime.ExecutionToken{
				Key:                tok,
				ElementInstanceKey: tok,
				ElementId:          "test-elem",
				ProcessInstanceKey: st.processInstance.ProcessInstance().Key,
				State:              bpmnruntime.TokenStateWaiting,
			},
		}

		err := s.SaveIncident(t.Context(), incident)
		assert.Nil(t, err)

	}
}

func (st *StorageTester) TestIncidentStorageReader(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {

		r := s.GenerateId()
		tok := s.GenerateId()

		token := bpmnruntime.ExecutionToken{
			Key:                tok,
			ElementInstanceKey: tok,
			ElementId:          "test-elem",
			ProcessInstanceKey: st.processInstance.ProcessInstance().Key,
			State:              bpmnruntime.TokenStateWaiting,
		}

		incident := bpmnruntime.Incident{
			Key:                r,
			ElementInstanceKey: r,
			ElementId:          "test-elem",
			ProcessInstanceKey: st.processInstance.ProcessInstance().Key,
			Message:            "test-message",
			CreatedAt:          time.Time{}.Local(),
			ResolvedAt:         nil,
			Token:              token,
		}

		err := s.SaveIncident(t.Context(), incident)
		assert.Nil(t, err)

		err = s.SaveToken(t.Context(), token)
		assert.Nil(t, err)

		testIncident, err := s.FindIncidentByKey(t.Context(), r)
		assert.Nil(t, err)
		assert.Equal(t, incident, testIncident)

		testIncidents, err := s.FindIncidentsByProcessInstanceKey(t.Context(), st.processInstance.ProcessInstance().Key)
		assert.Nil(t, err)
		assert.NotEmpty(t, testIncidents)
		for _, testIncident := range testIncidents {
			if testIncident.Token.Key == token.Key {
				assert.Equal(t, incident, testIncident)
			}
		}

		testIncidents, err = s.FindIncidentsByExecutionTokenKey(t.Context(), tok)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(testIncidents))
		assert.Equal(t, incident, testIncidents[0])

		// Verify that FindIncidentByKey returns the current token state
		updatedToken := token
		updatedToken.State = bpmnruntime.TokenStateFailed
		err = s.SaveToken(t.Context(), updatedToken)
		assert.Nil(t, err)

		refreshedIncident, err := s.FindIncidentByKey(t.Context(), r)
		assert.Nil(t, err)
		assert.Equal(t, bpmnruntime.TokenStateFailed, refreshedIncident.Token.State, "FindIncidentByKey should return the current token state, not a stale snapshot")

		// The list readers must expose the same current token state as FindIncidentByKey.
		byProcessInstance, err := s.FindIncidentsByProcessInstanceKey(t.Context(), st.processInstance.ProcessInstance().Key)
		assert.Nil(t, err)
		for _, listed := range byProcessInstance {
			if listed.Key == r {
				assert.Equal(t, bpmnruntime.TokenStateFailed, listed.Token.State, "FindIncidentsByProcessInstanceKey should return the current token state, not a stale snapshot")
			}
		}

		byToken, err := s.FindIncidentsByExecutionTokenKey(t.Context(), tok)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(byToken))
		assert.Equal(t, bpmnruntime.TokenStateFailed, byToken[0].Token.State, "FindIncidentsByExecutionTokenKey should return the current token state, not a stale snapshot")

		// Incidents created for event subprocess start event subscriptions have no
		// execution token yet, so the token is left as a zero value. FindIncidentByKey
		// must still be able to return such incidents instead of failing to load a token.
		noTokenIncidentKey := s.GenerateId()
		noTokenIncident := bpmnruntime.Incident{
			Key:                noTokenIncidentKey,
			ElementInstanceKey: noTokenIncidentKey,
			ElementId:          "event-subprocess-start",
			ProcessInstanceKey: st.processInstance.ProcessInstance().Key,
			Message:            "failed to evaluate correlation key",
			CreatedAt:          time.Time{}.Local(),
			ResolvedAt:         nil,
			Token:              bpmnruntime.ExecutionToken{},
		}

		err = s.SaveIncident(t.Context(), noTokenIncident)
		assert.Nil(t, err)

		testNoTokenIncident, err := s.FindIncidentByKey(t.Context(), noTokenIncidentKey)
		assert.Nil(t, err)
		assert.Equal(t, noTokenIncident, testNoTokenIncident)
		assert.Equal(t, int64(0), testNoTokenIncident.Token.Key)

		// Incidents that reference a token which is no longer persisted must remain readable
		// for history/listing purposes. Callers that need to act on the token are responsible
		// for loading the current token state explicitly.
		danglingIncidentKey := s.GenerateId()
		danglingTokenKey := s.GenerateId()
		danglingIncident := bpmnruntime.Incident{
			Key:                danglingIncidentKey,
			ElementInstanceKey: danglingIncidentKey,
			ElementId:          "dangling-token-incident",
			ProcessInstanceKey: st.processInstance.ProcessInstance().Key,
			Message:            "referenced token was removed",
			CreatedAt:          time.Time{}.Local(),
			ResolvedAt:         nil,
			Token: bpmnruntime.ExecutionToken{
				Key:                danglingTokenKey,
				ElementInstanceKey: danglingIncidentKey,
				ElementId:          "dangling-token-incident",
				ProcessInstanceKey: st.processInstance.ProcessInstance().Key,
			},
		}

		err = s.SaveIncident(t.Context(), danglingIncident)
		assert.Nil(t, err)

		testDanglingIncident, err := s.FindIncidentByKey(t.Context(), danglingIncidentKey)
		assert.Nil(t, err)
		assert.Equal(t, danglingIncident, testDanglingIncident)
	}
}

func (st *StorageTester) TestDecisionDefinitionStorageWriter(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		//setup
		r := s.GenerateId()
		def := getDmnResourceDefinition(r)
		err := s.SaveDmnResourceDefinition(t.Context(), def)
		assert.NoError(t, err)

		//run
		definition, err := s.FindDmnResourceDefinitionByKey(t.Context(), def.Key)
		assert.NoError(t, err)
		assert.Equal(t, def.Key, definition.Key)
	}
}

func (st *StorageTester) TestDecisionDefinitionStorageReaderGetSingle(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		//setup
		r := s.GenerateId()
		def := getDmnResourceDefinition(r)
		err := s.SaveDmnResourceDefinition(t.Context(), def)
		assert.NoError(t, err)

		r2 := s.GenerateId()
		def2 := getDmnResourceDefinition(r2)
		err = s.SaveDmnResourceDefinition(t.Context(), def2)
		assert.NoError(t, err)

		//run
		definition, err := s.FindLatestDmnResourceDefinitionById(t.Context(), def.Id)
		assert.NoError(t, err)
		assert.Equal(t, r, definition.Key)

		definition, err = s.FindDmnResourceDefinitionByKey(t.Context(), def.Key)
		assert.NoError(t, err)
		assert.Equal(t, r, definition.Key)
	}
}

func (st *StorageTester) TestDecisionDefinitionStorageReaderGetMultiple(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		//setup
		r := s.GenerateId()
		def := getDmnResourceDefinition(r)
		err := s.SaveDmnResourceDefinition(t.Context(), def)
		assert.NoError(t, err)

		r2 := s.GenerateId()
		def2 := getDmnResourceDefinition(r2)
		err = s.SaveDmnResourceDefinition(t.Context(), def2)
		assert.NoError(t, err)

		//run
		definitions, err := s.FindDmnResourceDefinitionsById(t.Context(), def.Id)
		assert.NoError(t, err)
		assert.Len(t, definitions, 1)
		assert.Equal(t, definitions[0].Key, def.Key)
		assert.Equal(t, definitions[0].Id, def.Id)
	}
}

func (st *StorageTester) TestDecisionStorageWriter(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		//setup
		dmnResourceDefinitionKey := s.GenerateId()
		def := getDmnResourceDefinition(dmnResourceDefinitionKey)
		err := s.SaveDmnResourceDefinition(t.Context(), def)
		assert.NoError(t, err)

		r := s.GenerateId()
		dec := getDecisionDefinition(r, dmnResourceDefinitionKey)
		err = s.SaveDecisionDefinition(t.Context(), dec)
		assert.NoError(t, err)

		r2 := s.GenerateId()
		dec2 := getDecisionDefinition(r2, dmnResourceDefinitionKey)
		err = s.SaveDecisionDefinition(t.Context(), dec2)
		assert.NoError(t, err)

		//run
		decisionDefinition, err := s.GetDecisionDefinitionByIdAndDmnResourceDefinitionKey(t.Context(), dec.Id, dmnResourceDefinitionKey)
		assert.NoError(t, err)
		assert.Equal(t, dmnResourceDefinitionKey, dec.DmnResourceDefinitionKey)
		assert.Equal(t, dec.Id, decisionDefinition.Id)
	}
}

func (st *StorageTester) TestDecisionStorageReaderGetSingle(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		//setup
		decisionDefinitionKey := s.GenerateId()
		def := getDmnResourceDefinition(decisionDefinitionKey)
		err := s.SaveDmnResourceDefinition(t.Context(), def)
		assert.NoError(t, err)

		r := s.GenerateId()
		dec := getDecisionDefinition(r, decisionDefinitionKey)
		err = s.SaveDecisionDefinition(t.Context(), dec)
		assert.NoError(t, err)

		r2 := s.GenerateId()
		dec2 := getDecisionDefinition(r2, decisionDefinitionKey)
		err = s.SaveDecisionDefinition(t.Context(), dec2)
		assert.NoError(t, err)

		//run
		decisionDefinition, err := s.GetLatestDecisionDefinitionById(t.Context(), dec.Id)
		assert.NoError(t, err)
		assert.Equal(t, decisionDefinitionKey, decisionDefinition.DmnResourceDefinitionKey)
		assert.Equal(t, dec.Id, decisionDefinition.Id)
		assert.Equal(t, dec.DmnResourceDefinitionId, decisionDefinition.DmnResourceDefinitionId)

		decisionDefinition, err = s.GetLatestDecisionDefinitionByIdAndDmnResourceDefinitionId(t.Context(), dec.Id, dec.DmnResourceDefinitionId)
		assert.NoError(t, err)
		assert.Equal(t, decisionDefinitionKey, decisionDefinition.DmnResourceDefinitionKey)
		assert.Equal(t, dec.Id, decisionDefinition.Id)
		assert.Equal(t, dec.DmnResourceDefinitionId, decisionDefinition.DmnResourceDefinitionId)

		decisionDefinition, err = s.GetLatestDecisionDefinitionByIdAndVersionTag(t.Context(), dec.Id, dec.VersionTag)
		assert.NoError(t, err)
		assert.Equal(t, decisionDefinitionKey, decisionDefinition.DmnResourceDefinitionKey)
		assert.Equal(t, dec.Id, decisionDefinition.Id)
		assert.Equal(t, dec.VersionTag, decisionDefinition.VersionTag)
	}
}

func (st *StorageTester) TestDecisionStorageReaderGetMultiple(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		//setup
		dmnResourceDefinitionKey := s.GenerateId()
		def := getDmnResourceDefinition(dmnResourceDefinitionKey)
		err := s.SaveDmnResourceDefinition(t.Context(), def)
		assert.NoError(t, err)

		r := s.GenerateId()
		dec := getDecisionDefinition(r, dmnResourceDefinitionKey)
		err = s.SaveDecisionDefinition(t.Context(), dec)
		assert.NoError(t, err)

		r2 := s.GenerateId()
		dec2 := getDecisionDefinition(r2, dmnResourceDefinitionKey)
		err = s.SaveDecisionDefinition(t.Context(), dec2)
		assert.NoError(t, err)

		//run
		decisionDefinitions, err := s.GetDecisionDefinitionsById(t.Context(), dec.Id)
		assert.NoError(t, err)
		assert.Len(t, decisionDefinitions, 1)
		assert.Equal(t, dmnResourceDefinitionKey, decisionDefinitions[0].DmnResourceDefinitionKey)
		assert.Equal(t, dec.Id, decisionDefinitions[0].Id)
	}
}
