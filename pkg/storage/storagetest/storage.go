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
		st.TestSaveFlowElementHistoryWriter,
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
	data := `<?xml version="1.0" encoding="UTF-8"?><definitions xmlns="https://www.omg.org/spec/DMN/20191111/MODEL/" xmlns:dmndi="https://www.omg.org/spec/DMN/20191111/DMNDI/" xmlns:dc="http://www.omg.org/spec/DMN/20180521/DC/" id="Definitions_1e04521" name="DRD" namespace="http://camunda.org/schema/1.0/dmn" xmlns:modeler="http://camunda.org/schema/modeler/1.0" exporter="Camunda Modeler" exporterVersion="5.35.0" modeler:executionPlatform="Camunda Cloud" modeler:executionPlatformVersion="8.6.0"><decision id="Decision_0xcqx00" name="Decision 1"><decisionTable id="DecisionTable_0q2yhyz"><input id="Input_1"><inputExpression id="InputExpression_1" typeRef="string"><text></text></inputExpression></input><output id="Output_1" typeRef="string" /></decisionTable></decision><dmndi:DMNDI><dmndi:DMNDiagram><dmndi:DMNShape dmnElementRef="Decision_0xcqx00"><dc:Bounds height="80" width="180" x="160" y="100" /></dmndi:DMNShape></dmndi:DMNDiagram></dmndi:DMNDI></definitions>`
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
	return bpmnruntime.ProcessInstance{
		Definition: &d,
		Key:        r,
		VariableHolder: bpmnruntime.NewVariableHolder(nil, map[string]interface{}{
			"v1":   float64(123),
			"var2": "val2",
		}),
		CreatedAt: time.Now().Truncate(time.Millisecond),
		State:     bpmnruntime.ActivityStateActive,
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
		Variables:          map[string]any{"foo": "bar"},
	}
}

func (st *StorageTester) TestProcessInstanceStorageWriter(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {

		r := s.GenerateId()
		token := bpmnruntime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ProcessInstanceKey: st.processInstance.Key,
			State:              bpmnruntime.TokenStateWaiting,
		}
		s.SaveToken(t.Context(), token)

		inst := getProcessInstance(r, st.processDefinition, getJob(r, st.processInstance.Key, token))

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
			ProcessInstanceKey: st.processInstance.Key,
			State:              bpmnruntime.TokenStateWaiting,
		}
		s.SaveToken(t.Context(), token)

		inst := getProcessInstance(r, st.processDefinition, getJob(r, st.processInstance.Key, token))

		err := s.SaveProcessInstance(t.Context(), inst)
		assert.NoError(t, err)

		instance, err := s.FindProcessInstanceByKey(t.Context(), inst.Key)
		assert.NoError(t, err)
		assert.Equal(t, inst.Key, instance.Key)
		assert.Equal(t, inst.CreatedAt.Truncate(time.Millisecond), instance.CreatedAt.Truncate(time.Millisecond))
		assert.Equal(t, inst.VariableHolder, instance.VariableHolder)

		// TODO: uncomment once its implemented
		// assert.Equal(t, len(inst.Activities), len(instance.Activities))
		// assert.Equal(t, inst.Activities[0], instance.Activities[0])
	}
}

func getTimer(key, pdKey, piKey int64, originActivity bpmnruntime.Job) bpmnruntime.Timer {
	return bpmnruntime.Timer{
		ElementId:            fmt.Sprintf("timer-%d", key),
		Key:                  key,
		ProcessDefinitionKey: pdKey,
		ProcessInstanceKey:   piKey,
		TimerState:           bpmnruntime.TimerStateCreated,
		CreatedAt:            time.Now().Truncate(time.Millisecond),
		DueAt:                time.Now().Add(1 * time.Hour).Truncate(time.Millisecond),
		Token: bpmnruntime.ExecutionToken{
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
			ProcessInstanceKey: st.processInstance.Key,
			State:              bpmnruntime.TokenStateWaiting,
		}
		s.SaveToken(t.Context(), token)

		job := getJob(r, st.processInstance.Key, token)
		err := s.SaveJob(t.Context(), job)
		assert.NoError(t, err)

		timer := getTimer(r, st.processDefinition.Key, st.processInstance.Key, job)

		err = s.SaveTimer(t.Context(), timer)
		assert.NoError(t, err)
	}
}

func (st *StorageTester) TestTimerStorageReader(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {

		r := s.GenerateId()
		token := bpmnruntime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ProcessInstanceKey: st.processInstance.Key,
			State:              bpmnruntime.TokenStateWaiting,
		}
		s.SaveToken(t.Context(), token)

		job := getJob(r, st.processInstance.Key, token)
		err := s.SaveJob(t.Context(), job)
		assert.NoError(t, err)

		timer := getTimer(r, st.processDefinition.Key, st.processInstance.Key, job)

		err = s.SaveToken(t.Context(), timer.Token)
		assert.NoError(t, err)

		err = s.SaveTimer(t.Context(), timer)
		assert.NoError(t, err)

		timers, err := s.FindTimersTo(t.Context(), timer.DueAt.Add(1*time.Second))
		assert.NoError(t, err)
		assert.Truef(t, slices.ContainsFunc(timers, timer.EqualTo), "expected to find timer in timers array: %+v", timers)

		timers, err = s.FindTokenActiveTimerSubscriptions(t.Context(), timer.Token.Key)
		assert.NoError(t, err)
		assert.Truef(t, slices.ContainsFunc(timers, timer.EqualTo), "expected to find timer in timers array: %+v", timers)
	}
}

func (st *StorageTester) TestJobStorageWriter(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {

		r := s.GenerateId()
		token := bpmnruntime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ProcessInstanceKey: st.processInstance.Key,
			State:              bpmnruntime.TokenStateWaiting,
		}
		s.SaveToken(t.Context(), token)

		job := getJob(r, st.processInstance.Key, token)

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
			ProcessInstanceKey: st.processInstance.Key,
			State:              bpmnruntime.TokenStateWaiting,
		}
		s.SaveToken(t.Context(), token)

		job := getJob(r, st.processInstance.Key, token)
		err := s.SaveJob(t.Context(), job)
		assert.NoError(t, err)

		jobs, err := s.FindPendingProcessInstanceJobs(t.Context(), st.processInstance.Key)
		assert.NoError(t, err)
		assert.Contains(t, jobs, job)

		storeJob, err := s.FindJobByJobKey(t.Context(), job.Key)
		assert.NoError(t, err)
		assert.Equal(t, job, storeJob)
		assert.NotEmpty(t, job.Type)

		storeJobs, err := s.FindTokenJobsInState(t.Context(), token.Key, []bpmnruntime.ActivityState{bpmnruntime.ActivityStateActive})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(storeJobs))
		assert.Equal(t, job, storeJobs[0])
		assert.NotEmpty(t, storeJobs[0].Type)
	}
}

func getMessage(r int64, piKey int64, pdKey int64, token bpmnruntime.ExecutionToken) bpmnruntime.MessageSubscription {
	return bpmnruntime.MessageSubscription{
		ElementId:            fmt.Sprintf("message-%d", r),
		Key:                  r + 400,
		ProcessDefinitionKey: pdKey,
		ProcessInstanceKey:   piKey,
		Name:                 fmt.Sprintf("message-%d", r),
		CorrelationKey:       fmt.Sprintf("correlation-%d", r),
		State:                bpmnruntime.ActivityStateActive,
		CreatedAt:            time.Now().Truncate(time.Millisecond),
		Token:                token,
	}
}

func (st *StorageTester) TestMessageStorageWriter(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {

		r := s.GenerateId()
		token := bpmnruntime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ProcessInstanceKey: st.processInstance.Key,
			State:              bpmnruntime.TokenStateWaiting,
		}
		s.SaveToken(t.Context(), token)

		job := getJob(r, st.processInstance.Key, token)
		err := s.SaveJob(t.Context(), job)
		assert.NoError(t, err)

		message := getMessage(r, st.processDefinition.Key, st.processInstance.Key, bpmnruntime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ElementId:          "messageElementId",
			ProcessInstanceKey: st.processInstance.Key,
			State:              bpmnruntime.TokenStateWaiting,
		})

		err = s.SaveMessageSubscription(t.Context(), message)
		assert.NoError(t, err)
	}
}

func (st *StorageTester) TestMessageStorageReader(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {

		r := s.GenerateId()

		token := bpmnruntime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ElementId:          "47b623dd-54ab-407e-86dc-847b62d22318",
			ProcessInstanceKey: st.processInstance.Key,
			State:              bpmnruntime.TokenStateWaiting,
		}
		s.SaveToken(t.Context(), token)

		messageSub := getMessage(r, st.processDefinition.Key, st.processInstance.Key, token)
		err := s.SaveMessageSubscription(t.Context(), messageSub)
		assert.NoError(t, err)

		messageSubs, err := s.FindProcessInstanceMessageSubscriptions(t.Context(), st.processInstance.Key, bpmnruntime.ActivityStateActive)
		assert.NoError(t, err)
		assert.Truef(t, slices.ContainsFunc(messageSubs, messageSub.EqualTo), "expected to find message subscription in message subscriptions array: %+v", messageSubs)

		messageSubs, err = s.FindTokenMessageSubscriptions(t.Context(), token.Key, bpmnruntime.ActivityStateActive)
		assert.NoError(t, err)
		assert.Truef(t, slices.ContainsFunc(messageSubs, messageSub.EqualTo), "expected to find message subscription in message subscriptions array: %+v", messageSubs)
	}
}

func (st *StorageTester) TestTokenStorageWriter(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {

		r := s.GenerateId()

		token1 := bpmnruntime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ElementId:          "test-elem",
			ProcessInstanceKey: st.processInstance.Key,
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
			ProcessInstanceKey: st.processInstance.Key,
			State:              bpmnruntime.TokenStateRunning,
		}
		err := s.SaveToken(t.Context(), token1)
		assert.NoError(t, err)

		r = s.GenerateId()
		token2 := bpmnruntime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ElementId:          "test-elem",
			ProcessInstanceKey: st.processInstance.Key,
			State:              bpmnruntime.TokenStateCompleted,
		}
		err = s.SaveToken(t.Context(), token2)
		assert.NoError(t, err)

		r = s.GenerateId()
		token3 := bpmnruntime.ExecutionToken{
			Key:                r,
			ElementInstanceKey: r,
			ElementId:          "test-elem",
			ProcessInstanceKey: st.processInstance.Key,
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

		tokens, err = s.GetAllTokensForProcessInstance(t.Context(), st.processInstance.Key)
		assert.NoError(t, err)
		matchedTwice := 0
		for _, tok := range tokens {
			if tok.ElementInstanceKey == token1.ElementInstanceKey || tok.ElementInstanceKey == token2.ElementInstanceKey {
				matchedTwice++
			}
		}
		assert.Equal(t, 2, matchedTwice, "expected to find created tokens among tokens for partition")

		tokens, err = s.GetActiveTokensForProcessInstance(t.Context(), st.processInstance.Key)
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

func (st *StorageTester) TestSaveFlowElementHistoryWriter(s storage.Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		r := s.GenerateId()

		historyItem := bpmnruntime.FlowElementHistoryItem{
			Key:                r,
			ProcessInstanceKey: r,
			ElementId:          "test-elem",
			CreatedAt:          time.Now().Truncate(time.Millisecond),
		}
		err := s.SaveFlowElementHistory(t.Context(), historyItem)
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
			ProcessInstanceKey: st.processInstance.Key,
			Message:            "test-message",
			Token: bpmnruntime.ExecutionToken{
				Key:                tok,
				ElementInstanceKey: tok,
				ElementId:          "test-elem",
				ProcessInstanceKey: st.processInstance.Key,
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
			ProcessInstanceKey: st.processInstance.Key,
			State:              bpmnruntime.TokenStateWaiting,
		}

		incident := bpmnruntime.Incident{
			Key:                r,
			ElementInstanceKey: r,
			ElementId:          "test-elem",
			ProcessInstanceKey: st.processInstance.Key,
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

		testIncidents, err := s.FindIncidentsByProcessInstanceKey(t.Context(), st.processInstance.Key)
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
