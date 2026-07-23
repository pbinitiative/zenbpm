package bpmn

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/appcontext"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCallActivityBusinessKey(t *testing.T) {
	tests := []struct {
		name             string
		businessKeyInput string
		variables        map[string]interface{}
		expected         string
	}{
		{
			name:     "inherits parent business key when input is absent",
			expected: "parent-key",
		},
		{
			name:             "evaluates configured FEEL expression",
			businessKeyInput: `<zenbpm:in businessKey="=processBusinessKey" />`,
			variables:        map[string]interface{}{"processBusinessKey": "child-key"},
			expected:         "child-key",
		},
		{
			name:             "clears business key when configured expression is empty",
			businessKeyInput: `<zenbpm:in businessKey="" />`,
			expected:         "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			processID := fmt.Sprintf("business-key-child-%d", rand.Int63())
			loadBusinessKeyChildProcess(t, processID)
			parent := loadBusinessKeyCallActivityProcess(t, processID, test.businessKeyInput)
			ctx := appcontext.WithBusinessKey(t.Context(), "parent-key")

			instance, err := bpmnEngine.CreateInstanceByKey(ctx, parent.Key, test.variables)
			require.NoError(t, err)
			require.NotNil(t, instance.ProcessInstance().BusinessKey)
			assert.Equal(t, "parent-key", *instance.ProcessInstance().BusinessKey)

			waitForBusinessKeyProcessCompletion(t, instance.ProcessInstance().Key)
			child := findChildCallActivityInstance(t, instance.ProcessInstance().Key)
			require.NotNil(t, child.ProcessInstance().BusinessKey)
			assert.Equal(t, test.expected, *child.ProcessInstance().BusinessKey)
		})
	}
}

func TestSubProcessBusinessKey(t *testing.T) {
	tests := []struct {
		name             string
		businessKeyInput string
		variables        map[string]interface{}
		expected         string
	}{
		{
			name:     "inherits parent business key when input is absent",
			expected: "parent-key",
		},
		{
			name:             "evaluates configured FEEL expression",
			businessKeyInput: `<zenbpm:in businessKey="=processBusinessKey" />`,
			variables:        map[string]interface{}{"processBusinessKey": "sub-process-key"},
			expected:         "sub-process-key",
		},
		{
			name:             "clears business key when configured expression is empty",
			businessKeyInput: `<zenbpm:in businessKey="" />`,
			expected:         "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			processID := fmt.Sprintf("business-key-sub-process-%d", rand.Int63())
			process := loadBusinessKeySubProcess(t, processID, test.businessKeyInput)
			ctx := appcontext.WithBusinessKey(t.Context(), "parent-key")

			instance, err := bpmnEngine.CreateInstanceByKey(ctx, process.Key, test.variables)
			require.NoError(t, err)

			waitForBusinessKeyProcessCompletion(t, instance.ProcessInstance().Key)
			child := findChildSubProcessInstance(t, instance.ProcessInstance().Key)
			require.NotNil(t, child.ProcessInstance().BusinessKey)
			assert.Equal(t, test.expected, *child.ProcessInstance().BusinessKey)
		})
	}
}

func TestEventSubProcessBusinessKey(t *testing.T) {
	tests := []struct {
		name             string
		businessKeyInput string
		expected         string
	}{
		{
			name:     "inherits parent business key when input is absent",
			expected: "parent-key",
		},
		{
			name:             "evaluates configured FEEL expression against start event output",
			businessKeyInput: `<zenbpm:in businessKey="=messageStartEventOutputVar" />`,
			expected:         "messageStartEventOutputValue",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := inmemory.NewStorage()
			engine := NewEngine(EngineWithStorage(store))
			require.NoError(t, engine.Start(t.Context()))
			defer engine.Stop()

			handler := engine.NewTaskHandler().
				Type("input-task-message-event-subprocess-interrupting").
				Handler(func(ActivatedJob) {})
			defer engine.RemoveHandler(handler)

			process := loadBusinessKeyEventSubProcess(t, &engine, test.businessKeyInput)
			ctx := appcontext.WithBusinessKey(t.Context(), "parent-key")
			parent, err := engine.CreateInstanceByKey(ctx, process.Key, nil)
			require.NoError(t, err)

			correlationKey := "correlation-key-event-subprocess-1"
			require.Eventually(t, func() bool {
				_, err := store.FindMessageSubscriptionByName(t.Context(), "globalMessageRef", &correlationKey, runtime.ActivityStateActive)
				return err == nil
			}, 2*time.Second, 25*time.Millisecond)
			require.NoError(t, engine.PublishMessageByName(t.Context(), "globalMessageRef", &correlationKey, nil))
			require.Eventually(t, func() bool {
				engine.runningInstances.lockInstance(parent.ProcessInstance().Key)
				defer engine.runningInstances.unlockInstance(parent.ProcessInstance().Key)

				instance, err := store.FindProcessInstanceByKey(t.Context(), parent.ProcessInstance().Key)
				return err == nil && instance.ProcessInstance().State == runtime.ActivityStateCompleted
			}, 2*time.Second, 25*time.Millisecond)

			var eventSubProcess runtime.ProcessInstance
			require.Eventually(t, func() bool {
				for _, instance := range store.ProcessInstancesSnapshot() {
					parentKey := instance.GetParentProcessInstanceKey()
					if parentKey != nil && *parentKey == parent.ProcessInstance().Key {
						eventSubProcess = instance
						return true
					}
				}
				return false
			}, 2*time.Second, 25*time.Millisecond)
			require.NotNil(t, eventSubProcess.ProcessInstance().BusinessKey)
			assert.Equal(t, test.expected, *eventSubProcess.ProcessInstance().BusinessKey)
		})
	}
}

func TestEventSubProcessBusinessKeyFailureDoesNotInterruptParent(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	require.NoError(t, engine.Start(t.Context()))
	defer engine.Stop()

	handler := engine.NewTaskHandler().
		Type("input-task-message-event-subprocess-interrupting").
		Handler(func(ActivatedJob) {})
	defer engine.RemoveHandler(handler)

	process := loadBusinessKeyEventSubProcess(t, &engine, `<zenbpm:in businessKey="=42" />`)
	ctx := appcontext.WithBusinessKey(t.Context(), "parent-key")
	parent, err := engine.CreateInstanceByKey(ctx, process.Key, nil)
	require.NoError(t, err)

	correlationKey := "correlation-key-event-subprocess-1"
	require.Eventually(t, func() bool {
		_, err := store.FindMessageSubscriptionByName(t.Context(), "globalMessageRef", &correlationKey, runtime.ActivityStateActive)
		return err == nil
	}, 2*time.Second, 25*time.Millisecond)

	err = engine.PublishMessageByName(t.Context(), "globalMessageRef", &correlationKey, nil)
	require.ErrorContains(t, err, "business key expression must evaluate to a string")

	_, err = store.FindMessageSubscriptionByName(t.Context(), "globalMessageRef", &correlationKey, runtime.ActivityStateActive)
	require.NoError(t, err, "failed override must leave the event subprocess trigger active")

	engine.runningInstances.lockInstance(parent.ProcessInstance().Key)
	persistedParent, err := store.FindProcessInstanceByKey(t.Context(), parent.ProcessInstance().Key)
	engine.runningInstances.unlockInstance(parent.ProcessInstance().Key)
	require.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateActive, persistedParent.ProcessInstance().State)

	for _, instance := range store.ProcessInstancesSnapshot() {
		parentKey := instance.GetParentProcessInstanceKey()
		assert.False(t, parentKey != nil && *parentKey == parent.ProcessInstance().Key,
			"failed override must not create an event subprocess instance")
	}
}

func TestAncestorErrorEventSubProcessBusinessKeyFailureDoesNotTerminateIntermediateScope(t *testing.T) {
	store := inmemory.NewStorage()
	engine := NewEngine(EngineWithStorage(store))
	require.NoError(t, engine.Start(t.Context()))
	defer engine.Stop()

	activatedJobs := make(chan ActivatedJob, 1)
	releaseJobs := make(chan struct{}, 1)
	defer close(releaseJobs)
	handler := engine.NewTaskHandler().
		Type("business-key-ancestor-error").
		Handler(func(job ActivatedJob) {
			activatedJobs <- job
			<-releaseJobs
			job.Complete()
		})
	defer engine.RemoveHandler(handler)

	processID := fmt.Sprintf("business-key-ancestor-error-%d", rand.Int63())
	xml := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zenbpm="http://zenbpm.pbinitiative.org/1.0" targetNamespace="http://zenbpm.pbinitiative.org/test">
  <bpmn:process id="%s" isExecutable="true">
    <bpmn:startEvent id="root-start"><bpmn:outgoing>to-outer</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="to-outer" sourceRef="root-start" targetRef="outer-sub" />
    <bpmn:subProcess id="outer-sub">
      <bpmn:outgoing>outer-to-end</bpmn:outgoing>
      <bpmn:startEvent id="outer-start"><bpmn:outgoing>to-inner</bpmn:outgoing></bpmn:startEvent>
      <bpmn:sequenceFlow id="to-inner" sourceRef="outer-start" targetRef="inner-sub" />
      <bpmn:subProcess id="inner-sub">
        <bpmn:outgoing>inner-to-end</bpmn:outgoing>
        <bpmn:startEvent id="inner-start"><bpmn:outgoing>to-error-task</bpmn:outgoing></bpmn:startEvent>
        <bpmn:sequenceFlow id="to-error-task" sourceRef="inner-start" targetRef="error-task" />
        <bpmn:serviceTask id="error-task">
          <bpmn:extensionElements><zenbpm:taskDefinition type="business-key-ancestor-error" /></bpmn:extensionElements>
          <bpmn:incoming>to-error-task</bpmn:incoming><bpmn:outgoing>to-error-end</bpmn:outgoing>
        </bpmn:serviceTask>
        <bpmn:sequenceFlow id="to-error-end" sourceRef="error-task" targetRef="error-end" />
        <bpmn:endEvent id="error-end"><bpmn:incoming>to-error-end</bpmn:incoming><bpmn:errorEventDefinition id="throw-error-def" errorRef="test-error" /></bpmn:endEvent>
      </bpmn:subProcess>
      <bpmn:sequenceFlow id="inner-to-end" sourceRef="inner-sub" targetRef="outer-end" />
      <bpmn:endEvent id="outer-end" />
    </bpmn:subProcess>
    <bpmn:sequenceFlow id="outer-to-end" sourceRef="outer-sub" targetRef="root-end" />
    <bpmn:endEvent id="root-end" />
    <bpmn:subProcess id="error-event-subprocess" triggeredByEvent="true">
      <bpmn:extensionElements><zenbpm:in businessKey="=42" /></bpmn:extensionElements>
      <bpmn:startEvent id="error-start"><bpmn:outgoing>error-to-end</bpmn:outgoing><bpmn:errorEventDefinition id="catch-error-def" errorRef="test-error" /></bpmn:startEvent>
      <bpmn:sequenceFlow id="error-to-end" sourceRef="error-start" targetRef="handled-end" />
      <bpmn:endEvent id="handled-end" />
    </bpmn:subProcess>
  </bpmn:process>
  <bpmn:error id="test-error" errorCode="42" />
</bpmn:definitions>`, processID)
	process, err := engine.LoadFromBytes(t.Context(), []byte(xml), rand.Int63())
	require.NoError(t, err)
	_, err = engine.CreateInstanceByKey(t.Context(), process.Key, nil)
	require.NoError(t, err)

	var job ActivatedJob
	select {
	case job = <-activatedJobs:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "inner subprocess job was not activated")
	}
	innerKey := job.ProcessInstanceKey()
	activatedJobData, ok := job.(*activatedJob)
	require.True(t, ok)
	parentKey := activatedJobData.processInstanceInfo.GetParentProcessInstanceKey()
	require.NotNil(t, parentKey)
	outerKey := *parentKey

	releaseJobs <- struct{}{}
	require.Eventually(t, func() bool {
		engine.runningInstances.lockInstance(innerKey)
		defer engine.runningInstances.unlockInstance(innerKey)
		instance, err := store.FindProcessInstanceByKey(t.Context(), innerKey)
		return err == nil && instance.ProcessInstance().State == runtime.ActivityStateFailed
	}, 2*time.Second, 25*time.Millisecond)

	engine.runningInstances.lockInstance(outerKey)
	persistedOuter, err := store.FindProcessInstanceByKey(t.Context(), outerKey)
	engine.runningInstances.unlockInstance(outerKey)
	require.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateActive, persistedOuter.ProcessInstance().State,
		"invalid ancestor event-subprocess override must be validated before terminating intermediate scopes")
}

func loadBusinessKeyEventSubProcess(t *testing.T, engine *Engine, businessKeyInput string) *runtime.ProcessDefinition {
	t.Helper()
	xml, err := os.ReadFile("./test-cases/message_event_subprocess/message-event-subprocess-interrupting.bpmn")
	require.NoError(t, err)
	if businessKeyInput != "" {
		const eventSubProcessExtension = `<bpmn:subProcess id="Activity_0adcic4" triggeredByEvent="true">
      <bpmn:extensionElements>`
		xml = []byte(strings.Replace(
			string(xml),
			eventSubProcessExtension,
			eventSubProcessExtension+"\n        "+businessKeyInput,
			1,
		))
		require.Contains(t, string(xml), businessKeyInput)
	}

	process, err := engine.LoadFromBytes(t.Context(), xml, rand.Int63())
	require.NoError(t, err)
	return process
}

func TestMultiInstanceCallActivityInheritsBusinessKey(t *testing.T) {
	processID := fmt.Sprintf("business-key-multi-child-%d", rand.Int63())
	loadBusinessKeyChildProcess(t, processID)
	process := loadBusinessKeyMultiInstanceCallActivityProcess(t, processID)
	ctx := appcontext.WithBusinessKey(t.Context(), "parent-key")

	instance, err := bpmnEngine.CreateInstanceByKey(ctx, process.Key, map[string]interface{}{
		"items": []interface{}{"item"},
	})
	require.NoError(t, err)

	waitForBusinessKeyProcessCompletion(t, instance.ProcessInstance().Key)
	multiInstance := findChildMultiInstanceInstance(t, instance.ProcessInstance().Key)
	child := findChildCallActivityInstance(t, multiInstance.ProcessInstance().Key)
	require.NotNil(t, child.ProcessInstance().BusinessKey)
	assert.Equal(t, "parent-key", *child.ProcessInstance().BusinessKey)
}

// waitForBusinessKeyProcessCompletion reads the process state while holding the same
// per-instance lock used by engine execution, so aliased in-memory instances are not
// inspected while asynchronous child continuation is mutating them.
func waitForBusinessKeyProcessCompletion(t *testing.T, instanceKey int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		bpmnEngine.runningInstances.lockInstance(instanceKey)
		defer bpmnEngine.runningInstances.unlockInstance(instanceKey)

		instance, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instanceKey)
		return err == nil && instance.ProcessInstance().State == runtime.ActivityStateCompleted
	}, 2*time.Second, 50*time.Millisecond)
}

func TestChildBusinessKeyExpressionFailuresCreateIncident(t *testing.T) {
	activityTypes := []struct {
		name      string
		elementID string
		load      func(t *testing.T, processID, businessKeyInput string) *runtime.ProcessDefinition
	}{
		{
			name:      "CallActivity",
			elementID: "call",
			load: func(t *testing.T, processID, businessKeyInput string) *runtime.ProcessDefinition {
				loadBusinessKeyChildProcess(t, processID)
				return loadBusinessKeyCallActivityProcess(t, processID, businessKeyInput)
			},
		},
		{
			name:      "SubProcess",
			elementID: "sub-process",
			load:      loadBusinessKeySubProcess,
		},
	}
	failures := []struct {
		name             string
		businessKeyInput string
		variables        map[string]interface{}
		expectedError    string
		forbiddenError   string
	}{
		{
			name:             "missing leading equals",
			businessKeyInput: `<zenbpm:in businessKey="literal-key" />`,
			expectedError:    "business key expression must start with '='",
		},
		{
			name:             "invalid FEEL syntax",
			businessKeyInput: `<zenbpm:in businessKey="=1 +" />`,
			expectedError:    "failed to evaluate business key expression",
		},
		{
			name:             "undefined FEEL result",
			businessKeyInput: `<zenbpm:in businessKey="=missingBusinessKey" />`,
			expectedError:    "business key expression must evaluate to a string",
		},
		{
			name:             "non-string FEEL result",
			businessKeyInput: `<zenbpm:in businessKey="=42" />`,
			expectedError:    "business key expression must evaluate to a string",
		},
		{
			name:             "non-string result does not expose process variables",
			businessKeyInput: `<zenbpm:in businessKey="=sensitiveBusinessKey" />`,
			variables: map[string]interface{}{
				"sensitiveBusinessKey": map[string]interface{}{"secret": "do-not-expose"},
			},
			expectedError:  "business key expression must evaluate to a string, got map[string]interface {}",
			forbiddenError: "do-not-expose",
		},
	}

	for _, activityType := range activityTypes {
		for _, failure := range failures {
			t.Run(activityType.name+"/"+failure.name, func(t *testing.T) {
				processID := fmt.Sprintf("business-key-failure-%d", rand.Int63())
				process := activityType.load(t, processID, failure.businessKeyInput)

				instance, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, failure.variables)
				require.Error(t, err)
				assert.ErrorContains(t, err, failure.expectedError)
				if failure.forbiddenError != "" {
					assert.NotContains(t, err.Error(), failure.forbiddenError)
				}

				persisted, err := bpmnEngine.persistence.FindProcessInstanceByKey(t.Context(), instance.ProcessInstance().Key)
				require.NoError(t, err)
				assert.Equal(t, runtime.ActivityStateFailed, persisted.ProcessInstance().State)

				incidents, err := bpmnEngine.persistence.FindIncidentsByProcessInstanceKey(t.Context(), instance.ProcessInstance().Key)
				require.NoError(t, err)
				require.Len(t, incidents, 1)
				assert.Equal(t, activityType.elementID, incidents[0].ElementId)
				assert.Equal(t, runtime.TokenStateFailed, incidents[0].Token.State)
				assert.Contains(t, incidents[0].Message, failure.expectedError)
				if failure.forbiddenError != "" {
					assert.NotContains(t, incidents[0].Message, failure.forbiddenError)
				}

				assertNoChildBusinessKeyInstance(t, instance.ProcessInstance().Key)
			})
		}
	}
}

func assertNoChildBusinessKeyInstance(t *testing.T, parentInstanceKey int64) {
	t.Helper()
	tokens, err := bpmnEngine.persistence.GetAllTokensForProcessInstance(t.Context(), parentInstanceKey)
	require.NoError(t, err)
	require.NotEmpty(t, tokens)
	for _, token := range tokens {
		children, err := bpmnEngine.persistence.FindProcessInstancesByParentExecutionTokenKey(t.Context(), token.Key)
		require.NoError(t, err)
		assert.Empty(t, children, "failed business key expression must not create a child process instance")
	}
}

func loadBusinessKeyChildProcess(t *testing.T, processID string) {
	t.Helper()
	xml := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" targetNamespace="http://zenbpm.pbinitiative.org/test">
  <bpmn:process id="%s" isExecutable="true">
    <bpmn:startEvent id="child-start">
      <bpmn:outgoing>child-flow</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="child-flow" sourceRef="child-start" targetRef="child-end" />
    <bpmn:endEvent id="child-end" />
  </bpmn:process>
</bpmn:definitions>`, processID)
	_, err := bpmnEngine.LoadFromBytes(t.Context(), []byte(xml), rand.Int63())
	require.NoError(t, err)
}

func loadBusinessKeyCallActivityProcess(t *testing.T, calledProcessID, businessKeyInput string) *runtime.ProcessDefinition {
	t.Helper()
	processID := fmt.Sprintf("business-key-call-parent-%d", rand.Int63())
	xml := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zenbpm="http://zenbpm.pbinitiative.org/1.0" targetNamespace="http://zenbpm.pbinitiative.org/test">
  <bpmn:process id="%s" isExecutable="true">
    <bpmn:startEvent id="parent-start">
      <bpmn:outgoing>to-call</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="to-call" sourceRef="parent-start" targetRef="call" />
    <bpmn:callActivity id="call">
      <bpmn:outgoing>to-end</bpmn:outgoing>
      <bpmn:extensionElements>
        <zenbpm:calledElement processId="%s" />
        %s
      </bpmn:extensionElements>
    </bpmn:callActivity>
    <bpmn:sequenceFlow id="to-end" sourceRef="call" targetRef="parent-end" />
    <bpmn:endEvent id="parent-end" />
  </bpmn:process>
</bpmn:definitions>`, processID, calledProcessID, businessKeyInput)
	process, err := bpmnEngine.LoadFromBytes(t.Context(), []byte(xml), rand.Int63())
	require.NoError(t, err)
	return process
}

func loadBusinessKeyMultiInstanceCallActivityProcess(t *testing.T, calledProcessID string) *runtime.ProcessDefinition {
	t.Helper()
	processID := fmt.Sprintf("business-key-multi-parent-%d", rand.Int63())
	xml := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zenbpm="http://zenbpm.pbinitiative.org/1.0" targetNamespace="http://zenbpm.pbinitiative.org/test">
  <bpmn:process id="%s" isExecutable="true">
    <bpmn:startEvent id="parent-start">
      <bpmn:outgoing>to-call</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="to-call" sourceRef="parent-start" targetRef="call" />
    <bpmn:callActivity id="call">
      <bpmn:incoming>to-call</bpmn:incoming>
      <bpmn:outgoing>to-end</bpmn:outgoing>
      <bpmn:extensionElements>
        <zenbpm:calledElement processId="%s" />
      </bpmn:extensionElements>
      <bpmn:multiInstanceLoopCharacteristics isSequential="true">
        <bpmn:extensionElements>
          <zenbpm:loopCharacteristics inputCollection="=items" inputElement="item" outputCollection="results" outputElement="=item" />
        </bpmn:extensionElements>
      </bpmn:multiInstanceLoopCharacteristics>
    </bpmn:callActivity>
    <bpmn:sequenceFlow id="to-end" sourceRef="call" targetRef="parent-end" />
    <bpmn:endEvent id="parent-end">
      <bpmn:incoming>to-end</bpmn:incoming>
    </bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>`, processID, calledProcessID)
	process, err := bpmnEngine.LoadFromBytes(t.Context(), []byte(xml), rand.Int63())
	require.NoError(t, err)
	return process
}

func loadBusinessKeySubProcess(t *testing.T, processID, businessKeyInput string) *runtime.ProcessDefinition {
	t.Helper()
	xml := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zenbpm="http://zenbpm.pbinitiative.org/1.0" targetNamespace="http://zenbpm.pbinitiative.org/test">
  <bpmn:process id="%s" isExecutable="true">
    <bpmn:startEvent id="parent-start">
      <bpmn:outgoing>to-sub-process</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="to-sub-process" sourceRef="parent-start" targetRef="sub-process" />
    <bpmn:subProcess id="sub-process">
      <bpmn:outgoing>to-end</bpmn:outgoing>
      <bpmn:extensionElements>%s</bpmn:extensionElements>
      <bpmn:startEvent id="sub-process-start">
        <bpmn:outgoing>sub-process-flow</bpmn:outgoing>
      </bpmn:startEvent>
      <bpmn:sequenceFlow id="sub-process-flow" sourceRef="sub-process-start" targetRef="sub-process-end" />
      <bpmn:endEvent id="sub-process-end" />
    </bpmn:subProcess>
    <bpmn:sequenceFlow id="to-end" sourceRef="sub-process" targetRef="parent-end" />
    <bpmn:endEvent id="parent-end" />
  </bpmn:process>
</bpmn:definitions>`, processID, businessKeyInput)
	process, err := bpmnEngine.LoadFromBytes(t.Context(), []byte(xml), rand.Int63())
	require.NoError(t, err)
	return process
}
