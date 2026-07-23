package bpmn

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/appcontext"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
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

			child := findChildCallActivityInstance(t, instance.ProcessInstance().Key)
			require.NotNil(t, child.ProcessInstance().BusinessKey)
			assert.Equal(t, test.expected, *child.ProcessInstance().BusinessKey)
		})
	}
}

func TestSubProcessBusinessKeyEvaluatesConfiguredExpression(t *testing.T) {
	processID := fmt.Sprintf("business-key-sub-process-%d", rand.Int63())
	process := loadBusinessKeySubProcess(t, processID, `<zenbpm:in businessKey="=processBusinessKey" />`)
	ctx := appcontext.WithBusinessKey(t.Context(), "parent-key")

	instance, err := bpmnEngine.CreateInstanceByKey(ctx, process.Key, map[string]interface{}{
		"processBusinessKey": "sub-process-key",
	})
	require.NoError(t, err)

	child := findChildSubProcessInstance(t, instance.ProcessInstance().Key)
	require.NotNil(t, child.ProcessInstance().BusinessKey)
	assert.Equal(t, "sub-process-key", *child.ProcessInstance().BusinessKey)
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

	multiInstance := findChildMultiInstanceInstance(t, instance.ProcessInstance().Key)
	child := findChildCallActivityInstance(t, multiInstance.ProcessInstance().Key)
	require.NotNil(t, child.ProcessInstance().BusinessKey)
	assert.Equal(t, "parent-key", *child.ProcessInstance().BusinessKey)
	waitForProcessCompletion(t, instance.ProcessInstance().Key, 2*time.Second, 50*time.Millisecond)
}

func TestCallActivityBusinessKeyRejectsNonStringResult(t *testing.T) {
	processID := fmt.Sprintf("business-key-non-string-%d", rand.Int63())
	loadBusinessKeyChildProcess(t, processID)
	parent := loadBusinessKeyCallActivityProcess(t, processID, `<zenbpm:in businessKey="=42" />`)

	_, err := bpmnEngine.CreateInstanceByKey(t.Context(), parent.Key, nil)
	require.Error(t, err)
	assert.ErrorContains(t, err, "business key expression must evaluate to a string")
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
