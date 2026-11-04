package bpmn20

import (
	"encoding/xml"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManualTaskIsParsedWithItsSequenceFlows(t *testing.T) {
	definitions := unmarshalProcess(t, `<bpmn:startEvent id="start">
      <bpmn:outgoing>flow-in</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="flow-in" sourceRef="start" targetRef="manual-task" />
    <bpmn:manualTask id="manual-task" name="Sign the contract">
      <bpmn:incoming>flow-in</bpmn:incoming>
      <bpmn:outgoing>flow-out</bpmn:outgoing>
    </bpmn:manualTask>
    <bpmn:sequenceFlow id="flow-out" sourceRef="manual-task" targetRef="end" />
    <bpmn:endEvent id="end">
      <bpmn:incoming>flow-out</bpmn:incoming>
    </bpmn:endEvent>`)

	manualTask := requireManualTask(t, definitions.Process.GetFlowNodeById("manual-task"))
	assert.Equal(t, ElementTypeManualTask, manualTask.GetType())
	assert.Equal(t, "Sign the contract", manualTask.GetName())
	require.Len(t, manualTask.GetIncomingAssociation(), 1)
	assert.Equal(t, "start", manualTask.GetIncomingAssociation()[0].GetSourceRef().GetId())
	require.Len(t, manualTask.GetOutgoingAssociation(), 1)
	assert.Equal(t, "end", manualTask.GetOutgoingAssociation()[0].GetTargetRef().GetId())
	assert.NoError(t, definitions.ValidateForDeployment())
}

func TestManualTaskNestedInSubProcessIsIndexed(t *testing.T) {
	definitions := unmarshalProcess(t, `<bpmn:subProcess id="sub-process">
      <bpmn:startEvent id="inner-start" />
      <bpmn:manualTask id="nested-manual-task" />
    </bpmn:subProcess>`)

	subProcess, ok := definitions.Process.GetFlowNodeById("sub-process").(*TSubProcess)
	require.True(t, ok)
	requireManualTask(t, subProcess.GetFlowNodeById("nested-manual-task"))
	assert.NoError(t, definitions.ValidateForDeployment())
}

func TestManualTaskWithInputAndOutputMappingsIsAcceptedForDeployment(t *testing.T) {
	definitions := unmarshalProcess(t, `<bpmn:manualTask id="manual-task">
      <bpmn:extensionElements>
        <zenbpm:ioMapping>
          <zenbpm:input source="=amount" target="localAmount" />
          <zenbpm:output source="=localAmount" target="approvedAmount" />
        </zenbpm:ioMapping>
      </bpmn:extensionElements>
    </bpmn:manualTask>`)

	manualTask := requireManualTask(t, definitions.Process.GetFlowNodeById("manual-task"))
	assert.Equal(t, []extensions.TIoMapping{{Source: "=amount", Target: "localAmount"}}, manualTask.GetInputMapping())
	assert.Equal(t, []extensions.TIoMapping{{Source: "=localAmount", Target: "approvedAmount"}}, manualTask.GetOutputMapping())
	assert.NoError(t, definitions.ValidateForDeployment())
}

func TestMultiInstanceManualTaskIsAcceptedForDeployment(t *testing.T) {
	definitions := unmarshalProcess(t, multiInstanceActivityXML("manualTask", "manual-task", "", ""))

	manualTask := requireManualTask(t, definitions.Process.GetFlowNodeById("manual-task"))
	require.NotNil(t, manualTask.GetMultiInstance())
	assert.Equal(t, "=items", manualTask.GetMultiInstance().LoopCharacteristics.InputCollectionExpression)
	assert.NoError(t, definitions.ValidateForDeployment())
}

// unmarshalProcess parses process content wrapped in a definitions element. It
// does not run the deployment checks, so a test can assert on them separately.
func unmarshalProcess(t *testing.T, process string) TDefinitions {
	t.Helper()
	var definitions TDefinitions
	require.NoError(t, xml.Unmarshal([]byte(processDefinitionXML(process)), &definitions))
	return definitions
}

func requireManualTask(t *testing.T, flowNode FlowNode) *TManualTask {
	t.Helper()
	manualTask, ok := flowNode.(*TManualTask)
	require.True(t, ok, "expected a manual task, got %T", flowNode)
	return manualTask
}
