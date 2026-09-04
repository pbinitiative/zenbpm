package e2e

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCallActivityProcessVersionSelection(t *testing.T) {
	uniqueSuffix := time.Now().UnixNano()
	childProcessID := fmt.Sprintf("call-activity-version-child-%d", uniqueSuffix)
	versionOneKey := deployCallActivityVersionBPMN(t, "call-activity-version-child-v1.bpmn", callActivityVersionChildBPMNForE2E(childProcessID, "version-one-task"))
	versionTwoKey := deployCallActivityVersionBPMN(t, "call-activity-version-child-v2.bpmn", callActivityVersionChildBPMNForE2E(childProcessID, "version-two-task"))
	require.NotEqual(t, versionOneKey, versionTwoKey)

	tests := []struct {
		name                    string
		versionAttribute        string
		expectedChildDefinition int64
		expectedVersion         int
		expectedVersionTag      string
	}{
		{
			name:                    "explicit numeric version",
			versionAttribute:        ` version="1"`,
			expectedChildDefinition: versionOneKey,
			expectedVersion:         1,
			expectedVersionTag:      "VersionTagversion-one-task",
		},
		{
			name:                    "version tag binding selects stored version tag",
			versionAttribute:        ` bindingType="versionTag" versionTag="VersionTagversion-two-task"`,
			expectedChildDefinition: versionTwoKey,
			expectedVersion:         2,
			expectedVersionTag:      "VersionTagversion-two-task",
		},
		{
			name:                    "v-number version tag falls back to numeric version",
			versionAttribute:        ` bindingType="versionTag" versionTag="v1"`,
			expectedChildDefinition: versionOneKey,
			expectedVersion:         1,
			expectedVersionTag:      "VersionTagversion-one-task",
		},
		{
			name:                    "latest version by default",
			expectedChildDefinition: versionTwoKey,
			expectedVersion:         2,
			expectedVersionTag:      "VersionTagversion-two-task",
		},
	}

	for index, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parentProcessID := fmt.Sprintf("call-activity-version-parent-%d-%d", uniqueSuffix, index)
			parentKey := deployCallActivityVersionBPMN(t, fmt.Sprintf("%s.bpmn", parentProcessID), callActivityVersionParentBPMNForE2E(parentProcessID, childProcessID, tt.versionAttribute))
			parentInstance, err := createProcessInstance(t, &parentKey, nil)
			require.NoError(t, err)
			assert.Equal(t, 1, parentInstance.Version)
			assert.Nil(t, parentInstance.VersionTag)
			t.Cleanup(func() { cleanupOwnedProcessInstance(t, parentInstance.Key) })

			childInstance := waitForChildProcessInstance(t, parentInstance.Key, 0)
			assert.Equal(t, tt.expectedChildDefinition, childInstance.ProcessDefinitionKey)
			assert.Equal(t, tt.expectedVersion, childInstance.Version)
			require.NotNil(t, childInstance.VersionTag)
			assert.Equal(t, tt.expectedVersionTag, *childInstance.VersionTag)

			fetchedChildInstance, err := getProcessInstance(t, childInstance.Key)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedVersion, fetchedChildInstance.Version)
			require.NotNil(t, fetchedChildInstance.VersionTag)
			assert.Equal(t, tt.expectedVersionTag, *fetchedChildInstance.VersionTag)
		})
	}

	failureTests := []struct {
		name                  string
		versionAttribute      string
		expectedMessageSubstr string
	}{
		{
			name:                  "non-existing explicit numeric version creates an incident",
			versionAttribute:      ` version="3"`,
			expectedMessageSubstr: "version=3",
		},
		{
			name:                  "non-existing version tag creates an incident",
			versionAttribute:      ` bindingType="versionTag" versionTag="VersionTagMissing"`,
			expectedMessageSubstr: "VersionTagMissing",
		},
		{
			name:                  "non-existing v-number tag and numeric version create an incident",
			versionAttribute:      ` bindingType="versionTag" versionTag="v3"`,
			expectedMessageSubstr: "numeric version=3",
		},
	}

	for index, tt := range failureTests {
		t.Run(tt.name, func(t *testing.T) {
			parentProcessID := fmt.Sprintf("call-activity-missing-version-parent-%d-%d", uniqueSuffix, index)
			parentKey := deployCallActivityVersionBPMN(t, fmt.Sprintf("%s.bpmn", parentProcessID), callActivityVersionParentBPMNForE2E(parentProcessID, childProcessID, tt.versionAttribute))
			parentInstance, err := createProcessInstance(t, &parentKey, nil)
			require.NoError(t, err)
			t.Cleanup(func() { cleanupOwnedProcessInstance(t, parentInstance.Key) })

			waitForProcessInstanceState(t, parentInstance.Key, zenclient.ProcessInstanceStateFailed)
			require.EventuallyWithT(t, func(collect *assert.CollectT) {
				incidents, findErr := getProcessInstanceIncidents(t, parentInstance.Key)
				if !assert.NoError(collect, findErr) || !assert.Len(collect, incidents, 1) {
					return
				}
				assert.Equal(collect, "call-activity", incidents[0].ElementId)
				assert.Contains(collect, incidents[0].Message, childProcessID)
				assert.Contains(collect, incidents[0].Message, tt.expectedMessageSubstr)
			}, 10*time.Second, 100*time.Millisecond)

			children, err := getChildInstances(t, parentInstance.Key)
			require.NoError(t, err)
			assert.Zero(t, children.TotalCount)
		})
	}
}

func deployCallActivityVersionBPMN(t testing.TB, filename string, bpmn string) int64 {
	t.Helper()
	response := deployProcessDefinitionContent(t, filename, []byte(bpmn))
	if response.JSON201 != nil {
		return response.JSON201.ProcessDefinitionKey
	}
	if response.JSON200 != nil {
		return response.JSON200.ProcessDefinitionKey
	}
	t.Fatalf("deployment %s did not return a process definition key", filename)
	return 0
}

func callActivityVersionChildBPMNForE2E(processID string, taskID string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zenbpm="http://zenbpm.pbinitiative.org/1.0">
  <bpmn:process id="%s" isExecutable="true">
    <bpmn:extensionElements><zenbpm:versionTag value="VersionTag%s" /></bpmn:extensionElements>
    <bpmn:startEvent id="start"><bpmn:outgoing>to-task</bpmn:outgoing></bpmn:startEvent>
    <bpmn:serviceTask id="%s">
      <bpmn:extensionElements><zenbpm:taskDefinition type="call-activity-version-e2e" /></bpmn:extensionElements>
      <bpmn:incoming>to-task</bpmn:incoming>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="to-task" sourceRef="start" targetRef="%s" />
  </bpmn:process>
</bpmn:definitions>`, processID, taskID, taskID, taskID)
}

func callActivityVersionParentBPMNForE2E(processID string, calledProcessID string, versionAttribute string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zenbpm="http://zenbpm.pbinitiative.org/1.0">
  <bpmn:process id="%s" isExecutable="true">
    <bpmn:startEvent id="start"><bpmn:outgoing>to-call</bpmn:outgoing></bpmn:startEvent>
    <bpmn:callActivity id="call-activity">
      <bpmn:extensionElements><zenbpm:calledElement processId="%s"%s /></bpmn:extensionElements>
      <bpmn:incoming>to-call</bpmn:incoming>
      <bpmn:outgoing>to-end</bpmn:outgoing>
    </bpmn:callActivity>
    <bpmn:endEvent id="end"><bpmn:incoming>to-end</bpmn:incoming></bpmn:endEvent>
    <bpmn:sequenceFlow id="to-call" sourceRef="start" targetRef="call-activity" />
    <bpmn:sequenceFlow id="to-end" sourceRef="call-activity" targetRef="end" />
  </bpmn:process>
</bpmn:definitions>`, processID, calledProcessID, versionAttribute)
}

func TestCallActivity(t *testing.T) {
	var instance zenclient.ProcessInstance
	definition, err := deployGetDefinition(t, "call-activity/call-activity-with-simple-subprocess.bpmn", "Simple_CallActivity_Process")
	assert.NoError(t, err)

	_, err = deployGetDefinition(t, "simple-simple-sub-process.bpmn", "empty-sub-process")
	assert.NoError(t, err)

	instance, err = createProcessInstance(t, &definition.Key, map[string]any{
		"testVar": 123,
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, instance.Key)

	assert.Eventually(t, func() bool {
		resp, err := app.restClient.GetChildProcessInstancesWithResponse(t.Context(), instance.Key, &zenclient.GetChildProcessInstancesParams{})
		if err != nil || resp.HTTPResponse.StatusCode != http.StatusOK {
			return false
		}
		if len(resp.JSON200.Partitions[0].Items) != 1 {
			return false
		}

		return resp.JSON200.Partitions[0].Items[0].State == "completed"
	}, 10*time.Second, 1*time.Second, "job should have failed")
}
