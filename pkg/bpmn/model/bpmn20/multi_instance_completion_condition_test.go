package bpmn20

import (
	"encoding/xml"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultiInstanceCompletionConditionIsRefusedForDeployment(t *testing.T) {
	testCases := []struct {
		name            string
		process         string
		expectedElement string
	}{
		{
			name:            "service task",
			process:         multiInstanceActivityXML("serviceTask", "mi-activity", `<bpmn:completionCondition xsi:type="bpmn:tFormalExpression">=true</bpmn:completionCondition>`, ""),
			expectedElement: `serviceTask id="mi-activity"`,
		},
		{
			name:            "user task",
			process:         multiInstanceActivityXML("userTask", "mi-activity", `<bpmn:completionCondition xsi:type="bpmn:tFormalExpression">=true</bpmn:completionCondition>`, ""),
			expectedElement: `userTask id="mi-activity"`,
		},
		{
			name: "sub process",
			process: multiInstanceActivityXML("subProcess", "mi-activity", `<bpmn:completionCondition xsi:type="bpmn:tFormalExpression">=true</bpmn:completionCondition>`,
				`<bpmn:startEvent id="inner-start" />`),
			expectedElement: `subProcess id="mi-activity"`,
		},
		{
			name: "activity nested in a sub process",
			process: `<bpmn:subProcess id="outer-sub-process">
      <bpmn:startEvent id="inner-start" />
      ` + multiInstanceActivityXML("sendTask", "nested-mi-activity", `<bpmn:completionCondition xsi:type="bpmn:tFormalExpression">=true</bpmn:completionCondition>`, "") + `
    </bpmn:subProcess>`,
			expectedElement: `sendTask id="nested-mi-activity"`,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			var definitions TDefinitions
			require.NoError(t, xml.Unmarshal([]byte(processDefinitionXML(testCase.process)), &definitions),
				"a stored definition carrying a completionCondition must keep parsing")

			err := definitions.ValidateForDeployment()

			require.Error(t, err, "a new deployment declaring a completionCondition must be refused")
			assert.Contains(t, err.Error(), "unsupported element configuration")
			assert.Contains(t, err.Error(), testCase.expectedElement)
			assert.Contains(t, err.Error(), "completionCondition")
		})
	}

	t.Run("activity without completion condition is accepted", func(t *testing.T) {
		var definitions TDefinitions
		require.NoError(t, xml.Unmarshal([]byte(processDefinitionXML(multiInstanceActivityXML("serviceTask", "mi-activity", "", ""))), &definitions))

		assert.NoError(t, definitions.ValidateForDeployment())
	})

	t.Run("blank completion condition is accepted", func(t *testing.T) {
		var definitions TDefinitions
		require.NoError(t, xml.Unmarshal([]byte(processDefinitionXML(multiInstanceActivityXML("serviceTask", "mi-activity",
			`<bpmn:completionCondition xsi:type="bpmn:tFormalExpression">  </bpmn:completionCondition>`, ""))), &definitions))

		assert.NoError(t, definitions.ValidateForDeployment())
	})
}

func processDefinitionXML(process string) string {
	return fmt.Sprintf(`<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:zenbpm="http://zenbpm.pbinitiative.org/1.0" id="Definitions_completion_condition">
  <bpmn:process id="completion-condition" isExecutable="true">
    %s
  </bpmn:process>
</bpmn:definitions>`, process)
}

func multiInstanceActivityXML(elementName string, id string, completionCondition string, content string) string {
	return fmt.Sprintf(`<bpmn:%[1]s id="%[2]s">
      <bpmn:multiInstanceLoopCharacteristics>
        <bpmn:extensionElements>
          <zenbpm:loopCharacteristics inputCollection="=items" inputElement="item" />
        </bpmn:extensionElements>
        %[3]s
      </bpmn:multiInstanceLoopCharacteristics>
      %[4]s
    </bpmn:%[1]s>`, elementName, id, completionCondition, content)
}
