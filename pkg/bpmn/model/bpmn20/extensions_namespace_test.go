package bpmn20

import (
	"encoding/xml"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	zenbpmNS = "http://zenbpm.pbinitiative.org/1.0"
)

func TestUnmarshalZenbpmExtensions_CanonicalNamespace(t *testing.T) {
	zenbpmXML := `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zenbpm="` + zenbpmNS + `">
  <bpmn:process id="canonical" isExecutable="true">
    <bpmn:serviceTask id="task1" name="t1">
      <bpmn:extensionElements>
        <zenbpm:taskDefinition type="myType" retries="3" />
        <zenbpm:ioMapping>
          <zenbpm:input source="=1" target="in" />
          <zenbpm:output source="=out" target="outVar" />
        </zenbpm:ioMapping>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
  </bpmn:process>
</bpmn:definitions>`

	var def TDefinitions
	err := xml.Unmarshal([]byte(zenbpmXML), &def)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(def.Process.ServiceTasks))
	task := def.Process.ServiceTasks[0]
	assert.Equal(t, "myType", task.TaskDefinition.TypeName)
	assert.Equal(t, "3", task.TaskDefinition.Retries)
	assert.Equal(t, "in", task.GetInputMapping()[0].Target)
	assert.Equal(t, "outVar", task.GetOutputMapping()[0].Target)
}

func TestUnmarshalZenbpmExtensions_ArbitraryPrefix(t *testing.T) {
	// Go's encoding/xml matches by local element name, not prefix.
	// Any prefix that maps to our canonical URI should unmarshal correctly.
	customXML := `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:foo="` + zenbpmNS + `">
  <bpmn:process id="custom-prefix" isExecutable="true">
    <bpmn:serviceTask id="task1" name="t1">
      <bpmn:extensionElements>
        <foo:taskDefinition type="customType" />
        <foo:ioMapping>
          <foo:input source="=src" target="dst" />
        </foo:ioMapping>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
  </bpmn:process>
</bpmn:definitions>`

	var def TDefinitions
	err := xml.Unmarshal([]byte(customXML), &def)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(def.Process.ServiceTasks))
	task := def.Process.ServiceTasks[0]
	assert.Equal(t, "customType", task.TaskDefinition.TypeName)
	assert.Equal(t, "dst", task.GetInputMapping()[0].Target)
}

func TestUnmarshalZenbpmExtensions_ZeebeBackwardsCompat(t *testing.T) {
	// During migration, old files may still use xmlns:zeebe with the Camunda URI.
	// Go matches by local element name, so this must continue to unmarshal.
	oldXML := `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0">
  <bpmn:process id="legacy" isExecutable="true">
    <bpmn:serviceTask id="task1" name="t1">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="legacyType" />
        <zeebe:ioMapping>
          <zeebe:input source="=old" target="oldIn" />
          <zeebe:output source="=oldOut" target="oldOutVar" />
        </zeebe:ioMapping>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
  </bpmn:process>
</bpmn:definitions>`

	var def TDefinitions
	err := xml.Unmarshal([]byte(oldXML), &def)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(def.Process.ServiceTasks))
	task := def.Process.ServiceTasks[0]
	assert.Equal(t, "legacyType", task.TaskDefinition.TypeName)
	assert.Equal(t, "oldIn", task.GetInputMapping()[0].Target)
	assert.Equal(t, "oldOutVar", task.GetOutputMapping()[0].Target)
}

func TestUnmarshalZenbpmExtensions_BusinessKeyInput(t *testing.T) {
	businessKeyXML := `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:custom="` + zenbpmNS + `">
  <bpmn:process id="business-key" isExecutable="true">
    <bpmn:callActivity id="configured-call">
      <bpmn:extensionElements>
        <custom:in businessKey="=processBusinessKey" />
      </bpmn:extensionElements>
    </bpmn:callActivity>
    <bpmn:callActivity id="inherited-call" />
    <bpmn:subProcess id="cleared-sub-process">
      <bpmn:extensionElements>
        <custom:in businessKey="" />
      </bpmn:extensionElements>
    </bpmn:subProcess>
  </bpmn:process>
</bpmn:definitions>`

	var def TDefinitions
	err := xml.Unmarshal([]byte(businessKeyXML), &def)
	assert.NoError(t, err)
	assert.Equal(t, "=processBusinessKey", *def.Process.CallActivity[0].GetBusinessKey())
	assert.Nil(t, def.Process.CallActivity[1].GetBusinessKey())
	assert.Equal(t, "", *def.Process.SubProcess[0].GetBusinessKey())
}

func TestUnmarshalZenbpmExtensions_UserTaskWithAssignment(t *testing.T) {
	zenbpmXML := `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zenbpm="` + zenbpmNS + `">
  <bpmn:process id="ut-process" isExecutable="true">
    <bpmn:userTask id="userTask1" name="Review">
      <bpmn:extensionElements>
        <zenbpm:userTask />
        <zenbpm:ioMapping>
          <zenbpm:input source="=form-key" target="_formKey" />
          <zenbpm:output source="=approved" target="approved" />
        </zenbpm:ioMapping>
        <zenbpm:assignmentDefinition assignee="=reviewer" candidateGroups="group-a, group-b" />
      </bpmn:extensionElements>
    </bpmn:userTask>
  </bpmn:process>
</bpmn:definitions>`

	var def TDefinitions
	err := xml.Unmarshal([]byte(zenbpmXML), &def)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(def.Process.UserTasks))
	ut := def.Process.UserTasks[0]
	assert.Equal(t, "=reviewer", ut.GetAssignmentAssignee())
	assert.Equal(t, []string{"group-a", "group-b"}, ut.GetAssignmentCandidateGroups())
	assert.Equal(t, "_formKey", ut.GetInputMapping()[0].Target)
	assert.Equal(t, "approved", ut.GetOutputMapping()[0].Target)
}

func TestUnmarshalZenbpmExtensions_MultiInstanceLoopCharacteristics(t *testing.T) {
	zenbpmXML := `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zenbpm="` + zenbpmNS + `">
  <bpmn:process id="mi-process" isExecutable="true">
    <bpmn:userTask id="miTask" name="Approve">
      <bpmn:extensionElements>
        <zenbpm:userTask />
      </bpmn:extensionElements>
      <bpmn:multiInstanceLoopCharacteristics isSequential="false">
        <bpmn:extensionElements>
          <zenbpm:loopCharacteristics
            inputCollection="=approvers"
            inputElement="approver"
            outputCollection="results"
            outputElement="=approver" />
        </bpmn:extensionElements>
      </bpmn:multiInstanceLoopCharacteristics>
    </bpmn:userTask>
  </bpmn:process>
</bpmn:definitions>`

	var def TDefinitions
	err := xml.Unmarshal([]byte(zenbpmXML), &def)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(def.Process.UserTasks))
	mi := def.Process.UserTasks[0].GetMultiInstance()
	assert.NotNil(t, mi)
	assert.False(t, mi.IsSequential)
	assert.Equal(t, "=approvers", mi.LoopCharacteristics.InputCollectionExpression)
	assert.Equal(t, "approver", mi.LoopCharacteristics.InputElementName)
	assert.Equal(t, "results", mi.LoopCharacteristics.OutputCollectionName)
	assert.Equal(t, "=approver", mi.LoopCharacteristics.OutputElementExpression)
}
