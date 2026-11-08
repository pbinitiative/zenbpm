package bpmn20

import (
	"encoding/xml"
	"fmt"
	"strings"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecutionListenersAreReadInEveryNamespace(t *testing.T) {
	for _, prefix := range []string{"zeebe", "zenbpm", "custom", ""} {
		t.Run(fmt.Sprintf("prefix %q", prefix), func(t *testing.T) {
			model := listenerModel(prefix, "", listenersOf(prefix, `eventType="end" type="on-start" retries="3" retryBackoff="PT10S"`), "")
			definitions := parseListenerModel(t, model)

			startEvent := &definitions.Process.StartEvents[0]
			assert.Equal(t, []extensions.TExecutionListener{{EventType: "end", Type: "on-start", Retries: "3", RetryBackoff: "PT10S"}},
				startEvent.GetExecutionListeners())
			require.NotNil(t, ListenerTaskOf(startEvent, "end"))
			assert.NoError(t, definitions.ValidateForDeployment())
			assert.NoError(t, ValidateListenerPositions([]byte(model)))
		})
	}
}

func TestStartEventKeepsItsOutputMappingsBesideItsListener(t *testing.T) {
	definitions := parseListenerModel(t, listenerModel("zeebe", "", listenersOf("zeebe", `eventType="end" type="on-start"`)+`
        <zenbpm:ioMapping><zenbpm:output source="=1" target="one" /></zenbpm:ioMapping>`, ""))

	startEvent := &definitions.Process.StartEvents[0]
	assert.Len(t, startEvent.GetOutputMapping(), 1)
	assert.NotNil(t, ListenerTaskOf(startEvent, "end"))
}

func TestElementsWithoutListenersAnswerNone(t *testing.T) {
	definitions := parseListenerModel(t, listenerModel("zeebe", "", "", ""))

	assert.Nil(t, ListenerTaskOf(&definitions.Process.StartEvents[0], "end"))
	assert.Nil(t, definitions.Process.TaskOfJob("start", "end"))
	assert.False(t, definitions.HasExecutionListeners())
}

func TestTaskOfJobAnswersTheListenerOfAStartEvent(t *testing.T) {
	definitions := parseListenerModel(t, listenerModel("zeebe", "",
		listenersOf("zeebe", `eventType="end" type="on-start" retries="2"`), ""))
	process := &definitions.Process

	start, ok := process.TaskOfJob("start", "end").(*ListenerTask)
	require.True(t, ok)
	assert.Equal(t, "end", start.EventType())
	assert.Equal(t, "on-start", start.GetTaskType())
	assert.Equal(t, extensions.TTaskDefinition{TypeName: "on-start", Retries: "2"}, start.GetTaskDefinition())
	assert.Equal(t, ElementTypeStartEvent, start.GetType())
	assert.Nil(t, start.GetInputMapping())
	assert.Nil(t, start.GetOutputMapping())
	assert.Len(t, start.GetOutgoingAssociation(), 1, "the start event's own sequence flows")
	assert.Equal(t, &definitions.Process.StartEvents[0], start.Element())
	assert.Nil(t, process.TaskOfJob("unknown", "end"))
	assert.Nil(t, process.TaskOfJob("start", "start"), "the start event has no listener of that event type")
	assert.Nil(t, process.TaskOfJob("task", "end"), "a service task reads no listeners")

	task := process.TaskOfJob("task", "")
	require.NotNil(t, task)
	assert.Equal(t, "work", task.GetTaskType())
}

func TestListenerJobTypeIsTrimmed(t *testing.T) {
	definitions := parseListenerModel(t, listenerModel("zeebe", "", listenersOf("zeebe", `eventType="end" type=" on-start "`), ""))
	require.NoError(t, definitions.ValidateForDeployment())

	start := ListenerTaskOf(&definitions.Process.StartEvents[0], "end")
	require.NotNil(t, start)
	assert.Equal(t, "on-start", start.GetTaskType(), "a worker of type on-start gets the job")
	assert.Equal(t, "on-start", start.GetTaskDefinition().TypeName)
}

func TestOnlyAStartEventOfTheProcessRunsAListener(t *testing.T) {
	definitions := parseListenerModel(t, listenerModel("zeebe", "", "", subProcessWith("", "")))
	process := &definitions.Process

	assert.Equal(t, []string{"end"}, process.RunnableListenerEventTypes(&process.StartEvents[0]))
	subStart, ok := process.GetFlowNodeById("sub-start").(ExecutionListenerOwner)
	require.True(t, ok)
	assert.Empty(t, process.RunnableListenerEventTypes(subStart))
}

func TestListenerEventTypeOfJobFollowsTheElementType(t *testing.T) {
	assert.Equal(t, "end", ListenerEventTypeOfJob(string(ElementTypeStartEvent)))
	assert.Equal(t, "", ListenerEventTypeOfJob(string(ElementTypeServiceTask)))
	assert.Equal(t, "", ListenerEventTypeOfJob(""))
}

func TestDeploymentRefusesListenersWhereTheEngineDoesNotRunThem(t *testing.T) {
	cases := []struct {
		name         string
		startEvent   string
		other        string
		errorMessage string
	}{
		{
			name:         "a start listener of another event type",
			startEvent:   listenersOf("zeebe", `eventType="start" type="t"`),
			errorMessage: `execution listener eventType="start" on start event id="start" is not supported: a start event takes one listener with eventType="end"`,
		},
		{
			name:         "two listeners on a start event",
			startEvent:   listenersOf("zeebe", `eventType="end" type="a"`, `eventType="end" type="b"`),
			errorMessage: `start event id="start" has 2 execution listeners, which is not supported: a start event takes one listener with eventType="end"`,
		},
		{
			name:         "a listener on a sub-process",
			other:        subProcessWith(listenersOf("zeebe", `eventType="start" type="t"`), ""),
			errorMessage: `execution listener eventType="start" on sub process id="sub" is not supported`,
		},
		{
			name:         "a listener on a start event of a sub-process",
			other:        subProcessWith("", listenersOf("zeebe", `eventType="end" type="t"`)),
			errorMessage: `execution listener eventType="end" on start event id="sub-start" is not supported: only a start event directly under the process takes an execution listener, with eventType="end"`,
		},
		{
			name:         "a listener on a task",
			other:        `<bpmn:serviceTask id="listened-task"><bpmn:extensionElements>` + listenersOf("zeebe", `eventType="start" type="t"`) + `</bpmn:extensionElements></bpmn:serviceTask>`,
			errorMessage: `execution listener eventType="start" on service task id="listened-task" is not supported: only a start event directly under the process takes an execution listener, with eventType="end"`,
		},
		{
			name:         "a task listener",
			other:        `<bpmn:userTask id="listened-user-task"><bpmn:extensionElements><zeebe:taskListeners><zeebe:taskListener eventType="creating" type="t" /></zeebe:taskListeners></bpmn:extensionElements></bpmn:userTask>`,
			errorMessage: `task listener on user task id="listened-user-task" is not supported: the engine runs no task listeners`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			model := listenerModel("zeebe", "", tc.startEvent, tc.other)

			err := ValidateListenerPositions([]byte(model))
			if err == nil {
				definitions := parseListenerModel(t, model)
				err = definitions.ValidateForDeployment()
			}

			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errorMessage)
		})
	}
}

func TestDeploymentRefusesAStartListenerOutsideItsContainers(t *testing.T) {
	cases := []struct {
		name       string
		startEvent string
		listener   string
	}{
		{
			name:       "without executionListeners",
			startEvent: `<bpmn:extensionElements><zeebe:executionListener eventType="end" type="t" /></bpmn:extensionElements>`,
			listener:   `execution listener eventType="end"`,
		},
		{
			name:       "without extensionElements",
			startEvent: `<zeebe:executionListeners><zeebe:executionListener eventType="end" type="t" /></zeebe:executionListeners>`,
			listener:   `execution listener eventType="end"`,
		},
		{
			name:       "in the shape of Camunda 7",
			startEvent: `<bpmn:extensionElements><camunda:executionListener event="start" class="Listener" /></bpmn:extensionElements>`,
			listener:   "execution listener without an eventType",
		},
		{
			name:       "in two executionListeners",
			startEvent: `<bpmn:extensionElements><zeebe:executionListeners><zeebe:executionListeners><zeebe:executionListener eventType="end" type="t" /></zeebe:executionListeners></zeebe:executionListeners></bpmn:extensionElements>`,
			listener:   `execution listener eventType="end"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			model := withStartEventContent(listenerModel("zeebe", "", "", ""), tc.startEvent)
			definitions := parseListenerModel(t, model)
			assert.False(t, definitions.HasExecutionListeners(), "the model drops a listener in this shape")

			err := ValidateListenerPositions([]byte(model))

			require.Error(t, err)
			assert.Equal(t, tc.listener+` on start event id="start" is not supported where it sits: `+
				`a listener has to sit in an executionListeners element directly inside the extensionElements of its element`, err.Error())
		})
	}
}

func TestListenerOfASubProcessStartEventPassesThePositionCheckForTheModelCheck(t *testing.T) {
	model := listenerModel("zeebe", "", "", subProcessWith("", listenersOf("zeebe", `eventType="end" type="t"`)))

	require.NoError(t, ValidateListenerPositions([]byte(model)), "the model reads the listeners of every start event")
	definitions := parseListenerModel(t, model)
	assert.True(t, definitions.HasExecutionListeners())
	assert.Error(t, definitions.ValidateForDeployment())
}

func TestListenerOnTheProcessElementIsNeitherRunNorRefused(t *testing.T) {
	model := listenerModel("zeebe", listenersOf("zeebe", `eventType="cancel" type="t"`), "", "")

	assert.NoError(t, ValidateListenerPositions([]byte(model)))
	definitions := parseListenerModel(t, model)
	assert.NoError(t, definitions.ValidateForDeployment())
	assert.False(t, definitions.HasExecutionListeners(), "the model of the process reads no listeners")

	withoutContainer := listenerModel("zeebe", `<zeebe:executionListener eventType="cancel" type="t" />`, "", "")
	assert.NoError(t, ValidateListenerPositions([]byte(withoutContainer)), "a listener on the process is left alone wherever it sits in it")
}

func TestDeploymentRefusesListenersWithoutLiterals(t *testing.T) {
	cases := []struct {
		attributes   string
		errorMessage string
	}{
		{attributes: `eventType="end" type=""`, errorMessage: "its type is blank"},
		{attributes: `eventType="end" type="  "`, errorMessage: "its type is blank"},
		{attributes: `eventType="end"`, errorMessage: "its type is blank"},
		{attributes: `eventType="end" type="=jobType"`, errorMessage: `its type "=jobType" is an expression`},
		{attributes: `eventType="end" type="t" retries="=retries"`, errorMessage: `its retries "=retries" is an expression`},
		{attributes: `eventType="end" type="t" retries="-1"`, errorMessage: "must not be negative"},
		{attributes: `eventType="end" type="t" retries="many"`, errorMessage: "is not an integer"},
		{attributes: `eventType="end" type="t" retryBackoff="=backoff"`, errorMessage: `its retryBackoff "=backoff" is an expression`},
		{attributes: `eventType="end" type="t" retryBackoff="P1M"`, errorMessage: "uses years or months"},
	}
	for _, tc := range cases {
		t.Run(tc.attributes, func(t *testing.T) {
			definitions := parseListenerModel(t, listenerModel("zeebe", "", listenersOf("zeebe", tc.attributes), ""))

			err := definitions.ValidateForDeployment()

			require.Error(t, err)
			assert.Contains(t, err.Error(), `invalid execution listener eventType="end" on start event id="start"`)
			assert.Contains(t, err.Error(), tc.errorMessage)
		})
	}
}

func TestDeploymentAcceptsListenersWithBlankRetriesAndBackoff(t *testing.T) {
	for _, attributes := range []string{
		`eventType="end" type="s" retries="" retryBackoff=""`,
		`eventType="end" type="s" retries="5" retryBackoff="PT1S,PT1M"`,
	} {
		definitions := parseListenerModel(t, listenerModel("zeebe", "", listenersOf("zeebe", attributes), ""))

		assert.NoError(t, definitions.ValidateForDeployment(), attributes)
	}
}

func TestStoredDefinitionWithARefusedListenerStillParses(t *testing.T) {
	model := listenerModel("zeebe", "", listenersOf("zeebe", `eventType="start" type="t"`), "")

	definitions := parseListenerModel(t, model)

	assert.Error(t, definitions.ValidateForDeployment(), "a deployment refuses it")
	assert.True(t, definitions.HasExecutionListeners(), "parsing alone, as an import does, keeps it")
}

func parseListenerModel(t *testing.T, model string) TDefinitions {
	t.Helper()
	var definitions TDefinitions
	require.NoError(t, xml.Unmarshal([]byte(model), &definitions))
	return definitions
}

// listenerModel is a process with a start event, a service task and an end
// event; the listeners are placed on the process and on the start event, and
// other is added inside the process.
func listenerModel(prefix string, processListeners string, startEventExtensions string, other string) string {
	namespaceDeclaration := ""
	if prefix != "" {
		namespaceDeclaration = fmt.Sprintf(` xmlns:%s="http://example.com/listeners/%s"`, prefix, prefix)
	}
	if prefix != "zenbpm" {
		namespaceDeclaration += ` xmlns:zenbpm="http://zenbpm.pbinitiative.org/1.0"`
	}
	if prefix != "zeebe" {
		namespaceDeclaration += ` xmlns:zeebe="http://camunda.org/schema/zeebe/1.0"`
	}
	processExtensions := ""
	if processListeners != "" {
		processExtensions = "<bpmn:extensionElements>" + processListeners + "</bpmn:extensionElements>"
	}
	startExtensions := ""
	if startEventExtensions != "" {
		startExtensions = "<bpmn:extensionElements>" + startEventExtensions + "</bpmn:extensionElements>"
	}
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"%s id="definitions">
  <bpmn:process id="listener-process" isExecutable="true">
    %s
    <bpmn:startEvent id="start">%s<bpmn:outgoing>flow-1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="flow-1" sourceRef="start" targetRef="task" />
    <bpmn:serviceTask id="task">
      <bpmn:extensionElements><zenbpm:taskDefinition type="work" /></bpmn:extensionElements>
      <bpmn:incoming>flow-1</bpmn:incoming><bpmn:outgoing>flow-2</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="flow-2" sourceRef="task" targetRef="end" />
    <bpmn:endEvent id="end"><bpmn:incoming>flow-2</bpmn:incoming></bpmn:endEvent>
    %s
  </bpmn:process>
</bpmn:definitions>`, namespaceDeclaration, processExtensions, startExtensions, other)
}

func listenersOf(prefix string, attributes ...string) string {
	element := func(name string) string {
		if prefix == "" {
			return name
		}
		return prefix + ":" + name
	}
	listeners := make([]string, len(attributes))
	for i, attribute := range attributes {
		listeners[i] = fmt.Sprintf("<%s %s />", element("executionListener"), attribute)
	}
	return fmt.Sprintf("<%s>%s</%s>", element("executionListeners"), strings.Join(listeners, ""), element("executionListeners"))
}

func subProcessWith(subProcessExtensions string, startEventExtensions string) string {
	if subProcessExtensions != "" {
		subProcessExtensions = "<bpmn:extensionElements>" + subProcessExtensions + "</bpmn:extensionElements>"
	}
	if startEventExtensions != "" {
		startEventExtensions = "<bpmn:extensionElements>" + startEventExtensions + "</bpmn:extensionElements>"
	}
	return fmt.Sprintf(`<bpmn:subProcess id="sub">%s
      <bpmn:startEvent id="sub-start">%s<bpmn:outgoing>sub-flow</bpmn:outgoing></bpmn:startEvent>
      <bpmn:sequenceFlow id="sub-flow" sourceRef="sub-start" targetRef="sub-end" />
      <bpmn:endEvent id="sub-end"><bpmn:incoming>sub-flow</bpmn:incoming></bpmn:endEvent>
    </bpmn:subProcess>`, subProcessExtensions, startEventExtensions)
}

// withStartEventContent puts the content into the start event "start" of the
// model, and declares the namespace of Camunda 7 it may use.
func withStartEventContent(model string, content string) string {
	model = strings.Replace(model, `id="definitions"`, `xmlns:camunda="http://camunda.org/schema/1.0/bpmn" id="definitions"`, 1)
	return strings.Replace(model, `<bpmn:startEvent id="start">`, `<bpmn:startEvent id="start">`+content, 1)
}
