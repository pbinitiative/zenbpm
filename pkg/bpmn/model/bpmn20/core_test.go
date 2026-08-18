package bpmn20

import (
	"encoding/xml"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTDefinitions_HasTypedIndexes(t *testing.T) {
	xmlData, err := os.ReadFile("./test-cases/simple_task.bpmn")
	require.NoError(t, err)
	var defs TDefinitions
	require.NoError(t, xml.Unmarshal(xmlData, &defs))

	assert.NotNil(t, defs.flowNodes)
	assert.NotNil(t, defs.internalTasks)
	assert.NotNil(t, defs.elementOwner)
	assert.NotNil(t, defs.subprocessParent)
}

func TestResolveReferences_PopulatesTypedIndexes(t *testing.T) {
	xmlData, err := os.ReadFile("./test-cases/simple_task.bpmn")
	require.NoError(t, err)
	var defs TDefinitions
	require.NoError(t, xml.Unmarshal(xmlData, &defs))

	// Flow-node index covers every token-driven flow node.
	assert.Contains(t, defs.flowNodes, "StartEvent_1")
	assert.Contains(t, defs.flowNodes, "id")
	assert.Contains(t, defs.flowNodes, "Event_1j4mcqg")
	assert.NotContains(t, defs.flowNodes, "Flow_0xt1d7q",
		"sequence flows are not flow nodes")

	// Internal-task index covers every InternalTask implementor, including TEndEvent.
	assert.Contains(t, defs.internalTasks, "id", "service task is an internal task")
	assert.Contains(t, defs.internalTasks, "Event_1j4mcqg",
		"TEndEvent implements InternalTask and must be registered via interface assertion")
	assert.NotContains(t, defs.internalTasks, "StartEvent_1",
		"TStartEvent does not implement InternalTask")

	// Top-level elements have no owning subprocess.
	assert.Equal(t, "", defs.elementOwner["StartEvent_1"])
	assert.Equal(t, "", defs.elementOwner["id"])
	assert.Equal(t, "", defs.elementOwner["Event_1j4mcqg"])
}

func TestFindFlowNodeById_BoundaryEventsNeverExposed(t *testing.T) {
	// nested_sub_process.bpmn has Event_07bcheq (boundary event on "id" task).
	xmlData, err := os.ReadFile("./test-cases/nested_sub_process.bpmn")
	require.NoError(t, err)
	var defs TDefinitions
	require.NoError(t, xml.Unmarshal(xmlData, &defs))

	n, err := FindFlowNodeById(&defs, "id")
	require.NoError(t, err)
	assert.Equal(t, "id", n.GetId())

	_, err = FindFlowNodeById(&defs, "Event_07bcheq")
	require.Error(t, err, "boundary events must not be exposed by FindFlowNodeById")
	assert.ErrorIs(t, err, ErrFlowNodeNotFound)
}

func TestFindFlowNodeById_ErrorForUninitialisedDefinitions(t *testing.T) {
	defs := &TDefinitions{} // no ResolveReferences
	_, err := FindFlowNodeById(defs, "anything")
	require.Error(t, err, "uninitialised typed index must return an explicit error, not panic")
	assert.ErrorIs(t, err, ErrLookupIndexNotInitialised,
		"error must explicitly identify the unresolved-index condition")

	defsNil := (*TDefinitions)(nil)
	_, err = FindFlowNodeById(defsNil, "anything")
	require.Error(t, err, "nil definitions must return an explicit error")
}

func TestFindInternalTaskById_IncludesEndEvent(t *testing.T) {
	xmlData, err := os.ReadFile("./test-cases/simple_task.bpmn")
	require.NoError(t, err)
	var defs TDefinitions
	require.NoError(t, xml.Unmarshal(xmlData, &defs))

	end, err := FindInternalTaskById(&defs, "Event_1j4mcqg")
	require.NoError(t, err, "TEndEvent implements InternalTask and must be indexed")
	assert.Equal(t, "Event_1j4mcqg", end.GetId())

	_, err = FindInternalTaskById(&defs, "StartEvent_1")
	require.Error(t, err, "TStartEvent does not implement InternalTask")
	assert.ErrorIs(t, err, ErrInternalTaskNotFound)
}

func TestIsElementInSubProcessScope_RootAcceptsPresentElements(t *testing.T) {
	xmlData, err := os.ReadFile("../../test-cases/nested_sub_process_lookup.bpmn")
	require.NoError(t, err)
	var defs TDefinitions
	require.NoError(t, xml.Unmarshal(xmlData, &defs))

	// Root scope (subprocessID == "") accepts every registered element.
	assert.True(t, IsElementInSubProcessScope(&defs, "", "LookupEnd"))
	assert.True(t, IsElementInSubProcessScope(&defs, "", "OuterSub"))
	assert.True(t, IsElementInSubProcessScope(&defs, "", "DeepTask"))

	// Root scope REJECTS missing elements — see docstring on
	// IsElementInSubProcessScope.
	assert.False(t, IsElementInSubProcessScope(&defs, "", "missing-id"))
}

func TestIsElementInSubProcessScope_NestedMatrix(t *testing.T) {
	xmlData, err := os.ReadFile("../../test-cases/nested_sub_process_lookup.bpmn")
	require.NoError(t, err)
	var defs TDefinitions
	require.NoError(t, xml.Unmarshal(xmlData, &defs))

	cases := []struct {
		sub, elem string
		want      bool
	}{
		{"OuterSub", "OuterEnd", true},    // direct child
		{"OuterSub", "InnerSub", true},    // nested subprocess
		{"OuterSub", "DeepTask", true},    // deeply nested
		{"OuterSub", "LookupEnd", false},  // top-level — BLOCKER 1
		{"InnerSub", "DeepTask", true},    // direct child of inner
		{"InnerSub", "OuterEnd", false},   // sibling inside OuterSub
		{"InnerSub", "LookupEnd", false},  // top-level
		{"OuterSub", "OuterSub", false},   // instance cannot resolve itself
		{"does-not-exist", "LookupEnd", false}, // missing parent subprocess
		{"OuterSub", "missing-id", false}, // missing element
	}
	for _, tc := range cases {
		got := IsElementInSubProcessScope(&defs, tc.sub, tc.elem)
		assert.Equalf(t, tc.want, got, "sub=%q elem=%q", tc.sub, tc.elem)
	}
}

func TestFindSubprocessAndStartEventById_O1(t *testing.T) {
	xmlData, err := os.ReadFile("../../test-cases/nested_sub_process_lookup.bpmn")
	require.NoError(t, err)
	var defs TDefinitions
	require.NoError(t, xml.Unmarshal(xmlData, &defs))

	sp, se, ok := FindSubprocessAndStartEventById(&defs, "OuterStart")
	require.True(t, ok)
	assert.Equal(t, "OuterSub", sp.GetId())
	assert.Equal(t, "OuterStart", se.GetId())

	sp, se, ok = FindSubprocessAndStartEventById(&defs, "InnerStart")
	require.True(t, ok)
	assert.Equal(t, "InnerSub", sp.GetId())

	_, _, ok = FindSubprocessAndStartEventById(&defs, "LookupStart")
	assert.False(t, ok, "top-level start events do not belong to a subprocess")

	_, _, ok = FindSubprocessAndStartEventById(&defs, "no-such-id")
	assert.False(t, ok)
}

// TestSiblingScopeFixture_ParsesAndIndexes ensures the new
// sibling_sub_process_scope.bpmn fixture is valid BPMN and the
// ResolveReferences pipeline populates the typed indexes for every
// element in it. If the fixture XML is invalid or the pipeline
// regresses, this test fails.
func TestSiblingScopeFixture_ParsesAndIndexes(t *testing.T) {
	xmlData, err := os.ReadFile("../../test-cases/sibling_sub_process_scope.bpmn")
	require.NoError(t, err)
	var defs TDefinitions
	require.NoError(t, xml.Unmarshal(xmlData, &defs),
		"sibling_sub_process_scope.bpmn must parse cleanly")

	for _, id := range []string{
		// root-level
		"ScopeStart", "ScopeEnd",
		// SiblingA
		"SiblingA", "AStart", "AEnd",
		// ANested (child of SiblingA)
		"ANested", "ANestedStart", "ANestedTask", "ANestedEnd",
		// SiblingB
		"SiblingB", "BStart", "BEnd",
		// BNested (child of SiblingB)
		"BNested", "BNestedStart", "BNestedTask", "BNestedEnd",
	} {
		assert.Containsf(t, defs.flowNodes, id,
			"fixture element %q must be indexed", id)
	}
}

// TestIsElementInSubProcessScope_SiblingFixture exercises the deeper
// nesting tree (root → SiblingA → ANested, root → SiblingB → BNested)
// and pins down the scope algorithm against siblings and ancestor
// subprocesses.
func TestIsElementInSubProcessScope_SiblingFixture(t *testing.T) {
	xmlData, err := os.ReadFile("../../test-cases/sibling_sub_process_scope.bpmn")
	require.NoError(t, err)
	var defs TDefinitions
	require.NoError(t, xml.Unmarshal(xmlData, &defs))

	cases := []struct {
		sub, elem string
		want      bool
	}{
		// SiblingA scope
		{"SiblingA", "AEnd", true},
		{"SiblingA", "ANested", true},
		{"SiblingA", "ANestedTask", true},
		{"SiblingA", "BEnd", false},
		{"SiblingA", "BNestedTask", false},
		{"SiblingA", "ScopeEnd", false},
		{"SiblingA", "ScopeStart", false},
		// ANested scope (sibling of ANested inside SiblingA is AEnd)
		{"ANested", "ANestedTask", true},
		{"ANested", "ANestedEnd", true},
		{"ANested", "AEnd", false},
		{"ANested", "BEnd", false},
		{"ANested", "BNestedTask", false},
		// SiblingB scope
		{"SiblingB", "BEnd", true},
		{"SiblingB", "BNestedTask", true},
		{"SiblingB", "AEnd", false},
		{"SiblingB", "ANestedTask", false},
		// BNested scope
		{"BNested", "BNestedTask", true},
		{"BNested", "BEnd", false},
		// Edge cases
		{"SiblingA", "SiblingA", false},
		{"missing", "ScopeStart", false},
		{"SiblingA", "missing-id", false},
	}
	for _, tc := range cases {
		got := IsElementInSubProcessScope(&defs, tc.sub, tc.elem)
		assert.Equalf(t, tc.want, got, "sub=%q elem=%q", tc.sub, tc.elem)
	}
}
func TestTProcessGetFlowNodeById_StablePointer(t *testing.T) {
	proc := TProcess{
		TCallableElement: TCallableElement{TBaseElement: TBaseElement{Id: "proc"}},
		TFlowElementsContainer: TFlowElementsContainer{
			ServiceTasks: []TServiceTask{
				{
					TExternallyProcessedTask: TExternallyProcessedTask{
						TTask: TTask{
							TActivity: TActivity{
								TFlowNode: TFlowNode{
									TFlowElement: TFlowElement{TBaseElement: TBaseElement{Id: "task"}},
								},
							},
						},
					},
				},
			},
		},
	}
	got := proc.GetFlowNodeById("task")
	require.NotNil(t, got)
	assert.Same(t, &proc.ServiceTasks[0], got, "must return slice element pointer, not range-loop copy")
}

func TestTProcessGetInternalTaskById_StablePointer(t *testing.T) {
	proc := TProcess{
		TFlowElementsContainer: TFlowElementsContainer{
			ServiceTasks: []TServiceTask{
				{
					TExternallyProcessedTask: TExternallyProcessedTask{
						TTask: TTask{
							TActivity: TActivity{
								TFlowNode: TFlowNode{
									TFlowElement: TFlowElement{TBaseElement: TBaseElement{Id: "task"}},
								},
							},
						},
					},
				},
			},
		},
	}
	got := proc.GetInternalTaskById("task")
	require.NotNil(t, got)
	assert.Same(t, &proc.ServiceTasks[0], got)
}

func TestFindFlowNodeById_AllSupportedTypes(t *testing.T) {
	xmlStr := `<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zenbpm="http://zenbpm.pbinitiative.org/1.0" id="defs_all_types">
  <bpmn:process id="p_all" isExecutable="true">
    <bpmn:startEvent id="se" />
    <bpmn:endEvent id="ee" />
    <bpmn:serviceTask id="st"><bpmn:extensionElements><zenbpm:taskDefinition type="x" /></bpmn:extensionElements></bpmn:serviceTask>
    <bpmn:userTask id="ut"><bpmn:extensionElements><zenbpm:assignmentDefinition /></bpmn:extensionElements></bpmn:userTask>
    <bpmn:businessRuleTask id="brt"><bpmn:extensionElements><zenbpm:taskDefinition type="y" /></bpmn:extensionElements></bpmn:businessRuleTask>
    <bpmn:sendTask id="snd"><bpmn:extensionElements><zenbpm:taskDefinition type="z" /></bpmn:extensionElements></bpmn:sendTask>
    <bpmn:receiveTask id="rcv" />
    <bpmn:parallelGateway id="pg" />
    <bpmn:exclusiveGateway id="eg" />
    <bpmn:eventBasedGateway id="ebg" />
    <bpmn:inclusiveGateway id="ig" />
    <bpmn:intermediateCatchEvent id="ice" />
    <bpmn:intermediateThrowEvent id="ite" />
    <bpmn:callActivity id="ca" />
    <bpmn:subProcess id="sp">
      <bpmn:startEvent id="sp_se" />
    </bpmn:subProcess>
  </bpmn:process>
</bpmn:definitions>`
	var defs TDefinitions
	require.NoError(t, xml.Unmarshal([]byte(xmlStr), &defs))

	for _, id := range []string{
		"se", "ee", "st", "ut", "brt", "snd", "rcv",
		"pg", "eg", "ebg", "ig", "ice", "ite", "ca", "sp", "sp_se",
	} {
		fn, err := FindFlowNodeById(&defs, id)
		require.NoErrorf(t, err, "%s must be indexable as a flow node", id)
		require.NotNilf(t, fn, "%s must not be nil", id)
		assert.Equal(t, id, fn.GetId())
	}
}

func TestFindInternalTaskById_AllSupportedTypes(t *testing.T) {
	xmlStr := `<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zenbpm="http://zenbpm.pbinitiative.org/1.0" id="defs_internal_tasks">
  <bpmn:process id="p_it" isExecutable="true">
    <bpmn:serviceTask id="st"><bpmn:extensionElements><zenbpm:taskDefinition type="x" /></bpmn:extensionElements></bpmn:serviceTask>
    <bpmn:userTask id="ut"><bpmn:extensionElements><zenbpm:assignmentDefinition /></bpmn:extensionElements></bpmn:userTask>
    <bpmn:businessRuleTask id="brt"><bpmn:extensionElements><zenbpm:taskDefinition type="y" /></bpmn:extensionElements></bpmn:businessRuleTask>
    <bpmn:sendTask id="snd"><bpmn:extensionElements><zenbpm:taskDefinition type="z" /></bpmn:extensionElements></bpmn:sendTask>
    <bpmn:intermediateThrowEvent id="ite"><bpmn:messageEventDefinition id="med" /></bpmn:intermediateThrowEvent>
    <bpmn:endEvent id="ee" />
  </bpmn:process>
</bpmn:definitions>`
	var defs TDefinitions
	require.NoError(t, xml.Unmarshal([]byte(xmlStr), &defs))

	for _, id := range []string{"st", "ut", "brt", "snd", "ite", "ee"} {
		t2, err := FindInternalTaskById(&defs, id)
		require.NoErrorf(t, err, "%s must be indexable as an internal task (including TEndEvent)", id)
		require.NotNilf(t, t2, "%s must not be nil", id)
		assert.Equal(t, id, t2.GetId())
	}

	_, err := FindInternalTaskById(&defs, "missing-id")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInternalTaskNotFound)
}

func TestTypedIndex_StablePointerIdentity(t *testing.T) {
	xmlData, err := os.ReadFile("../../test-cases/nested_sub_process_lookup.bpmn")
	require.NoError(t, err)
	var defs TDefinitions
	require.NoError(t, xml.Unmarshal(xmlData, &defs))

	n1, err := FindFlowNodeById(&defs, "DeepTask")
	require.NoError(t, err)
	n2, err := FindFlowNodeById(&defs, "DeepTask")
	require.NoError(t, err)
	assert.Same(t, n1, n2, "repeated lookups must return the same pointer")

	expected := &defs.Process.SubProcess[0].TProcess.SubProcess[0].TProcess.ServiceTasks[0]
	assert.Same(t, expected, n1, "indexed lookup must return the slice element pointer")

	t1, err := FindInternalTaskById(&defs, "DeepTask")
	require.NoError(t, err)
	assert.Same(t, expected, t1, "internal-task lookup must return the same slice element pointer")
}

func TestTypedHelpers_MissingIDs(t *testing.T) {
	xmlData, err := os.ReadFile("./test-cases/simple_task.bpmn")
	require.NoError(t, err)
	var defs TDefinitions
	require.NoError(t, xml.Unmarshal(xmlData, &defs))

	assert.NotPanics(t, func() {
		n, err := FindFlowNodeById(&defs, "")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrFlowNodeNotFound)
		assert.Nil(t, n)

		t2, err := FindInternalTaskById(&defs, "")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrInternalTaskNotFound)
		assert.Nil(t, t2)

		_, _, ok := FindSubprocessAndStartEventById(&defs, "")
		assert.False(t, ok)
	})

	n, err := FindFlowNodeById(&defs, "absolutely-no-such-element")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrFlowNodeNotFound)
	assert.Nil(t, n)

	_, _, ok := FindSubprocessAndStartEventById(&defs, "absolutely-no-such-element")
	assert.False(t, ok)

	assert.False(t, IsElementInSubProcessScope(&defs, "any", "absent-element"))
}
