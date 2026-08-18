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