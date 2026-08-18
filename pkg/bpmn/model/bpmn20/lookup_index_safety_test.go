package bpmn20

import (
	"encoding/xml"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTypedHelpers_UninitialisedIndexReturnsError pins down the contract
// that programmatic definitions must call ResolveReferences before
// passing to the engine. The typed helpers return an explicit error
// wrapping ErrLookupIndexNotInitialised instead of lazy-initialising or
// silently looking like a missing id.
func TestTypedHelpers_UninitialisedIndexReturnsError(t *testing.T) {
	defs := unindexedLookupDefinition()
	require.Nil(t, defs.flowNodes)

	_, err := FindFlowNodeById(&defs, "task")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrLookupIndexNotInitialised,
		"uninitialised typed index must return the sentinel error so callers can distinguish it from a missing id")

	_, err = FindInternalTaskById(&defs, "task")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrLookupIndexNotInitialised)

	assert.False(t, IsElementInSubProcessScope(&defs, "any", "task"),
		"uninitialised typed index must report no scope match")
	_, _, ok := FindSubprocessAndStartEventById(&defs, "any")
	assert.False(t, ok)
}

// TestTypedLookups_ConcurrentReadsOnSameDefinitions pins down that the
// typed indexes are safe for concurrent access by many goroutines
// reading the same prepared definitions.
func TestTypedLookups_ConcurrentReadsOnSameDefinitions(t *testing.T) {
	xmlData, err := os.ReadFile("../../test-cases/nested_sub_process_lookup.bpmn")
	require.NoError(t, err)
	var defs TDefinitions
	require.NoError(t, xml.Unmarshal(xmlData, &defs))

	const N = 100
	const Iterations = 100

	errCh := make(chan error, N)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					errCh <- fmt.Errorf("goroutine panic: %v", r)
				}
			}()
			<-start
			for j := 0; j < Iterations; j++ {
				n, err := FindFlowNodeById(&defs, "DeepTask")
				if err != nil || n == nil {
					errCh <- errors.New("flow lookup failed")
					return
				}
				t2, err := FindInternalTaskById(&defs, "DeepTask")
				if err != nil || t2 == nil {
					errCh <- errors.New("task lookup failed")
					return
				}
				if !IsElementInSubProcessScope(&defs, "OuterSub", "DeepTask") {
					errCh <- errors.New("scope check failed")
					return
				}
				sp, se, ok := FindSubprocessAndStartEventById(&defs, "InnerStart")
				if !ok || sp == nil || se == nil {
					errCh <- errors.New("subprocess lookup failed")
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("goroutine error: %v", err)
	}
}

func unindexedLookupDefinition() TDefinitions {
	return TDefinitions{
		TRootElementsContainer: TRootElementsContainer{
			Process: TProcess{
				TCallableElement: TCallableElement{
					TBaseElement: TBaseElement{Id: "process"},
				},
				TFlowElementsContainer: TFlowElementsContainer{
					StartEvents: []TStartEvent{{
						TEvent: TEvent{TFlowNode: TFlowNode{
							TFlowElement:            TFlowElement{TBaseElement: TBaseElement{Id: "start"}},
							OutgoingAssociationsIDs: []string{"flow-start-task"},
						}},
					}},
					ServiceTasks: []TServiceTask{{
						TExternallyProcessedTask: TExternallyProcessedTask{TTask: TTask{TActivity: TActivity{
							TFlowNode: TFlowNode{
								TFlowElement:            TFlowElement{TBaseElement: TBaseElement{Id: "task"}},
								IncomingAssociationsIDs: []string{"flow-start-task"},
							},
						}}},
					}},
					SequenceFlows: []TSequenceFlow{{
						TFlowElement: TFlowElement{TBaseElement: TBaseElement{Id: "flow-start-task"}},
						SourceRefId:  "start",
						TargetRefId:  "task",
					}},
				},
			},
		},
	}
}