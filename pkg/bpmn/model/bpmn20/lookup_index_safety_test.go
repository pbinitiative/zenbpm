package bpmn20

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFindBaseElementByIdUnindexedUsesReadOnlyFallback(t *testing.T) {
	definitions := unindexedLookupDefinition()
	require.Nil(t, definitions.baseElements)

	task, ok := FindBaseElementById(&definitions, "task")
	require.True(t, ok)
	assert.Same(t, &definitions.Process.ServiceTasks[0], task)

	flow, ok := FindBaseElementById(&definitions, "flow-start-task")
	require.True(t, ok)
	assert.Same(t, &definitions.Process.SequenceFlows[0], flow)

	_, ok = FindBaseElementById(&definitions, "does-not-exist")
	assert.False(t, ok)

	assert.Nil(t, definitions.baseElements,
		"fallback lookup must not initialize or mutate the definition")
	assert.Empty(t, definitions.Process.StartEvents[0].OutgoingAssociations,
		"fallback lookup must not resolve references as a side effect")
}

func TestFindBaseElementByIdUnindexedCopiesAreConcurrencySafe(t *testing.T) {
	definitions := unindexedLookupDefinition()
	first := definitions
	second := definitions

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	for _, definition := range []*TDefinitions{&first, &second} {
		go func() {
			defer wg.Done()
			<-start
			element, ok := FindBaseElementById(definition, "task")
			assert.True(t, ok)
			assert.NotNil(t, element)
		}()
	}
	close(start)
	wg.Wait()
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
