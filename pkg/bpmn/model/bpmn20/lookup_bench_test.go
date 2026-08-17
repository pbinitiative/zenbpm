package bpmn20

import (
	"fmt"
	"testing"
)

// BenchmarkFindBaseElementById_Breadth measures the cost of resolving an
// element near the end of a large, wide flow-node collection. A constant
// lookup cost as the model grows is the primary acceptance criterion for
// issue #725.
//
// To compare recursive and indexed costs, the benchmark runs both
// implementations over the same model. The recursive path uses
// TProcess.GetFlowNodeById directly (the production method, whose
// internal slices the loop walks) so the comparison is honest.
func BenchmarkFindBaseElementById_Breadth(b *testing.B) {
	for _, size := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("indexed/size=%d", size), func(b *testing.B) {
			defs := buildWideProcess(size)
			// Look up the very last element in the collection.
			target := fmt.Sprintf("task-%d", size-1)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				elem, ok := FindBaseElementById(defs, target)
				if !ok || elem == nil {
					b.Fatalf("expected to resolve %q in indexed map", target)
				}
				// Sanity check that the indexed path returns the
				// actual slice element — not a copy or a different
				// element with the same id.
				if _, isServiceTask := elem.(*TServiceTask); !isServiceTask {
					b.Fatalf("expected %q to resolve to *TServiceTask, got %T", target, elem)
				}
			}
		})

		b.Run(fmt.Sprintf("recursive/size=%d", size), func(b *testing.B) {
			defs := buildWideProcess(size)
			target := fmt.Sprintf("task-%d", size-1)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				elem := defs.Process.GetFlowNodeById(target)
				if elem == nil {
					b.Fatalf("expected to resolve %q in legacy scan", target)
				}
			}
		})
	}
}

// BenchmarkFindBaseElementById_Depth measures the cost of resolving an
// element nested several subprocess levels deep. The recursive scan
// performs an O(depth * siblings_per_level) walk, while the indexed
// lookup stays O(1) regardless of nesting.
func BenchmarkFindBaseElementById_Depth(b *testing.B) {
	for _, depth := range []int{1, 3, 5} {
		b.Run(fmt.Sprintf("indexed/depth=%d", depth), func(b *testing.B) {
			defs := buildDeepProcess(depth)
			target := deepestTaskID(depth)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				elem, ok := FindBaseElementById(defs, target)
				if !ok || elem == nil {
					b.Fatalf("expected to resolve %q in indexed map", target)
				}
				// Sanity check: the deepest element is a service
				// task, not the leaf subprocess that surrounds it.
				if _, isServiceTask := elem.(*TServiceTask); !isServiceTask {
					b.Fatalf("expected %q to resolve to *TServiceTask, got %T", target, elem)
				}
			}
		})

		b.Run(fmt.Sprintf("recursive/depth=%d", depth), func(b *testing.B) {
			defs := buildDeepProcess(depth)
			target := deepestTaskID(depth)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				elem := defs.Process.GetFlowNodeById(target)
				if elem == nil {
					b.Fatalf("expected to resolve %q in legacy scan", target)
				}
			}
		})
	}
}

// buildWideProcess synthesizes a process that contains `count` service
// tasks, all named "task-0" through "task-(count-1)". The shape is
// deliberately simple: every added byte is a new element in the indexed
// collection.
func buildWideProcess(count int) *TDefinitions {
	tasks := make([]TServiceTask, count)
	for i := 0; i < count; i++ {
		tasks[i] = TServiceTask{
			TExternallyProcessedTask: TExternallyProcessedTask{
				TTask: TTask{
					TActivity: TActivity{
						TFlowNode: TFlowNode{
							TFlowElement: TFlowElement{
								TBaseElement: TBaseElement{
									Id: fmt.Sprintf("task-%d", i),
								},
							},
						},
					},
				},
			},
		}
	}
	defs := &TDefinitions{
		TRootElementsContainer: TRootElementsContainer{
			Process: TProcess{
				TFlowElementsContainer: TFlowElementsContainer{
					ServiceTasks: tasks,
				},
			},
		},
	}
	if err := defs.ResolveReferences(); err != nil {
		panic(fmt.Sprintf("ResolveReferences failed: %v", err))
	}
	return defs
}

// buildDeepProcess synthesizes a process whose subprocess nesting reaches
// exactly `depth` levels. The leaf holds a service task whose id is
// distinct from the surrounding subprocesses' ids, so the lookup target
// unambiguously resolves to the deepest flow node rather than whichever
// element happened to be registered first.
func buildDeepProcess(depth int) *TDefinitions {
	if depth < 1 {
		panic("depth must be >= 1")
	}

	// Build leaf task at the bottom of the nesting. The leaf subprocess
	// and the service task inside it have distinct ids so the index
	// does not collapse them into a single entry.
	leaf := TProcess{
		TCallableElement: TCallableElement{
			TBaseElement: TBaseElement{Id: deepestSubprocessID(depth)},
		},
		TFlowElementsContainer: TFlowElementsContainer{
			ServiceTasks: []TServiceTask{
				{
					TExternallyProcessedTask: TExternallyProcessedTask{
						TTask: TTask{
							TActivity: TActivity{
								TFlowNode: TFlowNode{
									TFlowElement: TFlowElement{
										TBaseElement: TBaseElement{
											Id: deepestTaskID(depth),
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Wrap in N-1 subprocess shells.
	current := leaf
	for level := depth - 1; level >= 0; level-- {
		current = TProcess{
			TCallableElement: TCallableElement{
				TBaseElement: TBaseElement{
					Id: fmt.Sprintf("sub-%d", level),
				},
			},
			TFlowElementsContainer: TFlowElementsContainer{
				SubProcess: []TSubProcess{
					{
						TProcess: current,
					},
				},
			},
		}
	}

	defs := &TDefinitions{
		TRootElementsContainer: TRootElementsContainer{
			Process: current,
		},
	}
	if err := defs.ResolveReferences(); err != nil {
		panic(fmt.Sprintf("ResolveReferences failed: %v", err))
	}
	return defs
}

// deepestTaskID returns the id of the service task placed at the bottom
// of a buildDeepProcess(depth) model. Distinct from the surrounding
// subprocess ids so the indexed lookup target is unambiguous.
func deepestTaskID(depth int) string {
	return fmt.Sprintf("task-deep-%d", depth-1)
}

// deepestSubprocessID returns the id of the innermost subprocess shell in
// a buildDeepProcess(depth) model.
func deepestSubprocessID(depth int) string {
	return fmt.Sprintf("sub-deep-%d", depth-1)
}
