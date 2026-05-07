package bpmn

import (
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
)

type unknownFlowNode struct {
	bpmn20.TFlowNode
}

func (u *unknownFlowNode) GetId() string                          { return "unknown-id" }
func (u *unknownFlowNode) GetDocumentation() []bpmn20.Documentation { return nil }
func (u *unknownFlowNode) GetType() bpmn20.ElementType            { return "UnknownType" }
func (u *unknownFlowNode) GetName() string                        { return "unknown" }

func TestProcessFlowNode_UnsupportedElementReturnsError(t *testing.T) {
	engineBatch, err := bpmnEngine.NewEngineBatchClean()
	assert.NoError(t, err)

	activity := &elementActivity{
		key:     1,
		state:   runtime.ActivityStateReady,
		element: &unknownFlowNode{},
	}

	instance := &runtime.DefaultProcessInstance{}

	_, err = bpmnEngine.processFlowNode(t.Context(), &engineBatch, instance, activity, runtime.ExecutionToken{})
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "unsupported element"), "error should mention unsupported element, got: %s", err.Error())
}

func TestSafeGo_GoroutinePanicDoesNotCrashEngine(t *testing.T) {
	isolatedStorage := inmemory.NewStorage()
	eng := NewEngine(EngineWithStorage(isolatedStorage))
	eng.Start(t.Context())

	process, err := eng.LoadFromFile(t.Context(), "./test-cases/safego-sub-process-panic-test.bpmn")
	assert.NoError(t, err)

	panicFired := false
	panicHandler := eng.NewTaskHandler().Id("SubTask_1").Handler(func(job ActivatedJob) {
		panicFired = true
		panic("intentional panic inside subprocess goroutine")
	})
	defer eng.RemoveHandler(panicHandler)

	_, err = eng.CreateInstanceByKey(t.Context(), process.Key, map[string]any{})
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		return panicFired
	}, 2*time.Second, 50*time.Millisecond, "panic handler should have been called")

	eng.RemoveHandler(panicHandler)
	normalHandler := eng.NewTaskHandler().Id("SubTask_1").Handler(func(job ActivatedJob) {
		job.Complete()
	})
	defer eng.RemoveHandler(normalHandler)

	instance2, err := eng.CreateInstanceByKey(t.Context(), process.Key, map[string]any{})
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		pi, ok := isolatedStorage.ProcessInstances[instance2.ProcessInstance().Key]
		return ok && pi.ProcessInstance().State == runtime.ActivityStateCompleted
	}, 3*time.Second, 50*time.Millisecond, "engine should still process instances after goroutine panic")
}
