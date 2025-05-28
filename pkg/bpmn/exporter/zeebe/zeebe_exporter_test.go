package zeebe

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"

	bpmn_engine "github.com/pbinitiative/zenbpm/pkg/bpmn"
)

var numberOfHazelcastSendToRingbufferCalls = 0

func TestPublishNewProAcessEvent(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := bpmn_engine.NewEngine(bpmn_engine.EngineWithStorage(store))
	zeebeExporter := createExporterWithHazelcastMock()
	bpmnEngine.AddEventExporter(&zeebeExporter)

	// when
	bpmnEngine.LoadFromFile("../.././test-cases/simple_task.bpmn")

	assert.Equal(t, 1, numberOfHazelcastSendToRingbufferCalls)
}

func TestPublishNewProcessInstanceEvent(t *testing.T) {
	t.Skip("reimplement exporters")
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := bpmn_engine.NewEngine(bpmn_engine.EngineWithStorage(store))
	zeebeExporter := createExporterWithHazelcastMock()
	bpmnEngine.AddEventExporter(&zeebeExporter)
	process, _ := bpmnEngine.LoadFromFile("../.././test-cases/simple_task.bpmn")
	numberOfHazelcastSendToRingbufferCalls = 0 // reset

	// when
	_, err := bpmnEngine.CreateInstance(t.Context(), process, nil)
	assert.NoError(t, err)

	assert.Equal(t, 1, numberOfHazelcastSendToRingbufferCalls)
}

func TestPublishNewElementEvent(t *testing.T) {
	t.Skip("reimplement exporters")
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := bpmn_engine.NewEngine(bpmn_engine.EngineWithStorage(store))
	zeebeExporter := createExporterWithHazelcastMock()
	bpmnEngine.AddEventExporter(&zeebeExporter)
	process, _ := bpmnEngine.LoadFromFile("../.././test-cases/simple_task.bpmn")
	numberOfHazelcastSendToRingbufferCalls = 0 // reset

	h := bpmnEngine.NewTaskHandler().Id("id").Handler(func(job bpmn_engine.ActivatedJob) {
		job.Complete()
	})
	defer bpmnEngine.RemoveHandler(h)

	// when
	_, err := bpmnEngine.CreateInstanceByKey(t.Context(), process.Key, nil)
	assert.NoError(t, err)

	assert.Less(t, 1, numberOfHazelcastSendToRingbufferCalls)
}

func createExporterWithHazelcastMock() exporter {
	numberOfHazelcastSendToRingbufferCalls = 0
	zeebeExporter := exporter{
		hazelcast: Hazelcast{
			sendToRingbufferFunc: func(data []byte) error {
				numberOfHazelcastSendToRingbufferCalls++
				return nil
			},
		},
	}
	return zeebeExporter
}
