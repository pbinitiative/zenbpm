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
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := bpmn_engine.NewEngine(bpmn_engine.EngineWithStorage(store))
	zeebeExporter := createExporterWithHazelcastMock()
	bpmnEngine.AddEventExporter(&zeebeExporter)
	process, _ := bpmnEngine.LoadFromFile("../.././test-cases/simple_task.bpmn")
	numberOfHazelcastSendToRingbufferCalls = 0 // reset

	// when
	bpmnEngine.CreateInstance(process, nil)

	assert.Equal(t, 1, numberOfHazelcastSendToRingbufferCalls)
}

func TestPublishNewElementEvent(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	bpmnEngine := bpmn_engine.NewEngine(bpmn_engine.EngineWithStorage(store))
	zeebeExporter := createExporterWithHazelcastMock()
	bpmnEngine.AddEventExporter(&zeebeExporter)
	process, _ := bpmnEngine.LoadFromFile("../.././test-cases/simple_task.bpmn")
	numberOfHazelcastSendToRingbufferCalls = 0 // reset

	bpmnEngine.NewTaskHandler().Id("id").Handler(func(job bpmn_engine.ActivatedJob) {
		job.Complete()
	})

	// when
	bpmnEngine.CreateAndRunInstance(process.ProcessKey, nil)

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
