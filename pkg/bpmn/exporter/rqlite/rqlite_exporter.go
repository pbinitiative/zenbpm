package rqlite

import (
	"context"
	"log"
	"time"

	"github.com/bwmarrin/snowflake"
	bpmnEngineExporter "github.com/pbinitiative/zenbpm/pkg/bpmn/exporter"

	rqlitePersitence "github.com/pbinitiative/zenbpm/internal/rqlite"
	"github.com/pbinitiative/zenbpm/internal/rqlite/sql"
)

const noInstanceKey = -1

type exporter struct {
	persistence        rqlitePersitence.BpmnEnginePersistence
	snowflakeGenerator *snowflake.Node
}

// NewExporter creates an exporter with a default Rqlite client.
// The default settings of a Rqlite client are using localhost:28015 as target for the Rqlite server
// it will return an error, when the connection can't be established to the Rqlite server
func NewExporter(rqlite rqlitePersitence.BpmnEnginePersistence) (exporter, error) {

	return NewExporterWithRqliteClient(rqlite)
}

// NewExporterWithRqliteClient creates an exporter with the given Rqlite client.
// it will return any connection or RingBuffer error
func NewExporterWithRqliteClient(rqlite rqlitePersitence.BpmnEnginePersistence) (exporter, error) {
	snowflakeGenerator, err := snowflake.NewNode(1)
	if err != nil {
		return exporter{}, err
	}
	return exporter{
		persistence:        rqlite,
		snowflakeGenerator: snowflakeGenerator,
	}, nil
}

func (e *exporter) NewProcessEvent(event *bpmnEngineExporter.ProcessEvent) {

	log.Println("rqlite exporter: NewProcessEvent - not implemented yet")
}

func (e *exporter) EndProcessEvent(event *bpmnEngineExporter.ProcessInstanceEvent) {
	log.Println("rqlite exporter: EndProcessEvent - not implemented yet")
}

func (e *exporter) NewProcessInstanceEvent(event *bpmnEngineExporter.ProcessInstanceEvent) {
	log.Println("rqlite exporter: NewProcessInstanceEvent - not implemented yet")
}

func (e *exporter) NewElementEvent(event *bpmnEngineExporter.ProcessInstanceEvent, elementInfo *bpmnEngineExporter.ElementInfo) {
	// processInstanceRecord := ProcessInstanceRecord{
	// 	Metadata: &RecordMetadata{
	// 		PartitionId:          1,
	// 		Position:             e.position,
	// 		Key:                  event.ProcessInstanceKey,
	// 		Timestamp:            time.Now().UnixMilli(),
	// 		RecordType:           RecordMetadata_EVENT,
	// 		Intent:               elementInfo.Intent,
	// 		ValueType:            RecordMetadata_PROCESS_INSTANCE,
	// 		SourceRecordPosition: e.position,
	// 		RejectionReason:      "NULL_VAL",
	// 	},
	// 	BpmnProcessId:            event.ProcessId,
	// 	Version:                  event.Version,
	// 	ProcessDefinitionKey:     event.ProcessKey,
	// 	ProcessInstanceKey:       event.ProcessInstanceKey,
	// 	ElementId:                elementInfo.ElementId,
	// 	FlowScopeKey:             event.ProcessInstanceKey,
	// 	BpmnElementType:          elementInfo.BpmnElementType,
	// 	ParentProcessInstanceKey: noInstanceKey,
	// 	ParentElementInstanceKey: noInstanceKey,
	// }
	activity := sql.ActivityInstance{
		Key:                  e.snowflakeGenerator.Generate().Int64(),
		ProcessInstanceKey:   event.ProcessInstanceKey,
		ProcessDefinitionKey: event.ProcessKey,
		CreatedAt:            time.Now().Unix(),
		State:                elementInfo.Intent,
		ElementID:            elementInfo.ElementId,
		BpmnElementType:      elementInfo.BpmnElementType,
	}
	e.persistence.SaveActivity(context.Background(), activity)

}
