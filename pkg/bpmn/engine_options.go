package bpmn

import (
	"fmt"

	"github.com/pbinitiative/zenbpm/internal/rqlite"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/exporter"
	"github.com/pbinitiative/zenbpm/pkg/storage"
)

type EngineOption = func(*Engine)

// New creates a new instance of the BPMN Engine;
func New(options ...EngineOption) Engine {

	name := fmt.Sprintf("Bpmn-Engine-%d", getGlobalSnowflakeIdGenerator().Generate().Int64())
	engine := Engine{
		name:         name,
		taskHandlers: []*taskHandler{},
		snowflake:    getGlobalSnowflakeIdGenerator(),
		exporters:    []exporter.EventExporter{},
		persistence:  nil,
	}

	for _, option := range options {
		option(&engine)
	}

	return engine
}

func WithExporter(exporter exporter.EventExporter) EngineOption {
	return func(engine *Engine) { engine.AddEventExporter(exporter) }
}

func WithStorage(persistence storage.PersistentStorage) EngineOption {
	return func(engine *Engine) {
		rqliteService := rqlite.NewPersistenceRqlite(persistence)
		engine.persistence = NewBpmnEnginePersistenceRqlite(getGlobalSnowflakeIdGenerator(), rqliteService)
	}
}

func WithName(name string) EngineOption {
	return func(engine *Engine) {
		engine.name = name
	}
}
