package bpmn

import (
	"fmt"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/exporter"
	rqliteExporter "github.com/pbinitiative/zenbpm/pkg/bpmn/exporter/rqlite"
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

func WithRqliteExporter() EngineOption {
	return func(engine *Engine) {
		exp, err := rqliteExporter.NewExporter(engine.GetPersistence())
		if err != nil {

		}
		engine.AddEventExporter(&exp)
	}
}

func WithStorage(persistence storage.PersistentStorageNew) EngineOption {
	return func(engine *Engine) {
		// rqliteService := rqlite.NewPersistenceRqlite(persistence)
		// engine.persistence = NewBpmnEnginePersistenceRqlite(getGlobalSnowflakeIdGenerator(), rqliteService)
		engine.persistence = persistence
	}
}

func WithName(name string) EngineOption {
	return func(engine *Engine) {
		engine.name = name
	}
}
