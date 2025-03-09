package bpmn

import (
	"fmt"
	"github.com/pbinitiative/zenbpm/internal/rqlite"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/exporter"
	"github.com/pbinitiative/zenbpm/pkg/storage"
)

type engineBuilderCommand struct {
	name     string
	store    storage.PersistentStorage
	exporter exporter.EventExporter
}

type EngineBuilderCommand interface {
	WithExporter(exporter exporter.EventExporter) EngineBuilderCommand
	WithName(name string) EngineBuilderCommand
	WithStorage(persistence storage.PersistentStorage) EngineBuilderCommand
	Engine() BpmnEngineState
}

// New creates a new instance of the BPMN Engine;
func New() EngineBuilderCommand {
	return &engineBuilderCommand{
		name: fmt.Sprintf("Bpmn-Engine-%d", getGlobalSnowflakeIdGenerator().Generate().Int64()),
	}
}

func (b *engineBuilderCommand) WithExporter(exporter exporter.EventExporter) EngineBuilderCommand {
	b.exporter = exporter
	return b
}

func (b *engineBuilderCommand) WithStorage(persistence storage.PersistentStorage) EngineBuilderCommand {
	b.store = persistence
	return b
}

func (b *engineBuilderCommand) WithName(name string) EngineBuilderCommand {
	b.name = name
	return b
}

func (b *engineBuilderCommand) Engine() BpmnEngineState {
	// TODO: this should be removed and replaced by calls to store, using WithStorage()
	rqliteService := rqlite.NewBpmnEnginePersistenceRqlite(b.store)

	engine := BpmnEngineState{
		name:         b.name,
		taskHandlers: []*taskHandler{},
		snowflake:    getGlobalSnowflakeIdGenerator(),
		exporters:    []exporter.EventExporter{},
		persistence:  NewBpmnEnginePersistenceRqlite(getGlobalSnowflakeIdGenerator(), rqliteService),
	}

	if b.exporter != nil {
		engine.AddEventExporter(b.exporter)
	}
	return engine
}
