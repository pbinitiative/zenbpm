package storage_inmemory

import (
	"context"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"sort"
)

type InMemoryStorage struct {
	processDefinitions []*storage.ProcessDefinition
}

func (mem *InMemoryStorage) FindProcessDefinitionsById(ctx context.Context, processIds ...string) (definitions []storage.ProcessDefinition, err error) {
	for _, d := range mem.processDefinitions {
		if contains(processIds, d.BpmnProcessId) {
			definitions = append(definitions, *d)
		}
	}
	sort.Slice(definitions, func(i, j int) bool {
		return definitions[i].Version < definitions[j].Version
	})
	return definitions, nil
}

func (mem *InMemoryStorage) SaveProcessDefinition(ctx context.Context, definition storage.ProcessDefinition) error {
	for _, d := range mem.processDefinitions {
		if d.ProcessKey == definition.ProcessKey {
			return nil
		}
	}
	mem.processDefinitions = append(mem.processDefinitions, &definition)
	return nil
}

func contains(strings []string, s string) bool {
	for _, aString := range strings {
		if aString == s {
			return true
		}
	}
	return false
}
