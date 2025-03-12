package storage_inmemory

import (
	"context"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"sort"
)

// InMemoryStorage keeps process information in memory,
// please use New to create a new object of this type.
type InMemoryStorage struct {
	processDefinitions map[string]storage.ProcessDefinition
	processInstances   map[int64]storage.ProcessInstance
}

func New() InMemoryStorage {
	return InMemoryStorage{
		processDefinitions: make(map[string]storage.ProcessDefinition),
		processInstances:   make(map[int64]storage.ProcessInstance),
	}
}

func (mem *InMemoryStorage) FindProcessDefinitionsById(ctx context.Context, processIds ...string) (definitions []storage.ProcessDefinition, err error) {
	for _, d := range mem.processDefinitions {
		if contains(processIds, d.BpmnProcessId()) {
			definitions = append(definitions, d)
		}
	}
	sort.Slice(definitions, func(i, j int) bool {
		return definitions[i].Version() < definitions[j].Version()
	})
	return definitions, nil
}

func (mem *InMemoryStorage) SaveProcessDefinition(ctx context.Context, definition storage.ProcessDefinition) error {
	for i, d := range mem.processDefinitions {
		if d.ProcessKey() == definition.ProcessKey() {
			mem.processDefinitions[i] = definition
			return nil
		}
	}
	mem.processDefinitions[definition.BpmnChecksum()] = definition
	return nil
}

func (mem *InMemoryStorage) FindProcessInstancesByKey(ctx context.Context, processInstanceKeys ...int64) (instances []storage.ProcessInstance, err error) {
	for _, i := range mem.processInstances {
		if contains(processInstanceKeys, i.InstanceKey()) {
			instances = append(instances, i)
		}
	}
	return instances, nil
}

func (mem *InMemoryStorage) SaveProcessInstance(ctx context.Context, processInstance storage.ProcessInstance) error {
	for i, d := range mem.processInstances {
		if d.InstanceKey() == processInstance.InstanceKey() {
			mem.processInstances[i] = processInstance
			return nil
		}
	}
	mem.processInstances[processInstance.InstanceKey()] = processInstance
	return nil
}

func contains[T comparable](elems []T, v T) bool {
	for _, s := range elems {
		if v == s {
			return true
		}
	}
	return false
}
