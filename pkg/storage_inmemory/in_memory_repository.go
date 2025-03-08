package storage_inmemory

import (
	"context"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"sort"
)

type InMemoryStorage struct {
	processDefinitions []*storage.ProcessDefinition
	processInstances   []*storage.ProcessInstance
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

func (mem *InMemoryStorage) SaveProcessDefinition(ctx context.Context, definition *storage.ProcessDefinition) error {
	for i, d := range mem.processDefinitions {
		if d.ProcessKey == definition.ProcessKey {
			mem.processDefinitions[i] = definition
			return nil
		}
	}
	mem.processDefinitions = append(mem.processDefinitions, definition)
	return nil
}

func (mem *InMemoryStorage) FindProcessInstancesByKey(ctx context.Context, processInstanceKeys ...int64) (instances []storage.ProcessInstance, err error) {
	for _, i := range mem.processInstances {
		if contains(processInstanceKeys, i.InstanceKey) {
			instances = append(instances, *i)
		}
	}
	return instances, nil
}

func (mem *InMemoryStorage) SaveProcessInstance(ctx context.Context, processInstance *storage.ProcessInstance) error {
	for i, d := range mem.processInstances {
		if d.InstanceKey == processInstance.InstanceKey {
			mem.processInstances[i] = processInstance
			return nil
		}
	}
	mem.processInstances = append(mem.processInstances, processInstance)
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
