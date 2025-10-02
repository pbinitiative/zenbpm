package bpmn

import (
	"sync"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

type RunningInstance struct {
	instance *runtime.ProcessInstance
	mu       *sync.Mutex
}

type RunningInstancesCache struct {
	processInstances map[int64]*RunningInstance
	mu               *sync.Mutex
}

func newRunningInstanceCache() *RunningInstancesCache {
	return &RunningInstancesCache{
		processInstances: map[int64]*RunningInstance{},
		mu:               &sync.Mutex{},
	}
}

func (c *RunningInstancesCache) lockInstance(instance *runtime.ProcessInstance) {
	c.mu.Lock()
	if ins, ok := c.processInstances[instance.Key]; ok {
		ins.mu.Lock()
	} else {
		c.processInstances[instance.Key] = &RunningInstance{
			instance: instance,
			mu:       &sync.Mutex{},
		}
		c.processInstances[instance.Key].mu.Lock()
	}
	c.mu.Unlock()
}

func (c *RunningInstancesCache) unlockInstance(instance *runtime.ProcessInstance) {
	c.mu.Lock()
	c.processInstances[instance.Key].mu.Unlock()
	delete(c.processInstances, instance.Key)
	c.mu.Unlock()
}
