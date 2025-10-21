package bpmn

import (
	"sync"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

type RunningInstancesCache struct {
	processInstances map[int64]*sync.Mutex
	mu               *sync.Mutex
}

func newRunningInstanceCache() *RunningInstancesCache {
	return &RunningInstancesCache{
		processInstances: map[int64]*sync.Mutex{},
		mu:               &sync.Mutex{},
	}
}

func (c *RunningInstancesCache) lockInstance(instance *runtime.ProcessInstance) {
	c.mu.Lock()
	if ins, ok := c.processInstances[instance.Key]; ok {
		ins.Lock()
	} else {
		c.processInstances[instance.Key] = &sync.Mutex{}
		c.processInstances[instance.Key].Lock()
	}
	c.mu.Unlock()
}

func (c *RunningInstancesCache) unlockInstance(instance *runtime.ProcessInstance) {
	c.processInstances[instance.Key].Unlock()
	c.mu.Lock()
	if _, ok := c.processInstances[instance.Key]; ok {
		c.processInstances[instance.Key].Lock()
		delete(c.processInstances, instance.Key)
	}
	c.mu.Unlock()
}
