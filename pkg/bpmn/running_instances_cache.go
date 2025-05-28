package bpmn

import (
	"sync"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

type RunningInstancesCache struct {
	processInstances map[int64]*runtime.ProcessInstance
	mu               sync.RWMutex
}

func (c *RunningInstancesCache) addInstance(instance *runtime.ProcessInstance) {
	c.mu.Lock()
	c.processInstances[instance.Key] = instance
	c.mu.Unlock()
}

func (c *RunningInstancesCache) removeInstance(instance *runtime.ProcessInstance) {
	c.mu.Lock()
	delete(c.processInstances, instance.Key)
	c.mu.Unlock()
}

func (c *RunningInstancesCache) hasRunningKey(key int64) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if _, ok := c.processInstances[key]; ok {
		return true
	}
	return false
}

func (c *RunningInstancesCache) getRunningKeys() []int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]int64, 0, len(c.processInstances))
	for key := range c.processInstances {
		keys = append(keys, key)
	}
	return keys
}
