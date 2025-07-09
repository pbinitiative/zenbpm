package bpmn

import (
	"sync"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

type RunningInstance struct {
	instance *runtime.ProcessInstance
	mu       *sync.RWMutex
}

type RunningInstancesCache struct {
	processInstances map[int64]*RunningInstance
	mu               *sync.RWMutex
}

func (c *RunningInstancesCache) addInstance(instance *runtime.ProcessInstance) {
	c.mu.Lock()
	if ins, ok := c.processInstances[instance.Key]; ok {
		ins.mu.Lock()
	} else {
		c.processInstances[instance.Key] = &RunningInstance{
			instance: instance,
			mu:       &sync.RWMutex{},
		}
		c.processInstances[instance.Key].mu.Lock()
	}
	c.mu.Unlock()
}

func (c *RunningInstancesCache) removeInstance(instance *runtime.ProcessInstance) {
	c.mu.Lock()
	c.processInstances[instance.Key].mu.Unlock()
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
