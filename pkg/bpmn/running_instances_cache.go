package bpmn

import (
	"sync"
)

type RunningInstance struct {
	mu      *sync.Mutex
	waiters int64
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

func (c *RunningInstancesCache) lockInstance(instanceKey int64) {
	c.mu.Lock()
	ri, ok := c.processInstances[instanceKey]
	if !ok {
		ri = &RunningInstance{
			mu:      &sync.Mutex{},
			waiters: 0,
		}
		c.processInstances[instanceKey] = ri
	}
	ri.waiters++
	c.mu.Unlock()

	ri.mu.Lock()
}

func (c *RunningInstancesCache) unlockInstance(instanceKey int64) {
	c.mu.Lock()
	ri := c.processInstances[instanceKey]
	ri.mu.Unlock()
	ri.waiters--
	if ri.waiters == 0 {
		delete(c.processInstances, instanceKey)
	}
	c.mu.Unlock()
}
