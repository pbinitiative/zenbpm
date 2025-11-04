package bpmn

import (
	"sync"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

type RunningInstance struct {
	instance *runtime.ProcessInstance
	mu       *sync.Mutex
	waiters  int64
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
	ri, ok := c.processInstances[instance.Key]
	if !ok {
		ri = &RunningInstance{
			instance: instance,
			mu:       &sync.Mutex{},
			waiters:  0,
		}
		c.processInstances[instance.Key] = ri
	}
	ri.waiters++
	c.mu.Unlock()

	ri.mu.Lock()
}

func (c *RunningInstancesCache) unlockInstance(instance *runtime.ProcessInstance) {
	c.mu.Lock()
	ri := c.processInstances[instance.Key]
	ri.mu.Unlock()
	ri.waiters--
	if ri.waiters == 0 {
		delete(c.processInstances, instance.Key)
	}
	c.mu.Unlock()
}
