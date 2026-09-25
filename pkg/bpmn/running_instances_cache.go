package bpmn

import (
	"context"
	"fmt"
	"sync"
	"time"
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

func (c *RunningInstancesCache) registerWaiter(instanceKey int64) *RunningInstance {
	c.mu.Lock()
	defer c.mu.Unlock()
	ri, ok := c.processInstances[instanceKey]
	if !ok {
		ri = &RunningInstance{
			mu:      &sync.Mutex{},
			waiters: 0,
		}
		c.processInstances[instanceKey] = ri
	}
	ri.waiters++
	return ri
}

func (c *RunningInstancesCache) unregisterWaiter(instanceKey int64, ri *RunningInstance) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ri.waiters--
	if ri.waiters == 0 {
		delete(c.processInstances, instanceKey)
	}
}

func (c *RunningInstancesCache) tryLockInstance(ctx context.Context, instanceKey int64) error {
	ri := c.registerWaiter(instanceKey)

	triedLockCount := 0
	for {
		select {
		case <-ctx.Done():
			c.unregisterWaiter(instanceKey, ri)
			return fmt.Errorf("context canceled")
		default:
			locked := ri.mu.TryLock()
			if locked {
				return nil
			}
			triedLockCount++
			if triedLockCount > 5 {
				c.unregisterWaiter(instanceKey, ri)
				return fmt.Errorf("tried locking process instance %d, failed after 6 attempts", instanceKey)
			}
			time.Sleep(time.Millisecond * 100)
		}
	}
}

// tryLockInstanceOnce attempts to acquire an instance lock exactly once. It is
// used by background reconciliation, which must skip live work instead of
// queueing behind it.
func (c *RunningInstancesCache) tryLockInstanceOnce(instanceKey int64) bool {
	ri := c.registerWaiter(instanceKey)
	if ri.mu.TryLock() {
		return true
	}
	c.unregisterWaiter(instanceKey, ri)
	return false
}

func (c *RunningInstancesCache) lockInstance(instanceKey int64) {
	ri := c.registerWaiter(instanceKey)
	ri.mu.Lock()
}

func (c *RunningInstancesCache) unlockInstance(instanceKey int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ri := c.processInstances[instanceKey]
	ri.mu.Unlock()
	ri.waiters--
	if ri.waiters == 0 {
		delete(c.processInstances, instanceKey)
	}
}
