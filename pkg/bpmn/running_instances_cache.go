// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

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

func (c *RunningInstancesCache) lockInstance(instance *runtime.ProcessInstance) {
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

func (c *RunningInstancesCache) unlockInstance(instance *runtime.ProcessInstance) {
	c.mu.Lock()
	c.processInstances[instance.Key].mu.Unlock()
	delete(c.processInstances, instance.Key)
	c.mu.Unlock()
}
