// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package runtime

import (
	"maps"
	"sync"
)

type VariableHolder struct {
	parent    *VariableHolder
	variables map[string]interface{}
	mu        sync.RWMutex
}

// NewVariableHolder creates a new VariableHolder with a given parent and variables map.
// All variables from parent holder are copied into this one.
// (hint: the copy is necessary, due to the fact we need to have all variables for expression evaluation in one map)
func NewVariableHolder(parent *VariableHolder, variables map[string]interface{}) VariableHolder {
	if variables == nil {
		variables = make(map[string]interface{})
	}
	if parent != nil {
		parentVars := parent.snapshot()
		maps.Copy(variables, parentVars)
		//maps.Copy(variables, parent.variables)
	}
	return VariableHolder{
		parent:    parent,
		variables: variables,
	}
}

/*
NewVariableHolderForPropagation creates a new VariableHolder with a given parent and variables map. Used in job completion.
*/
func NewVariableHolderForPropagation(parent *VariableHolder, variables map[string]interface{}) VariableHolder {
	if variables == nil {
		variables = make(map[string]interface{})
	}
	return VariableHolder{
		parent:    parent,
		variables: variables,
	}
}

func (vh *VariableHolder) GetVariable(key string) interface{} {
	vh.mu.RLock()
	defer vh.mu.RUnlock()
	if v, ok := vh.variables[key]; ok {
		return v
	}
	return nil
}

func (vh *VariableHolder) SetVariable(key string, val interface{}) {
	vh.mu.Lock()
	defer vh.mu.Unlock()
	vh.variables[key] = val
}

// PropagateVariable set a value with given key to the parent VariableHolder
func (vh *VariableHolder) PropagateVariable(key string, value interface{}) {
	vh.mu.Lock()
	defer vh.mu.Unlock()
	if vh.parent != nil {
		vh.parent.SetVariable(key, value)
	}
}

// Variables return all variables within this holder
func (vh *VariableHolder) Variables() map[string]interface{} {
	// return vh.variables
	return vh.snapshot()
}

// snapshot returns a shallow copy of the current variables under read lock.
func (vh *VariableHolder) snapshot() map[string]interface{} {
	vh.mu.RLock()
	defer vh.mu.RUnlock()
	cp := make(map[string]interface{}, len(vh.variables))
	for k, v := range vh.variables {
		cp[k] = v
	}
	return cp
}
