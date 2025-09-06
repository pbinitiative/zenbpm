// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package bpmn

import (
	"strings"

	"github.com/pbinitiative/feel"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/extensions"
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

func evaluateExpression(expression string, variableContext map[string]interface{}) (interface{}, error) {
	expression = strings.TrimSpace(expression)
	expression = strings.TrimPrefix(expression, "=") // FIXME: this is just for convenience, but should be removed
	res, err := feel.EvalStringWithScope(expression, variableContext)
	if err == nil {
		if num, ok := res.(*feel.Number); ok {
			// TODO: tbc: what about smart conversion to int, in case of integer value?
			return num.Float64(), nil
		}
		if b, ok := res.(bool); ok {
			return b, nil
		}
	}
	return res, err
}

func evaluateLocalVariables(varHolder *runtime.VariableHolder, mappings []extensions.TIoMapping) error {
	return mapVariables(varHolder, mappings, func(key string, value interface{}) {
		varHolder.SetVariable(key, value)
	})
}

func propagateProcessInstanceVariables(varHolder *runtime.VariableHolder, mappings []extensions.TIoMapping) error {
	if len(mappings) == 0 {
		for k, v := range varHolder.Variables() {
			varHolder.PropagateVariable(k, v)
		}
	}
	return mapVariables(varHolder, mappings, func(key string, value interface{}) {
		varHolder.PropagateVariable(key, value)
	})
}

func mapVariables(varHolder *runtime.VariableHolder, mappings []extensions.TIoMapping, setVarFunc func(key string, value interface{})) error {
	for _, mapping := range mappings {
		evalResult, err := evaluateExpression(mapping.Source, varHolder.Variables())
		if err != nil {
			return err
		}
		setVarFunc(mapping.Target, evalResult)
	}
	return nil
}
