// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package bpmn

import "fmt"

type BpmnEngineError struct {
	Msg string
}

func (e *BpmnEngineError) Error() string {
	return e.Msg
}

// newEngineErrorf uses fmt.Sprintf(format, a...) to format the message
func newEngineErrorf(format string, a ...interface{}) error {
	return &BpmnEngineError{
		Msg: fmt.Sprintf(format, a...),
	}
}

type BpmnEngineUnmarshallingError struct {
	Msg string
	Err error
}

func (e *BpmnEngineUnmarshallingError) Error() string {
	if len(e.Msg) > 0 {
		return e.Msg + ": " + e.Err.Error()
	}
	return e.Err.Error()
}

type ExpressionEvaluationError struct {
	Msg string
	Err error
}

func (e *ExpressionEvaluationError) Error() string {
	if e.Err != nil {
		return e.Msg + "\nerror: " + e.Err.Error()
	}
	return e.Msg
}
