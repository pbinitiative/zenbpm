package bpmn

import (
	"errors"
	"fmt"
)

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

// TimerExpressionError tells that the FEEL expression of a timer value
// (timeDate, timeDuration, timeCycle) did not produce a timer: it failed to
// evaluate, evaluated to null, as a reference to a missing variable does, or
// evaluated to something its kind of timer cannot take. Its text becomes the
// message of the incident. It names the element, the expression and, when the
// result is a string, number or boolean, that result, which is what has to be
// corrected; it does not list the variables of the instance, and a result
// which is a context or a list is named by its kind only.
type TimerExpressionError struct {
	// Kind is the BPMN element holding the expression: timeDate, timeDuration
	// or timeCycle.
	Kind       string
	ElementId  string
	Expression string
	// Reason completes the sentence "<kind> expression <expression> of element
	// <id> ...".
	Reason string
	Err    error
}

func (e *TimerExpressionError) Error() string {
	message := fmt.Sprintf("%s expression %q of element %s %s", e.Kind, e.Expression, e.ElementId, e.Reason)
	if e.Err != nil {
		return message + ": " + e.Err.Error()
	}
	return message
}

func (e *TimerExpressionError) Unwrap() error {
	return e.Err
}

// isTimerExpressionError reports whether err is, or wraps, the error of a
// timer value which is a FEEL expression, as opposed to that of a literal.
func isTimerExpressionError(err error) bool {
	var expressionErr *TimerExpressionError
	return errors.As(err, &expressionErr)
}

var InvalidStateError = fmt.Errorf("InvalidStateError")
