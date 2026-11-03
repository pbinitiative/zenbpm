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

// ProcessSuspendedError is returned when a new instance is requested of a process which is suspended, by the
// calls which create an instance and by those which publish the message of a start event of the process.
// A call activity does not return it: the instance which makes the call exists and has an incident.
type ProcessSuspendedError struct {
	BpmnProcessId string
}

func (e *ProcessSuspendedError) Error() string {
	return fmt.Sprintf("process %q is suspended, no new instance can be started", e.BpmnProcessId)
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

var InvalidStateError = fmt.Errorf("InvalidStateError")
