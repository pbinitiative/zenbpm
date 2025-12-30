package cluster

import (
	"errors"
	"fmt"

	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
)

type Error struct {
	Result *proto.ErrorResult
	// optional: underlying cause
	Cause error
}

func (e *Error) Error() string {
	if e == nil || e.Result == nil {
		return "<nil>"
	}

	// Pull values safely (because generated fields are pointers)
	var (
		code = proto.ErrorResult_UNSPECIFIED // pick your zero value
		msg  = ""
	)
	if e.Result.Code != nil {
		code = *e.Result.Code
	}
	if e.Result.Message != nil {
		msg = *e.Result.Message
	}

	// nice readable output
	if msg == "" {
		return code.String()
	}
	return fmt.Sprintf("%s: %s", code.String(), msg)
}

// Unwrap lets errors.Is/As walk the chain if you store a Cause.
func (e *Error) Unwrap() error { return e.Cause }

// Helpers
func New(result *proto.ErrorResult) error {
	if result == nil {
		return nil
	}
	return &Error{Result: result}
}

func WithCause(result *proto.ErrorResult, cause error) error {
	if result == nil && cause == nil {
		return nil
	}
	return &Error{Result: result, Cause: cause}
}

func AsErrorResult(err error) (*proto.ErrorResult, bool) {
	var e *Error
	if ok := errors.As(err, &e); !ok || e.Result == nil {
		return nil, false
	}
	return e.Result, true
}
