package zenerr

import (
	"errors"
	"fmt"
	"io"

	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/rest/public"
)

var (
	// ErrNotOpen is returned when a Store is not open.
	ErrNotOpen = errors.New("store not open")

	// ErrAlreadyOpen is returned when a Store is already open.
	ErrAlreadyOpen = errors.New("store already open")

	// ErrNotLeader is returned when a node attempts to execute a leader-only
	// operation.
	ErrNotLeader = errors.New("not leader")

	// ErrNodeNotFound is returned when requested node is not found in the cluster.
	ErrNodeNotFound = errors.New("node not found")

	// ErrWaitForLeaderTimeout is returned when the Store cannot determine the leader
	// within the specified time.
	ErrWaitForLeaderTimeout = errors.New("timeout waiting for leader")

	// ErrApplyUncertain is returned when a raft command was handed to the log
	// but its outcome could not be confirmed (deadline, leadership lost). The
	// command may still commit later.
	ErrApplyUncertain = errors.New("raft apply outcome unknown")

	// ErrResourceLimit is returned when a restore input exceeds a configured
	// size limit.
	ErrResourceLimit = errors.New("restore resource limit exceeded")
)

type ZenErrorCode uint32

const (
	NoErrorCode ZenErrorCode = iota
	TechnicalErrorCode
	ClusterErrorCode
	NotFoundCode
	BadRequestCode
	ConflictCode
	MethodNotAllowedCode
	UnsupportedMediaTypeCode
	PayloadTooLargeCode
	// UnavailableCode marks a transient condition on the target node: the
	// partition engine has not started (yet) or its store is not open. Such
	// calls are safe to retry against the current partition leader.
	UnavailableCode
)

func (zenErrorCode ZenErrorCode) ToString() string {
	switch zenErrorCode {
	case NoErrorCode:
		return ""
	case TechnicalErrorCode:
		return "TECHNICAL_ERROR"
	case ClusterErrorCode:
		return "CLUSTER_ERROR"
	case NotFoundCode:
		return "NOT_FOUND"
	case BadRequestCode:
		return "BAD_REQUEST"
	case ConflictCode:
		return "CONFLICT"
	case MethodNotAllowedCode:
		return "METHOD_NOT_ALLOWED"
	case UnsupportedMediaTypeCode:
		return "UNSUPPORTED_MEDIA_TYPE"
	case PayloadTooLargeCode:
		return "PAYLOAD_TOO_LARGE"
	case UnavailableCode:
		return "UNAVAILABLE"
	default:
		return "UNKNOWN_ERROR"
	}
}

type ZenError struct {
	Code ZenErrorCode
	err  error
}

func (zenError *ZenError) Error() string {
	return zenError.err.Error()
}

// Unwrap exposes the wrapped cause so errors.Is / errors.As see sentinels
// (context.Canceled, zenerr.ErrNotLeader, ...) through a ZenError.
func (zenError *ZenError) Unwrap() error {
	return zenError.err
}

func TechnicalError(err error) *ZenError {
	return &ZenError{TechnicalErrorCode, err}
}

func ClusterError(err error) *ZenError {
	return &ZenError{ClusterErrorCode, err}
}

func NotFound(err error) *ZenError {
	return &ZenError{NotFoundCode, err}
}

func BadRequest(err error) *ZenError {
	return &ZenError{BadRequestCode, err}
}

func Conflict(err error) *ZenError {
	return &ZenError{ConflictCode, err}
}

// Unavailable marks a transient failure on the target node (engine not yet
// started, store not open) that the caller may retry.
func Unavailable(err error) *ZenError {
	return &ZenError{UnavailableCode, err}
}

// IsUnavailable reports whether err (or any error it wraps) carries
// UnavailableCode.
func IsUnavailable(err error) bool {
	var zerr *ZenError
	return errors.As(err, &zerr) && zerr.Code == UnavailableCode
}

func Join(new error, original *ZenError) *ZenError {
	return &ZenError{original.Code, errors.Join(new, original.err)}
}

func (zenError *ZenError) ToProtoError() *proto.ErrorResult {
	return &proto.ErrorResult{
		Code:    (*uint32)(new(zenError.Code)),
		Message: new(zenError.err.Error()),
	}
}

// CloseJoin closes c and joins a close failure into *err so it is neither
// dropped nor allowed to mask the primary error. It is meant to be deferred
// in a function with a named error result:
//
//	defer zenerr.CloseJoin(f, &err, "spool file")
func CloseJoin(c io.Closer, err *error, what string) {
	if closeErr := c.Close(); closeErr != nil {
		*err = errors.Join(*err, fmt.Errorf("failed to close %s: %w", what, closeErr))
	}
}

func (zenError *ZenError) ToApiError() public.Error {
	return public.Error{
		Code:    zenError.Code.ToString(),
		Message: zenError.err.Error(),
	}
}

func ToZenError(protoError *proto.ErrorResult, err ...error) *ZenError {
	var resErr = errors.Join(err...)
	if protoError.Message != nil {
		protoErr := errors.New(protoError.GetMessage())
		if resErr != nil {
			resErr = errors.Join(resErr, protoErr)
		} else {
			resErr = protoErr
		}
	}
	code := TechnicalErrorCode
	if protoError.Code != nil {
		code = ZenErrorCode(*protoError.Code)
	}
	return &ZenError{code, resErr}
}
