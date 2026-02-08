package zenerr

import (
	"errors"
	"fmt"

	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
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
)

type ZenErrorCode uint32

const (
	NoErrorCode ZenErrorCode = iota
	TechnicalErrorCode
	ClusterErrorCode
	NotFoundCode
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
	default:
		panic(fmt.Sprintf("unknown zen error code: %v", zenErrorCode))
	}
}

type ZenError struct {
	Code ZenErrorCode
	err  error
}

func (zenError *ZenError) Error() string {
	return zenError.err.Error()
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

func (zenError *ZenError) ToProtoError() *proto.ErrorResult {
	return &proto.ErrorResult{
		Code:    (*uint32)(ptr.To(zenError.Code)),
		Message: ptr.To(zenError.err.Error()),
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
		resErr = fmt.Errorf("%w %v", resErr, protoError.GetMessage())
	} else {
		resErr = fmt.Errorf("%w", resErr)
	}
	return &ZenError{ZenErrorCode(*protoError.Code), resErr}
}
