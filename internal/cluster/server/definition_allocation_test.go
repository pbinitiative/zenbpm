package server

import (
	"context"
	"errors"
	"testing"

	protoc "github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAllocateProcessDefinitionAnswersTheAllocation(t *testing.T) {
	tStore := &testStore{allocate: func(allocation *protoc.ProcessDefinitionAllocation) (state.ProcessDefinitionAllocation, bool, error) {
		assert.Equal(t, "order", allocation.GetProcessId())
		return state.ProcessDefinitionAllocation{Key: 42, Version: 3, Checksum: allocation.GetChecksum()}, true, nil
	}}
	srv := &Server{store: tStore}

	resp, err := srv.AllocateProcessDefinition(context.Background(), &proto.AllocateProcessDefinitionRequest{
		Allocation: &protoc.ProcessDefinitionAllocation{ProcessId: new("order"), Checksum: new("abc")},
	})
	require.NoError(t, err)
	require.Nil(t, resp.GetError())
	assert.Equal(t, int64(42), resp.GetKey())
	assert.Equal(t, int32(3), resp.GetVersion())
	assert.True(t, resp.GetAlreadyExisted())
}

func TestAllocateProcessDefinitionMapsFailures(t *testing.T) {
	cases := []struct {
		name string
		err  error
		code zenerr.ZenErrorCode
	}{
		{
			name: "a follower answers UNAVAILABLE so the caller retries against the leader",
			err:  errors.Join(zenerr.ErrNotLeader, errors.New("raft: not leader")),
			code: zenerr.UnavailableCode,
		},
		{
			name: "a leader that lost leadership mid-apply answers UNAVAILABLE so the caller retries the idempotent command",
			err:  errors.Join(zenerr.ErrApplyUncertain, errors.New("raft: leadership lost")),
			code: zenerr.UnavailableCode,
		},
		{
			name: "a rejected allocation is the client's fault",
			err:  &state.ProcessDefinitionAllocationRejectedError{ProcessID: "order", Reason: "version tag taken"},
			code: zenerr.BadRequestCode,
		},
		{
			name: "anything else is a cluster error",
			err:  errors.New("raft apply timed out"),
			code: zenerr.ClusterErrorCode,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := &Server{store: &testStore{allocate: func(*protoc.ProcessDefinitionAllocation) (state.ProcessDefinitionAllocation, bool, error) {
				return state.ProcessDefinitionAllocation{}, false, tc.err
			}}}
			resp, err := srv.AllocateProcessDefinition(context.Background(), &proto.AllocateProcessDefinitionRequest{
				Allocation: &protoc.ProcessDefinitionAllocation{ProcessId: new("order"), Checksum: new("abc")},
			})
			require.NoError(t, err, "failures travel in the response so the caller can classify them")
			require.NotNil(t, resp.GetError())
			assert.Equal(t, uint32(tc.code), resp.GetError().GetCode())
		})
	}
}
