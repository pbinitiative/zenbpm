package server

import (
	"context"
	stdsql "database/sql"
	"errors"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/jobmanager"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// jobInPartition builds a key of the partition, in the layout of the keys the
// engine generates.
func jobInPartition(partition int64, sequence int64) int64 {
	return partition<<zenflake.StepBits | sequence
}

func TestExtendJobLockOnANonLeaderIsAClusterError(t *testing.T) {
	// a job manager which never started its leader-side server answers
	// NodeIsNotALeader, as a node does after losing the partition
	store := extendLockTestStore{}
	server := &Server{store: store, jobManager: jobmanager.New(t.Context(), store, nil, nil, nil)}

	resp, err := server.ExtendJobLock(t.Context(), &proto.ExtendJobLockRequest{
		Key:      new(jobInPartition(1, 7)),
		ClientId: new("client-1"),
	})

	require.NoError(t, err)
	require.NotNil(t, resp.Error)
	assert.Equal(t, uint32(zenerr.ClusterErrorCode), resp.Error.GetCode(), "a stale leader is a routing failure to retry, not an internal error")
	assert.Equal(t, proto.LockRefusal_LOCK_REFUSAL_NONE, resp.GetRefusal())
}

// TestExtendJobLockOnANodeWhichDoesNotLeadThePartitionIsAClusterError shows
// a node whose job server runs, as every node's does after the production
// start, answers nothing about a job of a partition it does not lead: neither
// its lock table, which holds no lock of that partition, nor its possibly
// stale copy of the jobs may decide.
func TestExtendJobLockOnANodeWhichDoesNotLeadThePartitionIsAClusterError(t *testing.T) {
	server := leadingServer(t, &lookupController{row: sql.ConstructRow(t.Context(), nil, nil, nil, nil)})

	resp, err := server.ExtendJobLock(t.Context(), &proto.ExtendJobLockRequest{
		Key:      new(jobInPartition(2, 7)),
		ClientId: new("client-1"),
	})

	require.NoError(t, err)
	require.NotNil(t, resp.Error)
	assert.Equal(t, uint32(zenerr.ClusterErrorCode), resp.Error.GetCode(), "a missing row on a node which does not lead the partition is no evidence the job does not exist")
	assert.Equal(t, proto.LockRefusal_LOCK_REFUSAL_NONE, resp.GetRefusal())
}

// extendLockTestStore is a store of a node which leads nothing; the embedded
// interface fills the methods the tests never call.
type extendLockTestStore struct{ StoreService }

func (extendLockTestStore) ClusterState() state.Cluster { return state.Cluster{} }
func (extendLockTestStore) NodeID() string              { return "node-1" }

// TestExtendJobLockOfAJobTheLeaderDoesNotKnowIsNotFound shows the leader tells
// a key nobody ever created (404) apart from a job which merely holds no lock
// (409) from its own database, the only copy which is never behind.
func TestExtendJobLockOfAJobTheLeaderDoesNotKnowIsNotFound(t *testing.T) {
	server := leadingServer(t, &lookupController{row: sql.ConstructRow(t.Context(), nil, nil, nil, nil)})

	resp, err := server.ExtendJobLock(t.Context(), &proto.ExtendJobLockRequest{Key: new(jobInPartition(1, 7)), ClientId: new("client-1")})

	require.NoError(t, err)
	require.NotNil(t, resp.Error)
	assert.Equal(t, uint32(zenerr.NotFoundCode), resp.Error.GetCode())
	assert.Equal(t, proto.LockRefusal_LOCK_REFUSAL_NOT_HELD, resp.GetRefusal(), "a stream client still learns the lock is not held")
}

func TestExtendJobLockStaysAConflictWhenTheLookupCannotDecide(t *testing.T) {
	tests := []struct {
		name       string
		controller ControllerService
	}{
		{"the lookup fails", &lookupController{row: sql.ConstructRow(t.Context(), nil, nil, nil, errors.New("database is closed"))}},
		{"the partition has no queries", &fakeController{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := leadingServer(t, tt.controller)

			resp, err := server.ExtendJobLock(t.Context(), &proto.ExtendJobLockRequest{Key: new(jobInPartition(1, 7)), ClientId: new("client-1")})

			require.NoError(t, err)
			require.NotNil(t, resp.Error)
			assert.Equal(t, uint32(zenerr.ConflictCode), resp.Error.GetCode(), "only a positive answer that the job does not exist turns the refusal into not found")
			assert.Equal(t, proto.LockRefusal_LOCK_REFUSAL_NOT_HELD, resp.GetRefusal())
		})
	}
}

// leadingServer is a cluster server whose job manager leads partition 1 and
// so answers lock extensions itself.
func leadingServer(t *testing.T, controller ControllerService) *Server {
	t.Helper()
	manager := jobmanager.New(t.Context(), leadingStore{}, nil, nil, nil)
	manager.OnPartitionRoleChange(t.Context())
	return &Server{store: leadingStore{}, jobManager: manager, controller: controller}
}

type leadingStore struct{ StoreService }

func (leadingStore) NodeID() string { return "node-1" }

// ClusterState has node-1 lead partition 1 and follow partition 2, led by node-2.
func (leadingStore) ClusterState() state.Cluster {
	return state.Cluster{
		Partitions: map[uint32]state.Partition{
			1: {Id: 1, LeaderId: "node-1"},
			2: {Id: 2, LeaderId: "node-2"},
		},
		Nodes: map[string]state.Node{
			"node-1": {
				Id: "node-1",
				Partitions: map[uint32]state.NodePartition{
					1: {Id: 1, State: state.NodePartitionStateInitialized, Role: state.RoleLeader},
					2: {Id: 2, State: state.NodePartitionStateInitialized, Role: state.RoleFollower},
				},
			},
			"node-2": {
				Id: "node-2",
				Partitions: map[uint32]state.NodePartition{
					2: {Id: 2, State: state.NodePartitionStateInitialized, Role: state.RoleLeader},
				},
			},
		},
	}
}

// lookupController hands out queries whose every single-row lookup answers
// with the prepared row.
type lookupController struct {
	fakeController
	row *sql.Row
}

func (c *lookupController) PartitionQueries(context.Context, uint32) *sql.Queries {
	return sql.New(singleRowDBTX{row: c.row})
}

type singleRowDBTX struct{ row *sql.Row }

func (d singleRowDBTX) QueryRowContext(context.Context, string, ...interface{}) *sql.Row {
	return d.row
}
func (singleRowDBTX) ExecContext(context.Context, string, ...interface{}) (stdsql.Result, error) {
	return nil, errors.New("not expected")
}
func (singleRowDBTX) PrepareContext(context.Context, string) (*stdsql.Stmt, error) {
	return nil, errors.New("not expected")
}
func (singleRowDBTX) QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error) {
	return nil, errors.New("not expected")
}
