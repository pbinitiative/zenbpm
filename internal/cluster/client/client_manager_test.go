package client

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPartitionLeaderTellsAnAbsentPartitionFromOneWithoutALeader shows a
// partition without a known leader is reported as absent only when the store
// says the cluster does not have it, and otherwise as a leader missing for the
// moment.
func TestPartitionLeaderTellsAnAbsentPartitionFromOneWithoutALeader(t *testing.T) {
	manager := NewClientManager(leaderlessStore{absent: map[uint32]bool{0: true}})

	_, err := manager.PartitionLeader(0)
	require.Error(t, err)
	assert.ErrorIs(t, err, zenerr.ErrPartitionAbsent)

	_, err = manager.PartitionLeader(1)
	require.Error(t, err)
	assert.NotErrorIs(t, err, zenerr.ErrPartitionAbsent, "a partition the cluster has is only without a leader for the moment")
}

// leaderlessStore is a store which knows no leader of any partition and calls
// the partitions in absent absent.
type leaderlessStore struct {
	absent map[uint32]bool
}

func (leaderlessStore) LeaderWithID() (string, string) { return "", "" }

func (leaderlessStore) PartitionLeaderWithID(uint32) (string, string) { return "", "" }

func (s leaderlessStore) PartitionAbsent(partition uint32) bool { return s.absent[partition] }
