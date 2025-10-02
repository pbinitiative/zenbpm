package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_GetPartitionIdFromString(t *testing.T) {
	partitions := make(map[uint32]Partition)
	partitions[1] = Partition{
		Id:       1,
		LeaderId: "node-1",
	}
	partitions[2] = Partition{
		Id:       2,
		LeaderId: "node-2",
	}
	partitions[3] = Partition{
		Id:       3,
		LeaderId: "node-3",
	}
	state := Cluster{
		Config:     ClusterConfig{},
		Partitions: partitions,
		Nodes:      nil,
	}
	assert.Equal(t, uint32(1), state.GetPartitionIdFromString("0"))
	assert.Equal(t, uint32(2), state.GetPartitionIdFromString("1"))
	assert.Equal(t, uint32(3), state.GetPartitionIdFromString("2"))
}
