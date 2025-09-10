// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package store

import (
	"github.com/stretchr/testify/assert"
	"testing"
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
	state := ClusterState{
		Config:     ClusterConfig{},
		Partitions: partitions,
		Nodes:      nil,
	}
	assert.Equal(t, uint32(1), state.GetPartitionIdFromString("0"))
	assert.Equal(t, uint32(2), state.GetPartitionIdFromString("1"))
	assert.Equal(t, uint32(3), state.GetPartitionIdFromString("2"))
}
