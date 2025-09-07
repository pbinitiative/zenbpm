// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package zenflake

import (
	"fmt"
	"testing"

	"github.com/bwmarrin/snowflake"
	"github.com/stretchr/testify/assert"
)

func TestNodeMask(t *testing.T) {
	nodeId := int64(4)
	node, _ := snowflake.NewNode(nodeId)
	id := node.Generate()
	fmt.Printf("%d :\n- %b\n", id.Int64(), id.Int64())
	fmt.Printf("%d :\n- %b\n", id.Node(), id.Node())

	fmt.Printf("%d :\n- %b\n", GetPartitionMask(), GetPartitionMask())
	maskedId := id.Int64() & GetPartitionMask()
	fmt.Printf("%b\n", maskedId)
	fmt.Printf("%b\n", nodeShift)
	assert.Equal(t, nodeId, maskedId>>int64(nodeShift))
	assert.Equal(t, uint32(nodeId), GetPartitionId(id.Int64()))
}
