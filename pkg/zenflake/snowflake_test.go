package snowflake

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

	fmt.Printf("%d :\n- %b\n", GetNodeMask(), GetNodeMask())
	maskedId := id.Int64() & GetNodeMask()
	fmt.Printf("%b\n", maskedId)
	fmt.Printf("%b\n", nodeShift)
	assert.Equal(t, nodeId, maskedId>>int64(nodeShift))
}
