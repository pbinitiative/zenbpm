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
	nodePart := (id.Int64() & nodeMask) >> int64(nodeShift)
	fmt.Printf("%d :\n- %b\n", nodePart, nodePart)

	fmt.Printf("%d :\n- %b\n", GetPartitionMask(), GetPartitionMask())
	maskedId := id.Int64() & GetPartitionMask()
	fmt.Printf("%b\n", maskedId)
	fmt.Printf("%b\n", nodeShift)
	assert.Equal(t, nodeId, maskedId>>int64(nodeShift))
	assert.Equal(t, uint32(nodeId), GetPartitionId(id.Int64()))
}

func TestGlobalKeyIsAGlobalResourceKey(t *testing.T) {
	millis := int64(1_700_000_000_000)
	key := GlobalKey(millis, 5)
	assert.Equal(t, uint32(GlobalResourceNode), GetPartitionId(key))
	assert.Equal(t, millis, (key>>timeShift)+snowflake.Epoch, "the millisecond is the key's time")
	assert.Equal(t, int64(5), key&stepMask, "the sequence is the key's step")
	assert.Equal(t, GlobalKey(millis, 5), GlobalKey(millis, 5+1<<StepBits), "only the low step bits of the sequence are kept")
	assert.NotEqual(t, GlobalKey(millis, 5), GlobalKey(millis+1, 5))
}
