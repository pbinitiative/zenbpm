package zenflake

import "github.com/bwmarrin/snowflake"

// NODE with id 0 is used for global resources like definitions across all the partitions

var (
	// NodeBits holds the number of bits to use for Node
	// Remember, you have a total 22 bits to share between Node/Step
	NodeBits uint8 = 10

	// StepBits holds the number of bits to use for Step
	// Remember, you have a total 22 bits to share between Node/Step
	StepBits uint8 = 12

	// internal values of bwmarrin/snowflake
	nodeMax   int64 = -1 ^ (-1 << NodeBits)
	nodeMask        = nodeMax << StepBits
	stepMask  int64 = -1 ^ (-1 << StepBits)
	timeShift       = NodeBits + StepBits
	nodeShift       = StepBits
)

func GetPartitionMask() int64 {
	return nodeMask
}

func GetPartitionId(id int64) uint32 {
	maskedId := id & GetPartitionMask()
	nodeId := maskedId >> int64(nodeShift)
	return uint32(nodeId)
}

// GlobalResourceNode is the node id carried by keys of global resources:
// definitions that every partition holds are not routed by their key.
const GlobalResourceNode int64 = 0

// GlobalKey builds the key of a global resource from a millisecond timestamp
// and a sequence number, in the layout snowflake.Node.Generate uses, so that
// GetPartitionId reports GlobalResourceNode for it. Only the low StepBits of
// the sequence are kept; callers keep keys unique by never reusing a
// millisecond with the same low sequence bits.
func GlobalKey(millis int64, sequence int64) int64 {
	return (millis-snowflake.Epoch)<<timeShift | GlobalResourceNode<<nodeShift | (sequence & stepMask)
}
