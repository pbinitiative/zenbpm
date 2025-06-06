package zenflake

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
