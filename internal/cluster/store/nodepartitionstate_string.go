// Code generated by "stringer -type=NodePartitionState"; DO NOT EDIT.

package store

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[NodePartitionStateError-1]
	_ = x[NodePartitionStateJoining-2]
	_ = x[NodePartitionStateLeaving-3]
	_ = x[NodePartitionStateInitializing-4]
	_ = x[NodePartitionStateInitialized-5]
}

const _NodePartitionState_name = "NodePartitionStateErrorNodePartitionStateJoiningNodePartitionStateLeavingNodePartitionStateInitializingNodePartitionStateInitialized"

var _NodePartitionState_index = [...]uint8{0, 23, 48, 73, 103, 132}

func (i NodePartitionState) String() string {
	i -= 1
	if i < 0 || i >= NodePartitionState(len(_NodePartitionState_index)-1) {
		return "NodePartitionState(" + strconv.FormatInt(int64(i+1), 10) + ")"
	}
	return _NodePartitionState_name[_NodePartitionState_index[i]:_NodePartitionState_index[i+1]]
}
