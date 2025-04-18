// Code generated by "stringer -type=ActivityState"; DO NOT EDIT.

package runtime

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[ActivityStateActive-1]
	_ = x[ActivityStateCompensated-2]
	_ = x[ActivityStateCompensating-3]
	_ = x[ActivityStateCompleted-4]
	_ = x[ActivityStateCompleting-5]
	_ = x[ActivityStateFailed-6]
	_ = x[ActivityStateFailing-7]
	_ = x[ActivityStateReady-8]
	_ = x[ActivityStateTerminated-9]
	_ = x[ActivityStateTerminating-10]
	_ = x[ActivityStateWithdrawn-11]
}

const _ActivityState_name = "ActivityStateActiveActivityStateCompensatedActivityStateCompensatingActivityStateCompletedActivityStateCompletingActivityStateFailedActivityStateFailingActivityStateReadyActivityStateTerminatedActivityStateTerminatingActivityStateWithdrawn"

var _ActivityState_index = [...]uint8{0, 19, 43, 68, 90, 113, 132, 152, 170, 193, 217, 239}

func (i ActivityState) String() string {
	i -= 1
	if i < 0 || i >= ActivityState(len(_ActivityState_index)-1) {
		return "ActivityState(" + strconv.FormatInt(int64(i+1), 10) + ")"
	}
	return _ActivityState_name[_ActivityState_index[i]:_ActivityState_index[i+1]]
}
