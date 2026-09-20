package zenclient

import (
	"math"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient/proto"
	"github.com/stretchr/testify/assert"
)

func TestActiveJobsForWireNeverWraps(t *testing.T) {
	assert.Equal(t, int32(10), activeJobsForWire(10))
	assert.Equal(t, int32(0), activeJobsForWire(0), "zero asks for the engine default")
	assert.Equal(t, int32(0), activeJobsForWire(-5), "a negative count asks for the default like zero does")
	assert.Equal(t, int32(math.MaxInt32), activeJobsForWire(math.MaxInt32))
	assert.Equal(t, int32(math.MaxInt32), activeJobsForWire(math.MaxInt32+1), "one above the wire's range saturates instead of wrapping to a negative count")
}

func TestJobStreamErrorCodeOutsideTheEnumReadsAsUnspecified(t *testing.T) {
	notHeld := uint32(proto.JobStreamErrorCode_JOB_STREAM_ERROR_CODE_LOCK_NOT_HELD)
	assert.Equal(t, proto.JobStreamErrorCode_JOB_STREAM_ERROR_CODE_LOCK_NOT_HELD, jobStreamErrorCode(&proto.ErrorResult{Code: &notHeld}))
	assert.Equal(t, proto.JobStreamErrorCode_JOB_STREAM_ERROR_CODE_UNSPECIFIED, jobStreamErrorCode(&proto.ErrorResult{}), "no code is unspecified")
	tooLarge := uint32(math.MaxUint32)
	assert.Equal(t, proto.JobStreamErrorCode_JOB_STREAM_ERROR_CODE_UNSPECIFIED, jobStreamErrorCode(&proto.ErrorResult{Code: &tooLarge}), "a code the enum cannot hold is unspecified, not a wrapped negative value")
}
