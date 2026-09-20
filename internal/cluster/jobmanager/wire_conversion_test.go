package jobmanager

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestActiveJobsForWireNeverWraps(t *testing.T) {
	assert.Equal(t, int32(10), activeJobsForWire(10))
	assert.Equal(t, int32(0), activeJobsForWire(0), "zero asks for the engine default")
	assert.Equal(t, int32(0), activeJobsForWire(-5), "a negative count asks for the default like zero does")
	assert.Equal(t, int32(math.MaxInt32), activeJobsForWire(math.MaxInt32))
	assert.Equal(t, int32(math.MaxInt32), activeJobsForWire(math.MaxInt32+1), "one above the wire's range saturates instead of wrapping to a negative count")
}
