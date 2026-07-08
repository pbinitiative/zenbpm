package partition

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStopTerminatesObserver(t *testing.T) {
	partition, _, _, _, _ := prepareTestSetup(t, false)
	// Stop clears the channel fields once the goroutine is down, so capture the
	// completion signal first.
	observerDone := partition.observerDone

	err := partition.Stop()
	assert.NoError(t, err)

	select {
	case <-observerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("observer goroutine (metrics ticker) still running 5s after Stop")
	}
}
