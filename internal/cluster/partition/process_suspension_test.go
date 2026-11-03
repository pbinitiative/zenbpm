package partition

import (
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConcurrentChangesOfASuspensionNeverShareAVersion changes the suspension of one process from several
// goroutines at once: the versions the changes take are the numbers from 1 up, each of them once, so that a
// partition which is told of the changes in any order keeps the last one.
func TestConcurrentChangesOfASuspensionNeverShareAVersion(t *testing.T) {
	partition, conf, clientMgr, tStore, _ := prepareTestSetup(t, false)
	defer func() { require.NoError(t, partition.Stop()) }()
	db := newTestDB(t, partition, conf, clientMgr, tStore, "test-process-suspension-db")

	const changes = 16
	versions := make([]int64, changes)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range changes {
		wg.Go(func() {
			<-start // released together, to provoke contention
			version, err := db.ChangeProcessSuspension(t.Context(), "concurrent-process", i%2 == 0)
			assert.NoError(t, err)
			versions[i] = version
		})
	}
	close(start)
	wg.Wait()

	slices.Sort(versions)
	expected := make([]int64, changes)
	for i := range expected {
		expected[i] = int64(i + 1)
	}
	assert.Equal(t, expected, versions)
}
