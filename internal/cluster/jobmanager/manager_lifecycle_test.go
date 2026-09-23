package jobmanager

import (
	"sync"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/stretchr/testify/assert"
)

// TestStartAndRoleChangeShareTheServerLifecycle runs the two ways the
// leader-side server is started and stopped against each other; the race
// detector reports any access to the server context outside the shared lock.
func TestStartAndRoleChangeShareTheServerLifecycle(t *testing.T) {
	store := &testStore{state: state.Cluster{}, nodeId: "node-1"}
	manager := New(t.Context(), store, nil, nil, nil)

	var wg sync.WaitGroup
	for range 20 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			manager.Start()
		}()
		go func() {
			defer wg.Done()
			manager.OnPartitionRoleChange(t.Context())
		}()
	}
	wg.Wait()

	manager.roleChangeMu.Lock()
	defer manager.roleChangeMu.Unlock()
	assert.Equal(t, manager.serverCtx != nil, manager.server.Load() != nil, "the published server and its context belong together")
}
