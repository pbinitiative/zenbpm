//go:build cluster_e2e

package cluster

import (
	"slices"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/stretchr/testify/require"
)

// TestStatusListsAFeatureOnlyWhileEveryMemberCarriesIt verifies the features
// of GET /system/status across a cluster: every node lists them once every
// member announced them, no node lists them while a member is shut down, and
// every node lists them again once that member is back and announced again.
func TestStatusListsAFeatureOnlyWhileEveryMemberCarriesIt(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)
	WaitForHealthy(t, tc, 150*time.Second)
	waitForListenerFeatureOnEveryRunningNode(t, tc, true, 60*time.Second)

	followers := tc.Followers()
	require.NotEmpty(t, followers)
	killedID := followers[0].ID
	tc.KillNode(t, killedID)
	WaitForNodeObservedDown(t, tc, killedID, 60*time.Second)
	waitForListenerFeatureOnEveryRunningNode(t, tc, false, 30*time.Second)

	tc.RestartNode(t, killedID)
	WaitForHealthy(t, tc, 150*time.Second)
	waitForListenerFeatureOnEveryRunningNode(t, tc, true, 60*time.Second)
}

func waitForListenerFeatureOnEveryRunningNode(t *testing.T, tc *TestCluster, listed bool, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, n := range tc.RunningNodes() {
			s, err := getStatus(n)
			if err != nil || slices.Contains(s.Features, state.FeatureExecutionListenerStartEventEnd) != listed {
				return false
			}
		}
		return true
	}, timeout, 200*time.Millisecond, "every running node lists the feature %s: %t", state.FeatureExecutionListenerStartEventEnd, listed)
}
