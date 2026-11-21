package controller

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestControllerTreatsPendingRegistrationAsStartup verifies that the cluster
// state changes a starting node sees before the leader recorded it are no
// error: they are logged at debug level, and only an absence that outlasts
// the grace period is reported, as a warning, even if no further cluster
// state change arrives. The registration that finally lands says so.
func TestControllerTreatsPendingRegistrationAsStartup(t *testing.T) {
	controller, tStore, logs := newRegistrationTestController(t, 50*time.Millisecond)

	controller.ClusterStateChangeNotification(t.Context())
	controller.ClusterStateChangeNotification(t.Context())
	assert.Equal(t, []string{"debug", "debug"}, logs.registrationLevels(t), "log: %s", logs)

	// no state change arrives: the scheduled check reports the absence
	require.Eventually(t, func() bool {
		return logs.countLevel(t, "warn") == 1
	}, 5*time.Second, 10*time.Millisecond, "a persistent absence is reported; log: %s", logs)
	controller.ClusterStateChangeNotification(t.Context())
	assert.Equal(t, 1, logs.countLevel(t, "warn"), "the absence is reported once; log: %s", logs)

	tStore.setNode(registeredTestNode(tStore.id))
	controller.ClusterStateChangeNotification(t.Context())
	levels := logs.registrationLevels(t)
	assert.Equal(t, "info", levels[len(levels)-1], "the late registration is reported; log: %s", logs)
	assert.Zero(t, logs.countLevel(t, "error"), "log: %s", logs)
}

// TestControllerDoesNotReportATimelyRegistration verifies that a node listed
// within the grace period logs nothing above debug level, also after the
// grace period ended.
func TestControllerDoesNotReportATimelyRegistration(t *testing.T) {
	controller, tStore, logs := newRegistrationTestController(t, 50*time.Millisecond)

	controller.ClusterStateChangeNotification(t.Context())
	tStore.setNode(registeredTestNode(tStore.id))
	controller.ClusterStateChangeNotification(t.Context())

	time.Sleep(150 * time.Millisecond)
	assert.Equal(t, []string{"debug"}, logs.registrationLevels(t), "log: %s", logs)
}

// TestControllerReportsALostRegistration verifies that a node that was listed
// and is missing from the cluster state later reports that right away as an
// error, once per absence, and says so when it is listed again.
func TestControllerReportsALostRegistration(t *testing.T) {
	controller, tStore, logs := newRegistrationTestController(t, time.Hour)
	tStore.setNode(registeredTestNode(tStore.id))
	controller.ClusterStateChangeNotification(t.Context())
	require.Empty(t, logs.registrationLevels(t), "log: %s", logs)

	tStore.setClusterState(state.Cluster{Partitions: map[uint32]state.Partition{}, Nodes: map[string]state.Node{}})
	controller.ClusterStateChangeNotification(t.Context())
	controller.ClusterStateChangeNotification(t.Context())
	assert.Equal(t, []string{"error", "debug"}, logs.registrationLevels(t), "log: %s", logs)

	tStore.setNode(registeredTestNode(tStore.id))
	controller.ClusterStateChangeNotification(t.Context())
	assert.Equal(t, []string{"error", "debug", "info"}, logs.registrationLevels(t), "log: %s", logs)
}

func newRegistrationTestController(t *testing.T, gracePeriod time.Duration) (*Controller, *ControllerTestStore, *registrationTestLog) {
	t.Helper()
	tStore := &ControllerTestStore{
		id: "test-node-1",
		clusterState: state.Cluster{
			Partitions: map[uint32]state.Partition{},
			Nodes:      map[string]state.Node{},
		},
	}
	controller, err := NewController(nil, config.Cluster{})
	require.NoError(t, err)
	logs := &registrationTestLog{}
	controller.store = tStore
	controller.logger = hclog.New(&hclog.LoggerOptions{Output: logs, JSONFormat: true, Level: hclog.Trace})
	controller.registrationGracePeriod = gracePeriod
	controller.handleClusterChanges = true
	t.Cleanup(func() {
		assert.NoError(t, controller.Stop())
	})
	return controller, tStore, logs
}

// registeredTestNode is a listed node that already announced its protocol
// version and hosts no partition, so member operations have nothing to do.
func registeredTestNode(id string) state.Node {
	return state.Node{
		Id:              id,
		State:           state.NodeStateStarted,
		ProtocolVersion: state.CurrentProtocolVersion,
		Partitions:      map[uint32]state.NodePartition{},
	}
}

// registrationTestLog collects the JSON log of a controller whose background
// registration check writes concurrently with the test.
type registrationTestLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *registrationTestLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *registrationTestLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// registrationLevels returns the levels of the entries about this node's
// registration, in order.
func (l *registrationTestLog) registrationLevels(t *testing.T) []string {
	t.Helper()
	l.mu.Lock()
	data := bytes.Clone(l.buf.Bytes())
	l.mu.Unlock()
	levels := []string{}
	for line := range bytes.Lines(data) {
		var entry map[string]any
		require.NoError(t, json.Unmarshal(line, &entry), "log line: %s", line)
		if msg, _ := entry["@message"].(string); strings.Contains(msg, "registered in the cluster state") {
			levels = append(levels, entry["@level"].(string))
		}
	}
	return levels
}

func (l *registrationTestLog) countLevel(t *testing.T, level string) int {
	t.Helper()
	count := 0
	for _, l := range l.registrationLevels(t) {
		if l == level {
			count++
		}
	}
	return count
}
