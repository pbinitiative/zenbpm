package controller

import (
	"bytes"
	"encoding/json"
	"slices"
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

const registrationTestGrace = time.Minute

// TestControllerTreatsPendingRegistrationAsStartup verifies that the cluster
// state changes a starting node sees before the leader recorded it are no
// error: within the grace period they are logged at debug level, and the
// registration that lands in time is not reported at all.
func TestControllerTreatsPendingRegistrationAsStartup(t *testing.T) {
	rt := newRegistrationTest(t)

	rt.controller.ClusterStateChangeNotification(t.Context())
	rt.clock.advance(registrationTestGrace / 2)
	rt.controller.ClusterStateChangeNotification(t.Context())
	assert.Equal(t, []string{"debug", "debug"}, rt.logs.registrationLevels(t), "log: %s", rt.logs)

	rt.store.setNode(registeredTestNode(rt.store.id))
	rt.controller.ClusterStateChangeNotification(t.Context())
	assert.Equal(t, []string{"debug", "debug"}, rt.logs.registrationLevels(t), "a timely registration is not reported; log: %s", rt.logs)
	rt.logs.assertNoErrors(t)
}

// TestControllerReportsAPersistentAbsenceOnItsOwn verifies that an absence
// outlasting the grace period is reported as a warning when the grace period
// ends, even if no further cluster state change arrives, that it is reported
// once, and that the registration that finally lands says so.
func TestControllerReportsAPersistentAbsenceOnItsOwn(t *testing.T) {
	rt := newRegistrationTest(t)

	rt.controller.ClusterStateChangeNotification(t.Context())
	rt.endGracePeriod(t)
	require.Eventually(t, func() bool {
		return slices.Equal(rt.logs.registrationLevels(t), []string{"debug", "warn"})
	}, 5*time.Second, 5*time.Millisecond, "the scheduled check reports the absence; log: %s", rt.logs)

	rt.controller.ClusterStateChangeNotification(t.Context())
	assert.Equal(t, []string{"debug", "warn", "debug"}, rt.logs.registrationLevels(t), "the absence is reported once; log: %s", rt.logs)

	rt.store.setNode(registeredTestNode(rt.store.id))
	rt.controller.ClusterStateChangeNotification(t.Context())
	assert.Equal(t, []string{"debug", "warn", "debug", "info"}, rt.logs.registrationLevels(t), "the late registration is reported; log: %s", rt.logs)
	rt.logs.assertNoErrors(t)
}

// TestControllerReportsAPersistentAbsenceOnAStateChange verifies that a
// cluster state change observed after the grace period reports the absence
// without waiting for the scheduled check.
func TestControllerReportsAPersistentAbsenceOnAStateChange(t *testing.T) {
	rt := newRegistrationTest(t)

	rt.controller.ClusterStateChangeNotification(t.Context())
	rt.clock.advance(registrationTestGrace)
	rt.controller.ClusterStateChangeNotification(t.Context())
	assert.Equal(t, []string{"debug", "warn"}, rt.logs.registrationLevels(t), "log: %s", rt.logs)
	rt.logs.assertNoErrors(t)
}

// TestControllerDoesNotReportATimelyRegistration verifies that the check
// scheduled at the end of the grace period reports nothing for a node that
// was listed in the meantime.
func TestControllerDoesNotReportATimelyRegistration(t *testing.T) {
	rt := newRegistrationTest(t)

	rt.controller.ClusterStateChangeNotification(t.Context())
	rt.store.setNode(registeredTestNode(rt.store.id))
	rt.controller.ClusterStateChangeNotification(t.Context())
	rt.endGracePeriod(t)
	// Stop waits for the scheduled check, which the grace end released
	require.NoError(t, rt.controller.Stop())

	assert.Equal(t, []string{"debug"}, rt.logs.registrationLevels(t), "log: %s", rt.logs)
	rt.logs.assertNoErrors(t)
}

// TestControllerReportsALostRegistration verifies that a node that was listed
// and is missing from the cluster state later reports that right away as an
// error, once per absence, and says so when it is listed again.
func TestControllerReportsALostRegistration(t *testing.T) {
	rt := newRegistrationTest(t)
	rt.store.setNode(registeredTestNode(rt.store.id))
	rt.controller.ClusterStateChangeNotification(t.Context())
	require.Empty(t, rt.logs.registrationLevels(t), "log: %s", rt.logs)

	rt.store.setClusterState(state.Cluster{Partitions: map[uint32]state.Partition{}, Nodes: map[string]state.Node{}})
	rt.controller.ClusterStateChangeNotification(t.Context())
	rt.controller.ClusterStateChangeNotification(t.Context())
	assert.Equal(t, []string{"error", "debug"}, rt.logs.registrationLevels(t), "log: %s", rt.logs)

	rt.store.setNode(registeredTestNode(rt.store.id))
	rt.controller.ClusterStateChangeNotification(t.Context())
	assert.Equal(t, []string{"error", "debug", "info"}, rt.logs.registrationLevels(t), "log: %s", rt.logs)
}

// TestControllerDoesNotMistakeAStaleSnapshotForALostRegistration verifies
// that concurrent checks observe the cluster state in the order they read
// it: a check that read a snapshot without the node before the node was
// listed cannot be applied after a check that saw the node, which would
// report a registration loss that never happened.
func TestControllerDoesNotMistakeAStaleSnapshotForALostRegistration(t *testing.T) {
	rt := newRegistrationTest(t)
	staleRead := rt.store.blockNextRead()

	staleDone := make(chan struct{})
	go func() {
		defer close(staleDone)
		rt.controller.checkRegistration()
	}()
	<-staleRead.started // the stale check holds a snapshot without the node
	if rt.controller.registrationMu.TryLock() {
		rt.controller.registrationMu.Unlock()
		t.Error("the check reads the cluster state without holding registrationMu, so a newer check can observe first")
	}

	rt.store.setNode(registeredTestNode(rt.store.id))
	freshDone := make(chan struct{})
	go func() {
		defer close(freshDone)
		rt.controller.checkRegistration()
	}()

	close(staleRead.release)
	<-staleDone
	<-freshDone
	assert.Equal(t, []string{"debug"}, rt.logs.registrationLevels(t), "log: %s", rt.logs)
	rt.logs.assertNoErrors(t)
}

func TestRegistrationGracePeriodCoversTheConfiguredJoin(t *testing.T) {
	assert.Equal(t, time.Minute, registrationGracePeriod(config.ClusterRaft{
		JoinAttempts: 5, JoinInterval: 2 * time.Second, JoinAddresses: []string{"localhost:8090"}, BootstrapExpectTimeout: 10 * time.Second,
	}), "the defaults")
	assert.Equal(t, time.Minute, registrationGracePeriod(config.ClusterRaft{}))
	// 20 attempts calling 10 addresses for up to a second each, 19 intervals
	// of 5s, the bootstrap timeout, and half a minute for the leader
	assert.Equal(t, (200+95+30+30)*time.Second, registrationGracePeriod(config.ClusterRaft{
		JoinAttempts: 20, JoinInterval: 5 * time.Second, JoinAddresses: make([]string, 10), BootstrapExpectTimeout: 30 * time.Second,
	}))
}

type registrationTest struct {
	controller *Controller
	store      *registrationTestStore
	logs       *registrationTestLog
	clock      *registrationTestClock
	graceEnd   chan time.Time
}

func newRegistrationTest(t *testing.T) *registrationTest {
	t.Helper()
	rt := &registrationTest{
		store: &registrationTestStore{ControllerTestStore: &ControllerTestStore{
			id: "test-node-1",
			clusterState: state.Cluster{
				Partitions: map[uint32]state.Partition{},
				Nodes:      map[string]state.Node{},
			},
		}},
		logs:     &registrationTestLog{},
		clock:    &registrationTestClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		graceEnd: make(chan time.Time),
	}
	controller, err := NewController(nil, config.Cluster{})
	require.NoError(t, err)
	controller.store = rt.store
	controller.logger = hclog.New(&hclog.LoggerOptions{Output: rt.logs, JSONFormat: true, Level: hclog.Trace})
	controller.registrationGracePeriod = registrationTestGrace
	controller.registrationNow = rt.clock.Now
	controller.registrationAfter = func(d time.Duration) <-chan time.Time {
		assert.Equal(t, registrationTestGrace, d)
		return rt.graceEnd
	}
	controller.handleClusterChanges = true
	rt.controller = controller
	t.Cleanup(func() {
		assert.NoError(t, controller.Stop())
	})
	return rt
}

// endGracePeriod moves the clock past the grace period and releases the
// scheduled registration check; it returns once the check took the signal.
func (rt *registrationTest) endGracePeriod(t *testing.T) {
	t.Helper()
	now := rt.clock.advance(registrationTestGrace)
	select {
	case rt.graceEnd <- now:
	case <-time.After(5 * time.Second):
		t.Fatal("no registration check is scheduled")
	}
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

type registrationTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *registrationTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *registrationTestClock) advance(d time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	return c.now
}

// registrationTestStore can hold one cluster state read: the snapshot is
// taken when the read starts and returned once the test releases it, as a
// notification that was descheduled after reading would see it.
type registrationTestStore struct {
	*ControllerTestStore
	mu      sync.Mutex
	blocked *blockedRead
}

type blockedRead struct {
	started chan struct{}
	release chan struct{}
}

func (s *registrationTestStore) blockNextRead() *blockedRead {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blocked = &blockedRead{started: make(chan struct{}), release: make(chan struct{})}
	return s.blocked
}

func (s *registrationTestStore) ClusterState() state.Cluster {
	snapshot := s.ControllerTestStore.ClusterState()
	s.mu.Lock()
	blocked := s.blocked
	s.blocked = nil
	s.mu.Unlock()
	if blocked != nil {
		close(blocked.started)
		<-blocked.release
	}
	return snapshot
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

func (l *registrationTestLog) entries(t *testing.T) []map[string]any {
	t.Helper()
	l.mu.Lock()
	data := bytes.Clone(l.buf.Bytes())
	l.mu.Unlock()
	entries := []map[string]any{}
	for line := range bytes.Lines(data) {
		var entry map[string]any
		require.NoError(t, json.Unmarshal(line, &entry), "log line: %s", line)
		entries = append(entries, entry)
	}
	return entries
}

// registrationLevels returns the levels of the entries about this node's
// registration, in order.
func (l *registrationTestLog) registrationLevels(t *testing.T) []string {
	t.Helper()
	levels := []string{}
	for _, entry := range l.entries(t) {
		if msg, _ := entry["@message"].(string); strings.Contains(msg, "registered in the cluster state") {
			levels = append(levels, entry["@level"].(string))
		}
	}
	return levels
}

// assertNoErrors fails if any entry, whatever its message, is an error.
func (l *registrationTestLog) assertNoErrors(t *testing.T) {
	t.Helper()
	for _, entry := range l.entries(t) {
		assert.NotEqual(t, "error", entry["@level"], "unexpected error entry: %v", entry)
	}
}
