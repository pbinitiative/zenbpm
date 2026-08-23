package store

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const storeMeter = "zen-store"

// clusterGauges groups the observable instruments reported by the main (Zen)
// cluster store so registration and observation stay readable.
type clusterGauges struct {
	hasLeader          metric.Int64ObservableGauge
	nodeIsLeader       metric.Int64ObservableGauge
	partitionHasLeader metric.Int64ObservableGauge
	leaderInfo         metric.Int64ObservableGauge
	partitions         metric.Int64ObservableGauge
	desiredPartitions  metric.Int64ObservableGauge
	nodeUp             metric.Int64ObservableGauge
	nodeRole           metric.Int64ObservableGauge
	nodeUptime         metric.Int64ObservableGauge
	raftTerm           metric.Int64ObservableGauge
	raftLastLogIndex   metric.Int64ObservableGauge
	raftAppliedIndex   metric.Int64ObservableGauge
	raftFsmPending     metric.Int64ObservableGauge
	raftLastContact    metric.Int64ObservableGauge
}

// RegisterMetrics registers observable gauges that report main (Zen) cluster
// raft health: leader presence and identity, this node's leadership status,
// per-partition leader presence (from the replicated cluster state), per-member
// health/role, node uptime and raw raft statistics (term, log indexes, FSM
// backlog, last leader contact).
//
// These gauges are the data source for the NoClusterLeader / NoPartitionLeader /
// PartitionDeficit Prometheus alerts, so they are exported by every node:
// alerting does not depend on a single node being scrapeable.
//
// The callback registration is retained on the store and released in Close so
// the global meter provider does not keep closed stores alive.
func (s *Store) RegisterMetrics() error {
	meter := otel.Meter(storeMeter)

	g, instruments, err := newClusterGauges(meter)
	if err != nil {
		return err
	}

	reg, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		return s.observeClusterMetrics(o, g)
	}, instruments...)
	if err != nil {
		return err
	}
	s.metricsRegistration = reg
	return nil
}

// unregisterMetrics releases the observable callback registered by
// RegisterMetrics so a closed store is no longer referenced by the global
// meter provider. Safe to call when metrics were never registered.
func (s *Store) unregisterMetrics() error {
	if s.metricsRegistration == nil {
		return nil
	}
	err := s.metricsRegistration.Unregister()
	s.metricsRegistration = nil
	return err
}

func (s *Store) observeClusterMetrics(o metric.Observer, g clusterGauges) error {
	if !s.open.Load() {
		return nil
	}
	o.ObserveInt64(g.hasLeader, boolToInt64(s.HasLeader()))
	o.ObserveInt64(g.nodeIsLeader, boolToInt64(s.IsLeader()))

	// the leader identity is exported as attributes of an always-1 gauge so
	// dashboards can render *which* node leads instead of a plain 0/1 flag
	if leaderAddr, leaderID := s.LeaderWithID(); leaderID != "" {
		o.ObserveInt64(g.leaderInfo, 1, metric.WithAttributes(
			attribute.String("leader_id", leaderID),
			attribute.String("leader_addr", leaderAddr),
		))
	}

	cs := s.ClusterState()
	o.ObserveInt64(g.partitions, int64(len(cs.Partitions)))
	o.ObserveInt64(g.desiredPartitions, int64(cs.Config.DesiredPartitions))
	for id, partition := range cs.Partitions {
		o.ObserveInt64(g.partitionHasLeader, boolToInt64(partition.LeaderId != ""),
			metric.WithAttributes(attribute.Int64("partition", int64(id))))
	}
	s.observeClusterMembers(o, g, cs)

	stats := s.raft.Stats()
	observeRaftStat(o, g.raftTerm, stats, "term")
	observeRaftStat(o, g.raftLastLogIndex, stats, "last_log_index")
	observeRaftStat(o, g.raftAppliedIndex, stats, "applied_index")
	observeRaftStat(o, g.raftFsmPending, stats, "fsm_pending")
	return nil
}

// observeClusterMembers reports one series per cluster member from the
// replicated cluster state (so every node can describe every member) plus the
// metrics only the local node knows about itself (uptime, leader contact). The
// local metrics are reported even when this node is not (yet) part of the
// replicated state, so a node that fails to register stays observable.
func (s *Store) observeClusterMembers(o metric.Observer, g clusterGauges, cs state.Cluster) {
	var localAddr string
	for _, node := range cs.Nodes {
		nodeAttrs := metric.WithAttributes(
			attribute.String("node_id", node.Id),
			attribute.String("node_addr", node.Addr),
		)
		o.ObserveInt64(g.nodeUp, boolToInt64(node.State == state.NodeStateStarted), nodeAttrs)
		o.ObserveInt64(g.nodeRole, boolToInt64(node.Role == state.RoleLeader), nodeAttrs)
		if node.Id == s.NodeID() {
			localAddr = node.Addr
		}
	}

	localAttrs := metric.WithAttributes(
		attribute.String("node_id", s.NodeID()),
		attribute.String("node_addr", localAddr),
	)
	if !s.startedAt.IsZero() {
		o.ObserveInt64(g.nodeUptime, int64(time.Since(s.startedAt).Seconds()), localAttrs)
	}
	o.ObserveInt64(g.raftLastContact, lastContactMillis(s.raft.Stats()), localAttrs)
}

// newClusterGauges creates every observable instrument of the main cluster
// store and returns them both grouped and as the slice required by
// metric.Meter.RegisterCallback.
func newClusterGauges(meter metric.Meter) (clusterGauges, []metric.Observable, error) {
	var g clusterGauges
	defs := []struct {
		target *metric.Int64ObservableGauge
		name   string
		unit   string
		desc   string
	}{
		{&g.hasLeader, "cluster_has_leader", "", "1 when the main Zen cluster has an elected raft leader, 0 otherwise"},
		{&g.nodeIsLeader, "node_is_leader", "", "1 when this node is the main Zen cluster raft leader, 0 otherwise"},
		{&g.partitionHasLeader, "partition_has_leader", "", "1 when the partition has a leader registered in the cluster state (replicated view; see partition_raft_has_leader for the local raft view), 0 otherwise"},
		{&g.leaderInfo, "cluster_leader_info", "", "Always 1; the leader_id/leader_addr attributes identify the current main Zen cluster raft leader"},
		// partition existence gauges: partition_has_leader only emits series for
		// partitions that already exist in the cluster state, so a bootstrap
		// failure that leaves partitions missing would otherwise be invisible to
		// alerting (absent series never fire NoPartitionLeader)
		{&g.partitions, "cluster_partitions", "", "Number of partitions currently present in the cluster state"},
		{&g.desiredPartitions, "cluster_desired_partitions", "", "Number of partitions the cluster is configured to have"},
		// the cluster_node_* gauges come from the replicated state, so every node
		// emits a series per member; dashboards deduplicate with max by(node_id)
		{&g.nodeUp, "cluster_node_up", "", "1 when the cluster member is in the started state according to the replicated cluster state, 0 otherwise"},
		{&g.nodeRole, "cluster_node_role", "", "1 when the cluster member is the main Zen cluster raft leader, 0 when it is a follower"},
		{&g.nodeUptime, "node_uptime", "s", "Seconds since the local node opened its main Zen cluster store"},
		{&g.raftTerm, "raft_term", "", "Current raft term of the main Zen cluster"},
		{&g.raftLastLogIndex, "raft_last_log_index", "", "Last raft log index of the main Zen cluster"},
		{&g.raftAppliedIndex, "raft_applied_index", "", "Last applied raft log index of the main Zen cluster"},
		{&g.raftFsmPending, "raft_fsm_pending", "", "Number of raft log entries pending application to the FSM"},
		{&g.raftLastContact, "raft_last_contact", "ms", "Milliseconds since the local node last heard from the raft leader; 0 on the leader itself, -1 when there has never been contact"},
	}

	instruments := make([]metric.Observable, 0, len(defs))
	for _, def := range defs {
		opts := []metric.Int64ObservableGaugeOption{metric.WithDescription(def.desc)}
		if def.unit != "" {
			opts = append(opts, metric.WithUnit(def.unit))
		}
		gauge, err := meter.Int64ObservableGauge(def.name, opts...)
		if err != nil {
			return clusterGauges{}, nil, fmt.Errorf("create observable gauge %s: %w", def.name, err)
		}
		*def.target = gauge
		instruments = append(instruments, gauge)
	}
	return g, instruments, nil
}

func observeRaftStat(o metric.Observer, gauge metric.Int64ObservableGauge, stats map[string]string, key string) {
	raw, ok := stats[key]
	if !ok {
		return
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return
	}
	o.ObserveInt64(gauge, value)
}

// lastContactMillis converts the hashicorp raft "last_contact" stat into
// milliseconds. Raft reports "never" when the node has never heard from a
// leader (mapped to -1 so it stays distinguishable from a healthy 0) and "0"
// while the node is itself the leader.
func lastContactMillis(stats map[string]string) int64 {
	raw, ok := stats["last_contact"]
	if !ok || raw == "never" {
		return -1
	}
	if raw == "0" {
		return 0
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return -1
	}
	return d.Milliseconds()
}

func boolToInt64(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
