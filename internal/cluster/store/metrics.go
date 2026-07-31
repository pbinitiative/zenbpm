package store

import (
	"context"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const storeMeter = "zen-store"

// RegisterMetrics registers observable gauges that report main (Zen) cluster
// raft health: leader presence, this node's leadership status, per-partition
// leader presence (from the replicated cluster state), partition counts and
// raw raft statistics (term, log indexes, FSM backlog).
//
// These gauges are the data source for the NoClusterLeader / NoPartitionLeader /
// PartitionDeficit Prometheus alerts, so they are exported by every node:
// alerting does not depend on a single node being scrapeable.
//
// The callback registration is retained on the store and released in Close so
// the global meter provider does not keep closed stores alive.
func (s *Store) RegisterMetrics() error {
	meter := otel.Meter(storeMeter)

	clusterHasLeader, err := meter.Int64ObservableGauge("cluster_has_leader",
		metric.WithDescription("1 when the main Zen cluster has an elected raft leader, 0 otherwise"))
	if err != nil {
		return err
	}
	nodeIsLeader, err := meter.Int64ObservableGauge("node_is_leader",
		metric.WithDescription("1 when this node is the main Zen cluster raft leader, 0 otherwise"))
	if err != nil {
		return err
	}
	partitionHasLeader, err := meter.Int64ObservableGauge("partition_has_leader",
		metric.WithDescription("1 when the partition has a leader registered in the cluster state (replicated view; see partition_raft_has_leader for the local raft view), 0 otherwise"))
	if err != nil {
		return err
	}
	// partition existence gauges: partition_has_leader only emits series for
	// partitions that already exist in the cluster state, so a bootstrap
	// failure that leaves partitions missing would otherwise be invisible to
	// alerting (absent series never fire NoPartitionLeader)
	clusterPartitions, err := meter.Int64ObservableGauge("cluster_partitions",
		metric.WithDescription("Number of partitions currently present in the cluster state"))
	if err != nil {
		return err
	}
	clusterDesiredPartitions, err := meter.Int64ObservableGauge("cluster_desired_partitions",
		metric.WithDescription("Number of partitions the cluster is configured to have"))
	if err != nil {
		return err
	}
	raftTerm, err := meter.Int64ObservableGauge("raft_term",
		metric.WithDescription("Current raft term of the main Zen cluster"))
	if err != nil {
		return err
	}
	raftLastLogIndex, err := meter.Int64ObservableGauge("raft_last_log_index",
		metric.WithDescription("Last raft log index of the main Zen cluster"))
	if err != nil {
		return err
	}
	raftAppliedIndex, err := meter.Int64ObservableGauge("raft_applied_index",
		metric.WithDescription("Last applied raft log index of the main Zen cluster"))
	if err != nil {
		return err
	}
	raftFsmPending, err := meter.Int64ObservableGauge("raft_fsm_pending",
		metric.WithDescription("Number of raft log entries pending application to the FSM"))
	if err != nil {
		return err
	}

	reg, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		if !s.open.Load() {
			return nil
		}
		o.ObserveInt64(clusterHasLeader, boolToInt64(s.HasLeader()))
		o.ObserveInt64(nodeIsLeader, boolToInt64(s.IsLeader()))

		cs := s.ClusterState()
		o.ObserveInt64(clusterPartitions, int64(len(cs.Partitions)))
		o.ObserveInt64(clusterDesiredPartitions, int64(cs.Config.DesiredPartitions))
		for id, partition := range cs.Partitions {
			o.ObserveInt64(partitionHasLeader, boolToInt64(partition.LeaderId != ""),
				metric.WithAttributes(attribute.Int64("partition", int64(id))))
		}

		stats := s.raft.Stats()
		observeRaftStat(o, raftTerm, stats, "term")
		observeRaftStat(o, raftLastLogIndex, stats, "last_log_index")
		observeRaftStat(o, raftAppliedIndex, stats, "applied_index")
		observeRaftStat(o, raftFsmPending, stats, "fsm_pending")
		return nil
	}, clusterHasLeader, nodeIsLeader, partitionHasLeader, clusterPartitions, clusterDesiredPartitions, raftTerm, raftLastLogIndex, raftAppliedIndex, raftFsmPending)
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

func boolToInt64(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
