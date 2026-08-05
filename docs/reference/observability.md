# Observability

ZenBPM exposes metrics via a Prometheus endpoint, distributed traces via OTLP
(e.g. to Jaeger) and health endpoints for orchestrators and load balancers.

## Endpoints

| Endpoint | Purpose |
| -------- | ------- |
| `GET /system/metrics` | Prometheus metrics scrape endpoint |
| `GET /system/status` | Verbose diagnostic status: full cluster state. Always returns **200** (legacy contract, kept stable for existing consumers) |
| `GET /system/health/live` | Liveness probe. Returns 200 whenever the process is up. Does **not** check raft state, so a leaderless node is not restarted in a loop |
| `GET /system/health/ready` | Node readiness probe. Returns **503** until the main cluster has a raft leader, this node is registered in the cluster state, and all partitions owned by this node are initialized. Cluster-wide partition health is reported by metrics/alerts so one degraded partition does not remove every node from a load balancer |

Health responses have the shape:

```json
{"status": "DOWN", "reasons": ["no cluster leader elected", "partition 2 has no leader"]}
```

`/system/status` returns the raw cluster state (unchanged from previous releases).

## Tracing

- Configured via `tracing` section (`internal/config`): `enabled`, `endpoint`
  (OTLP HTTP), `name`, `samplerRatio` (env `TRACING_SAMPLER_RATIO`, default
  `1.0`; `0` disables root sampling; values outside `[0, 1]` fail startup).
  Sampling is parent-based: child spans follow the parent decision.
- Spans cover: REST requests, BPMN engine operations (instances, tokens,
  flow nodes, jobs, timers, incidents), DMN decision evaluations, and rqlite
  exec/query statements.
- The W3C `TraceContext` + `Baggage` propagators are registered globally.
  gRPC (public and node-to-node) is **not yet instrumented** — cross-partition
  proxied requests currently start separate traces.
- Span attributes use the `zenbpm.` namespace (e.g. `zenbpm.process.instance_key`,
  `zenbpm.job.key`, `zenbpm.decision.id`). Span (operation) names are stable —
  e.g. DMN evaluations use `dmn.evaluate-decision` with the decision id carried
  as an attribute — to keep tracing-backend operation indexes bounded.

> ⚠️ **BREAKING CHANGE:** span attributes were renamed from the legacy `bpmn-*`
> keys (dash separated) to the `zenbpm.*` namespace in this release, with no
> dual-write overlap period. Saved Jaeger searches, collector `attributes`
> processors and tail-sampling policies that reference the old keys **must** be
> updated before upgrading. This must be called out in the release notes.

## Metrics catalog

Prometheus names shown (OpenTelemetry counters get a `_total` suffix, `ms`
histograms a `_milliseconds` suffix).

### Engine (per node)

| Metric | Type | Attributes | Description |
| ------ | ---- | ---------- | ----------- |
| `processes_started_total` | counter | `bpmn_process_id` | Process instances started |
| `processes_completed_total` | counter | `bpmn_process_id` | Process instances ended (completed or failed) |
| `processes_running` | up/down counter | `bpmn_process_id` | Instances currently being executed |
| `process_instance_duration_milliseconds` | histogram | `bpmn_process_id`, `state` | Creation → completion/failure duration |
| `jobs_created_total` / `jobs_completed_total` / `jobs_failed_total` | counter | `type`, `internal` | Job lifecycle counters |
| `job_lifetime_milliseconds` | histogram | `type`, `outcome` | Job creation → terminal state duration |
| `incidents_created_total` / `incidents_resolved_total` | counter | `element_id` | Incident lifecycle counters (recorded only after the write batch is successfully flushed) |
| `timers_scheduled_total` / `timers_fired_total` / `timers_cancelled_total` | counter | — | Timer lifecycle counters (recorded only after the write batch is successfully flushed; covers instance-level timers as well as definition-level timer start events and their cycle renewals) |
| `messages_correlated_total` / `message_correlation_failed_total` | counter | `message_name` (+ `reason` on failures) | Message correlation outcomes. Failed lookups (no active subscription) use `message_name="unknown"`, `reason="subscription_not_found"` — the caller-provided name is never used as a label to keep cardinality bounded. Failures after a subscription was found use the (definition-bounded) real name with `reason="publish_failed"` |

### DMN

| Metric | Type | Attributes | Description |
| ------ | ---- | ---------- | ----------- |
| `dmn_evaluations_total` | counter | `decision_id`, `outcome` | Decision evaluations |
| `dmn_evaluation_duration_milliseconds` | histogram | `decision_id` | Evaluation duration |

### Cluster / raft (main Zen cluster, exported by every node)

| Metric | Type | Attributes | Description |
| ------ | ---- | ---------- | ----------- |
| `cluster_has_leader` | gauge 0/1 | — | Main cluster has an elected leader |
| `cluster_leader_info` | gauge (always 1) | `leader_id`, `leader_addr` | Identity of the current main cluster leader. The series is absent while there is no leader |
| `node_is_leader` | gauge 0/1 | — | This node is the main cluster leader |
| `cluster_node_up` | gauge 0/1 | `node_id`, `node_addr` | Cluster member is in the started state. Read from the replicated cluster state, so **every node emits a series per member** — deduplicate with `max by(node_id, node_addr)` |
| `cluster_node_role` | gauge 0/1 | `node_id`, `node_addr` | Cluster member is the leader (1) or a follower (0). Also replicated — deduplicate the same way |
| `node_uptime_seconds` | gauge | `node_id`, `node_addr` | Seconds since the local node opened its cluster store. Only emitted for the node itself |
| `raft_last_contact_milliseconds` | gauge | `node_id`, `node_addr` | Time since the local node last heard from the raft leader; `0` on the leader, `-1` when there has never been contact |
| `partition_has_leader` | gauge 0/1 | `partition` | Partition leader registered in cluster state (replicated view; see `partition_raft_has_leader` for the local raft view) |
| `cluster_partitions` / `cluster_desired_partitions` | gauge | — | Actual vs. configured partition count; feeds the `PartitionDeficit` alert (missing partitions emit no `partition_has_leader` series, so a count comparison is required) |
| `raft_term`, `raft_last_log_index`, `raft_applied_index`, `raft_fsm_pending` | gauge | — | Raft internals of the main cluster |

### Partition / rqlite

| Metric | Type | Attributes | Description |
| ------ | ---- | ---------- | ----------- |
| `jobs_waiting` | gauge | `partition` | Jobs waiting to be worked on. Exported by **every replica** of a partition — deduplicate with `max by(partition)` before aggregating |
| `process_instances_active` | gauge | `partition` | Active process instances. Exported by every replica — deduplicate with `max by(partition)` |
| `partition_raft_has_leader` | gauge 0/1 | `partition` | Partition raft group has a leader (local raft view; see `partition_has_leader` for the replicated cluster-state view) |
| `partition_node_is_leader` | gauge 0/1 | `partition` | This node leads the partition raft group |
| `partition_leader_changes_total` | counter | `partition` | Leader changes observed by this node (the first election after node start, repeated observations of the same leader and leadership-loss observations are not counted) |
| `rqlite_db_size_bytes` | gauge | `partition` | SQLite files size on disk (db + WAL/SHM) |
| `rqlite_raft_log_size_bytes` | gauge | `partition` | Physical size of the partition's bbolt raft-log file. bbolt reuses freed pages after logical log truncation, so this file does not normally shrink |
| `rqlite_snapshot_age_seconds` | gauge | `partition` | Age of the newest completed partition raft snapshot; `-1` until the first snapshot exists |
| `rqlite_snapshot_observation_age_seconds` | gauge | `partition` | Time since this process observed a new completed snapshot; `-1` until a snapshot is created after process startup |
| `rqlite_exec_duration_milliseconds` | histogram | `partition`, `outcome` | Raft-replicated write duration |
| `rqlite_query_duration_milliseconds` | histogram | `partition`, `outcome` | Read query duration |

### Job manager

| Metric | Type | Attributes | Description |
| ------ | ---- | ---------- | ----------- |
| `jobs_distributed_total` | counter | `type`, `client` | Jobs successfully handed to worker streams |
| `job_activation_latency_milliseconds` | histogram | `type` | Job creation → distribution latency (successful sends only; clamped at 0 to guard against cross-node clock skew) |

### REST / runtime

- `request_total`, `request_uri_total`, `request_body_size`, `response_body_size`,
  `request_duration_milliseconds` — REST server. `request_uri_total` carries
  `path`, `method` and `status` labels.
- Go runtime metrics (`go_*`, `process_*`) — exported by the Prometheus
  `client_golang` default collectors that the `/system/metrics` promhttp
  handler serves.

## Alerting

`make start-monitoring` starts Prometheus with `scripts/prometheus-rules.yml`
(technical + business alerts) and an Alertmanager (`scripts/alertmanager.yml`,
placeholder webhook receiver). Key alerts:

- **NoClusterLeader / NoPartitionLeader / PartitionDeficit** (critical) — driven
  by the leadership and partition-count gauges exported by every node, not by
  probing a single node's health URL. `PartitionDeficit` covers partitions that
  were never created (absent series cannot fire `NoPartitionLeader`).
- **TargetDown, HighErrorRate, RestLatencyDegradation, RqliteExecLatencyDegradation**
- **RqliteDbSizeLarge, RqliteDbGrowthPrediction, DiskSpaceLow** (fires below 20% free space), **HighCPU, HighMemory**
- **ThroughputDrop, RaftLeaderFlapping, GoroutineLeak**
- **IncidentCreated, HighJobFailureRate, JobBacklogGrowing, StuckProcessInstances, NoJobDistribution**

## Dashboards

Provisioned automatically from `scripts/grafana_provisioning/dashboards/zenbpm/`:

- `main.json` — processes, jobs, distribution, request duration
- `cluster.json` — leader identity, partition leaders/deficit, cluster member
  table (role, status, uptime, leader latency), raft health, leader changes
- `incidents.json` — incidents, job failures, message correlation, timers, DMN
- `storage.json` — rqlite DB size/growth, physical raft log size, time since the
  last completed snapshot and since this process observed a new snapshot,
  read/write latency percentiles, disk
- `latency.json` — business latency percentiles and throughput
- `host.json` — node_exporter CPU/memory/disk/network
- `go.json` — Go runtime
