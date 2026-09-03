# Cluster E2E Test Suite — Full Plan

## Design Decisions

| Decision | Choice |
|----------|--------|
| Fault injection | TCP Proxy per node, zero production changes (etcd-style) |
| Execution tiers | `//go:build cluster_e2e` + `testing.Short()` for fast/slow split |
| Port allocation | Dynamic `:0` via OS-assigned free ports |
| Package location | `test/e2e/cluster/` — separate package, own TestMain |
| gRPC/REST resilience | Dedicated Category 9: `stream_resilience_test.go` |
| Partition count | Configurable, default 1, `WithPartitions(n)` override per test |

---

## Architecture

### Two-Tier Cluster Under Test

```
ZenNode (Base Cluster — HashiCorp Raft)
├── Store          — Raft consensus on cluster metadata
├── Controller     — Partition lifecycle management
├── Server         — gRPC API (ZenService)
├── ClientManager  — Inter-node gRPC connections
└── JobManager     — Distributed job management

        ↓ orchestrates

ZenPartitionNode (Subcluster — RqLite Raft) × N
├── RqLite Store   — Partition-level Raft consensus
├── DB             — SQL queries
├── Engine         — BPMN processing (leader only)
└── Script Runtime — Feel/JS (leader only)
```

All inter-node communication is multiplexed over a **single TCP port** per node using header bytes. This is the key property that makes the TCP proxy approach effective.

### Fault Injection: TCP Proxy

Each test node is fronted by an in-process TCP proxy. Other nodes connect to the **proxy address**, not the real address. The proxy can inject faults on all traffic (base Raft, gRPC, partition Raft, partition cluster).

```
Node1 (real :9001) ←→ Proxy1 (:0) ←→ Proxy2 (:0) ←→ Node2 (real :9002)
                         ↑                  ↑
                    fault injection     fault injection
```

**Proxy capabilities:**
- `Blackhole()` — drop all traffic (inbound + outbound)
- `BlockPeer(addr)` — block traffic to/from specific peer
- `UnblockAll()` — restore normal operation
- `SetLatency(d)` — inject delay on all connections
- `SetDropRate(r)` — probabilistic packet drop (0.0–1.0)

**Cluster-level helpers:**
- `IsolateNode(nodeID)` — blackhole one node from all others (bidirectional)
- `PartitionNetwork(groupA, groupB)` — split-brain between two groups
- `HealNetwork()` — restore all connectivity
- `AddLatency(nodeID, duration)` — slow down one node

---

## File Structure

```
test/e2e/cluster/
├── harness.go                     # TestCluster, TestNode, ClusterOption, NodeOption
├── helpers.go                     # Assertion helpers (state convergence, partition balance, etc.)
├── proxy.go                       # TCP proxy implementation (~150 LOC)
├── cluster_suite_test.go          # TestMain, build tag, tier gating
│
│ # Fast tier (PR gate, ~30s)
├── formation_test.go              # Category 1: Cluster formation & membership
├── partition_test.go              # Category 2: Partition lifecycle
├── failover_test.go               # Category 3: Leader failover & election
├── data_test.go                   # Category 4: Data consistency & routing
├── stream_resilience_test.go      # Category 9: gRPC/REST client resilience (fast subset)
│
│ # Slow tier (nightly, ~5min)
├── network_test.go                # Category 5: Network partitions & split-brain
├── scaling_test.go                # Category 6: Dynamic scaling
├── recovery_test.go               # Category 7: Crash recovery & snapshots
└── stress_test.go                 # Category 8: Stress & chaos
```

---

## Test Harness API

### Core Types

```go
type TestCluster struct {
    Nodes    []*TestNode
    Proxies  map[string]*NodeProxy
    TempDirs []string
}

type TestNode struct {
    ID          string
    ZenNode     *cluster.ZenNode
    RestServer  *rest.Server
    GrpcServer  *grpc.Server
    RestClient  *zenclient.ClientWithResponses
    RestAddr    string
    GrpcAddr    string
    ClusterAddr string   // real address
    ProxyAddr   string   // address other nodes connect to
}
```

### Cluster Lifecycle

```go
// Create a 3-node cluster with default settings (1 partition)
c := NewTestCluster(t, 3)
defer c.Teardown(t)

// Create with options
c := NewTestCluster(t, 3,
    WithPartitions(3),
    WithBootstrapExpect(3),
)

// Dynamic operations
c.AddNode(t)                           // add a 4th node
c.StopNode(t, "node-2")               // graceful shutdown
c.KillNode(t, "node-2")               // hard kill (no graceful leave)
c.RestartNode(t, "node-2")            // restart with same data dir
c.RemoveNode(t, "node-2")             // remove from cluster membership
```

### Querying Cluster State

```go
c.Leader()                             // base cluster leader TestNode
c.Followers()                          // base cluster follower TestNodes
c.PartitionLeader(partitionID)         // partition subcluster leader
c.WaitForHealthy(t, 30*time.Second)    // all nodes up, all partitions initialized
c.WaitForPartitions(t, 3, 30*time.Second) // N partitions exist and have leaders
```

### Fault Injection

```go
c.IsolateNode(t, "node-2")                          // full isolation
c.PartitionNetwork(t, []string{"node-1"}, []string{"node-2", "node-3"})  // split-brain
c.HealNetwork(t)                                     // restore all
c.AddLatency(t, "node-2", 500*time.Millisecond)     // slow node
```

### Assertion Helpers

```go
AssertStateConverged(t, c, 10*time.Second)           // all nodes agree on cluster state
AssertPartitionHasLeader(t, c, partitionID)          // exactly one leader per partition
AssertRaftIndicesConverged(t, c, maxDelta)           // replication caught up
AssertPartitionsBalanced(t, c, 0.3)                  // no node has >30% more than average
AssertConsistentRead(t, c, queryFn)                  // same result from every node
AssertProcessReachableFromAllNodes(t, c, instanceKey) // process visible cluster-wide
```

---

## Make Targets

```makefile
test-cluster-e2e:            ## Fast tier — run on every PR (~30s)
    go test -tags cluster_e2e -short ./test/e2e/cluster/... -v -timeout 120s

test-cluster-e2e-full:       ## Full suite — nightly (~5-10min)
    go test -tags cluster_e2e ./test/e2e/cluster/... -v -timeout 600s
```

---

## Category 1: Cluster Formation & Membership (`formation_test.go`)

**Tier: Fast**

| # | Test | Nodes | Description |
|---|------|-------|-------------|
| 1.1 | `TestSingleNodeBootstrap` | 1 | Single node bootstraps, becomes leader, creates partitions, serves REST requests |
| 1.2 | `TestThreeNodeBootstrapExpect` | 3 | Three nodes start with `bootstrap-expect=3`. Cluster forms only when all 3 are up |
| 1.3 | `TestNodeJoinExistingCluster` | 1→3 | Start 1-node cluster, sequentially join 2 more. All 3 see consistent state |
| 1.4 | `TestNodeJoinOrder` | 3 | Nodes join in different orders. Cluster always converges to same state |
| 1.5 | `TestGracefulLeave` | 3→2 | Node gracefully departs. Remaining nodes converge, partitions reassigned |
| 1.6 | `TestNonVoterJoin` | 3+1 | Add a non-voter. Receives state but doesn't affect quorum |
| 1.7 | `TestNodeIDConflict` | 3 | Join with duplicate node ID. Expect rejection or well-defined behavior |
| 1.8 | `TestClusterStatusEndpoint` | 3 | `/system/status` returns consistent node list, roles, partitions from every node |
| 1.9 | `TestBootstrapTimeout` | 2 of 3 | `bootstrap-expect=3` but only 2 start. Verify timeout behavior |
| 1.10 | `TestNodeHeartbeatTimeout` | 3 | Kill node without graceful leave. Eventually marked as shutdown |

---

## Category 2: Partition Lifecycle (`partition_test.go`)

**Tier: Fast** | Default: `WithPartitions(3)` for most tests

| # | Test | Nodes | Partitions | Description |
|---|------|-------|------------|-------------|
| 2.1 | `TestPartitionCreation` | 3 | 3 | All partitions reach INITIALIZED state |
| 2.2 | `TestPartitionAssignment` | 3 | 3 | Each node gets at least 1 partition |
| 2.3 | `TestPartitionStateTransitions` | 3 | 3 | Observe JOINING → INITIALIZING → INITIALIZED on each node |
| 2.4 | `TestPartitionLeaderElection` | 3 | 3 | Each partition elects exactly one leader |
| 2.5 | `TestPartitionReassignmentOnLeave` | 3→2 | 3 | Remove node. Its partitions move to remaining nodes |
| 2.6 | `TestIncreasePartitionCount` | 3 | 1→3 | Dynamically increase partition count. New partitions created and assigned |
| 2.7 | `TestPartitionEngineLifecycle` | 3 | 1 | Only partition leader runs BPMN engine. Deploy process to verify |
| 2.8 | `TestMultiplePartitionsPerNode` | 2 | 4 | Each node hosts 2 partitions. Both work independently |
| 2.9 | `TestMaxPartitions` | 1 | ~122 | Approach the 122 partition limit (network mux byte constraint) |

---

## Category 3: Leader Failover & Election (`failover_test.go`)

**Tier: Fast**

*Inspired by Kafka's `UncleanLeaderElectionTest` and `ControllerFailoverTest`*

| # | Test | Nodes | Description |
|---|------|-------|-------------|
| 3.1 | `TestBaseClusterLeaderFailover` | 3 | Kill base cluster leader. New leader elected. Cluster serves requests |
| 3.2 | `TestBaseClusterLeaderGracefulStepDown` | 3 | Gracefully stop leader. Verify faster failover than hard kill |
| 3.3 | `TestPartitionLeaderFailover` | 3 | Kill partition leader node. New partition leader elected. Requests re-route |
| 3.4 | `TestPartitionLeaderFailoverDuringProcessExecution` | 3 | Multi-step process running. Kill partition leader mid-execution. Process completes on new leader |
| 3.5 | `TestDoubleFailover` | 5 | Kill leader → new leader elected → kill new leader. Third node takes over |
| 3.6 | `TestSimultaneousBaseAndPartitionLeaderFailure` | 5 | Kill node that is both base leader AND partition leader. Both re-elect |
| 3.7 | `TestFailoverPreservesInFlightJobs` | 3 | Activate jobs → kill partition leader → jobs completable via new leader |
| 3.8 | `TestLeaderElectionConvergence` | 5 | Kill leader, measure time until fully healthy. Assert within bounds |
| 3.9 | `TestFollowerFailureNoDisruption` | 3 | Kill a follower. Leader continues serving. No disruption |

---

## Category 4: Data Consistency & Routing (`data_test.go`)

**Tier: Fast** | Default: `WithPartitions(3)` for distribution tests

*Inspired by Kafka's `ProducerConsistencyTest` and Raft linearizable reads*

| # | Test | Nodes | Description |
|---|------|-------|-------------|
| 4.1 | `TestDeployDefinitionRoutesToLeader` | 3 | Deploy BPMN via any node. Available from all nodes |
| 4.2 | `TestCreateInstanceRoutesToPartitionLeader` | 3 | Create instance via follower. Correct partition leader handles it |
| 4.3 | `TestReadAfterWrite` | 3 | Deploy + create on node A. Immediately read from node B. Data present |
| 4.4 | `TestJobCompletionAcrossNodes` | 3 | Create on A, activate on B, complete on C. Process advances |
| 4.5 | `TestMessageCorrelationAcrossNodes` | 3 | Create on A, publish message on B. Correlates correctly |
| 4.6 | `TestDMNEvaluationAcrossNodes` | 3 | Deploy DMN on A, evaluate on B |
| 4.7 | `TestMultiPartitionProcessDistribution` | 3 | 100 instances distribute across partitions (not all on one) |
| 4.8 | `TestIncidentVisibilityAcrossNodes` | 3 | Trigger incident on A's partition. Query from B |
| 4.9 | `TestVariableUpdateConsistency` | 3 | Update vars on A, read from B. No stale reads |
| 4.10 | `TestConcurrentWritesToDifferentPartitions` | 3 | Simultaneous writes to different partitions. All succeed |
| 4.11 | `TestListAggregatesAcrossPartitions` | 3 | List instances/jobs/incidents returns results from ALL partitions |

---

## Category 5: Network Partitions & Split-Brain (`network_test.go`)

**Tier: Slow**

*Inspired by Jepsen-style testing and Kafka's `NetworkPartitionTest`*

| # | Test | Nodes | Description |
|---|------|-------|-------------|
| 5.1 | `TestNodeIsolation` | 3 | Isolate one node. Majority continues. Isolated node steps down |
| 5.2 | `TestSymmetricPartition` | 4 | Split 2+2. Only side with quorum continues |
| 5.3 | `TestAsymmetricPartition` | 5 | Split 3+2. Majority (3) continues. Minority (2) becomes unavailable |
| 5.4 | `TestHealAfterPartition` | 3 | Isolate node → writes continue → heal → isolated node catches up |
| 5.5 | `TestPartitionDuringProcessExecution` | 3 | Start process → isolate partition leader → new leader completes → heal → verify consistency |
| 5.6 | `TestNoSplitBrain` | 5 | Network partition. At most ONE leader for base cluster and each partition at all times |
| 5.7 | `TestPartitionBetweenBaseAndSubcluster` | 3 | Base cluster leader talks to all, but partition subcluster is split. Partition failover while base stays stable |
| 5.8 | `TestSlowNetwork` | 3 | 500ms latency on one node. Cluster functions (slower). No false failovers |

---

## Category 6: Dynamic Scaling (`scaling_test.go`)

**Tier: Slow**

*Inspired by Kafka's partition reassignment and consumer group rebalancing*

| # | Test | Nodes | Description |
|---|------|-------|-------------|
| 6.1 | `TestScaleUpFromOneToThree` | 1→3 | Running 1-node with data. Add 2 nodes. Partitions rebalance. Data accessible |
| 6.2 | `TestScaleDownFromThreeToOne` | 3→1 | Gracefully remove 2 nodes. Last node takes all partitions |
| 6.3 | `TestScaleUpDuringLoad` | 3→5 | Continuous writes while adding 2 nodes. No errors, no data loss |
| 6.4 | `TestScaleDownDuringLoad` | 5→3 | Continuous writes while removing 2 nodes. Brief failures then recovery |
| 6.5 | `TestRollingRestart` | 3 | Restart nodes one at a time. Cluster stays available throughout |
| 6.6 | `TestRapidScaleUpDown` | 3 | Add/remove/add rapidly. Cluster converges to stable state |
| 6.7 | `TestPartitionRebalanceAfterScaleUp` | 3→5 | 6 partitions on 3 nodes. Add 2. Partitions redistribute toward balance |

---

## Category 7: Crash Recovery & Snapshots (`recovery_test.go`)

**Tier: Slow**

*Inspired by Kafka's `LogRecoveryTest` and Raft snapshot testing*

| # | Test | Nodes | Description |
|---|------|-------|-------------|
| 7.1 | `TestNodeRecoveryFromDisk` | 3 | Kill node, restart with same data dir. Rejoins, catches up from Raft log |
| 7.2 | `TestNodeRecoveryAfterLongOutage` | 3 | Kill node, many writes, restart. Catches up via snapshot + log replay |
| 7.3 | `TestAllNodesRestart` | 3 | Stop all 3, restart all 3. Cluster reforms from persisted state |
| 7.4 | `TestColdBootFromPersistence` | 3 | Stop all. Restart in different order. Re-forms, all data intact |
| 7.5 | `TestPartitionRecoveryPreservesProcessState` | 3 | Process at user task → kill partition leader → restart → process still at user task |
| 7.6 | `TestRecoveryAfterDiskCorruption` | 3 | Kill node, corrupt Raft data, restart with clean dir. Rejoins as fresh member |
| 7.7 | `TestBaseClusterSnapshotAndRestore` | 3 | Force snapshot on leader. Kill node, restart from snapshot. State matches |
| 7.8 | `TestPartitionSnapshotAndRestore` | 3 | Same as above for partition-level RqLite Raft |
| 7.9 | `TestConcurrentNodeRecovery` | 5 | Kill 2 simultaneously, restart both. Both rejoin, cluster converges |

---

## Category 8: Stress & Chaos (`stress_test.go`)

**Tier: Slow**

*Inspired by Kafka's `ReplicationStressTest` and Netflix Chaos Monkey*

| # | Test | Nodes | Description |
|---|------|-------|-------------|
| 8.1 | `TestHighThroughputMultiNode` | 3 | Deploy 10 definitions, create 1000 instances, complete all jobs |
| 8.2 | `TestConcurrentDeployments` | 3 | 10 goroutines deploying different definitions across different nodes |
| 8.3 | `TestConcurrentInstanceCreation` | 3 | 50 goroutines creating instances across all nodes |
| 8.4 | `TestChaosMonkey` | 5 | Random kill/restart every 5-10s while workload runs. All instances eventually complete |
| 8.5 | `TestLeaderBounce` | 3 | Kill leader every 3s for 30s during workload. All data consistent at end |
| 8.6 | `TestLongRunningProcessWithFailures` | 3 | 20 sequential tasks, random node failures. Process completes |
| 8.7 | `TestMixedWorkload` | 3 | BPMN + DMN + Messages + Jobs concurrently. No deadlocks, no lost data |
| 8.8 | `TestGracefulDegradation` | 5→2 | Remove nodes until quorum lost. Verify proper error handling |

---

## Category 9: Stream & Client Resilience (`stream_resilience_test.go`)

**Tier: Mixed** (marked per test)

| # | Test | Nodes | Tier | Description |
|---|------|-------|------|-------------|
| 9.1 | `TestGrpcStreamReconnectAfterPartitionLeaderFailover` | 3 | Fast | Active job stream → kill partition leader → client error, reconnect to new leader |
| 9.2 | `TestGrpcStreamDuringNodeIsolation` | 3 | Slow | Stream from node A → isolate A → client retries on node B |
| 9.3 | `TestJobActivateCompleteAcrossFailover` | 3 | Fast | Activate job → leader dies before completion ack → complete via new leader |
| 9.4 | `TestRestRequestDuringLeaderElection` | 3 | Fast | REST requests during base leader election → proper error codes, no silent data loss |
| 9.5 | `TestRestRequestDuringPartitionLeaderElection` | 3 | Fast | REST write during partition failover → clear error or success, never partial |
| 9.6 | `TestGrpcStreamUnderLatency` | 3 | Slow | 500ms latency → stream stays alive, no false timeouts |
| 9.7 | `TestConcurrentStreamsMultiplePartitions` | 3 | Slow | Multiple streams to different partitions → kill one leader → only affected stream errors |
| 9.8 | `TestRestIdempotencyAfterRetry` | 3 | Fast | Request times out, client retries → no duplicate instances or jobs |

---

## Test Count Summary

| Category | File | Fast | Slow | Total |
|----------|------|:----:|:----:|:-----:|
| 1. Formation | `formation_test.go` | 10 | — | 10 |
| 2. Partition Lifecycle | `partition_test.go` | 9 | — | 9 |
| 3. Failover | `failover_test.go` | 9 | — | 9 |
| 4. Data Consistency | `data_test.go` | 11 | — | 11 |
| 5. Network | `network_test.go` | — | 8 | 8 |
| 6. Scaling | `scaling_test.go` | — | 7 | 7 |
| 7. Recovery | `recovery_test.go` | — | 9 | 9 |
| 8. Stress | `stress_test.go` | — | 8 | 8 |
| 9. Client Resilience | `stream_resilience_test.go` | 4 | 4 | 8 |
| **Total** | | **43** | **36** | **79** |

---

## Implementation Status (2026-03-15)

All test files implemented. Fast-tier results from first run:

| Category | Pass | Fail | Skip | Blocker |
|----------|:----:|:----:|:----:|---------|
| 1. Formation | 6 | 0 | 0 | — |
| 2. Partition | 3 | 3 | 1 | DesiredPartitions hardcoded to 1 |
| 3. Failover | 6 | 3 | 0 | Partition leader re-election timeout |
| 4. Data | 1 | 8 | 0 | Deploy not visible from non-leader nodes |
| 5. Network | 0 | 0 | 8 | Slow tier (not run in -short) |
| 6. Scaling | — | — | — | Not run (timeout) |
| 7. Recovery | — | — | — | Not run (slow tier) |
| 8. Stress | — | — | — | Not run (slow tier) |
| 9. Stream | — | — | — | Not run (timeout) |

### Key Blockers

1. **DesiredPartitions hardcoded to 1** (`store.go:125`, `server.go:177` unimplemented) — blocks all multi-partition tests
2. **Partition leader failover** — after killing partition leader, subcluster doesn't re-elect within 60s
3. **Data routing after deploy** — definitions deployed via non-leader not immediately queryable from same node
