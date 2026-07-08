# Clustering Implementation Plan

> Last updated: 2026-07-08

## Progress Summary

| Phase | Name | Status | Progress |
|-------|------|--------|----------|
| 1 | Partition Leader Propagation | **Done** (2026-07-08) — all blockers fixed incl. 1.3 formation livelock; DoD met; E2E green & blocking in CI | 2/2 code, E2E in CI (blocking) |
| 2 | Configurable Partitions | Not Started | 0/5 |
| 3 | Data Consistency & Routing | In Progress | 2/4 |
| 4 | Partition Rebalancing & Node Lifecycle | Not Started | 0/4 |
| 5 | Job Manager Multi-Node Support | Not Started | 0/4 |
| 6 | Hardening & Slow-Tier Tests | Not Started | 0/4 |
| 7 | Voter-Only Nodes | Not Started | 0/7 |

---

## Dependency Graph

```
Phase 1 (Partition Leader Prop.)
  ├── 1.1 partitionLeaderChange
  └── 1.2 Engine lifecycle
         │
         ├──────────────────────────────┐
         │                              │
Phase 2 (Configurable Partitions)  Phase 3 (Data Consistency)
  ├── 2.1 Proto changes              ├── 3.1 Leader reads (Won't fix)
  ├── 2.2 FSM handler                ├── 3.2 Off-by-one fix
  ├── 2.3 Initial config             ├── 3.3 Message correlation
  ├── 2.4 Config RPC                 └── 3.4 DMN visibility (Won't fix)
  └── 2.5 Controller loop
         │                              │
         ├──────────────┬───────────────┘
         │              │
Phase 4 (Rebalancing)  Phase 5 (Job Manager)
  ├── 4.1 Assignment   ├── 5.1 Enable role change
  ├── 4.2 Shutdown     ├── 5.2 Stream reconnect
  ├── 4.3 Reap cleanup ├── 5.3 Job rejection
  ├── 4.4 Handoff      └── 5.4 Throttling
  └── 4.5 Node lifecycle callbacks + RPCs
         │              │
         └──────┬───────┘
                │
Phase 6 (Hardening)               Phase 7 (Voter-Only Nodes)
  ├── 6.1 Slow-tier tests            ├── 7.1 Node role config + state
  ├── 6.2 Backup/restore RPCs        ├── 7.2 Base cluster decline leadership
  ├── 6.3 Parallel queries           ├── 7.3 Partition assignment excludes voters from leader
  └── 6.4 Incremental migrations     ├── 7.4 Partition Raft decline leadership
                                     ├── 7.5 Engine lifecycle guard
                                     ├── 7.6 Quorum safety validation
                                     └── 7.7 Tests
```

Note: Phase 2 and Phase 3 can be done in parallel after Phase 1. Phase 4 and Phase 5 can be done in parallel after Phases 2+3. Phase 7 depends on Phase 4 and can run in parallel with Phase 5 and 6.

---

## Phase 1: Partition Leader Propagation

**Goal:** When a partition elects a new leader, the base cluster state reflects it. Engine starts/stops accordingly.
**Dependency:** None — start here.

**Definition of Done — 1 partition, 3 nodes, production-viable:**
- [x] 3 nodes form a cluster; 1 partition is created with all 3 nodes in its Raft group (proper quorum, `BootstrapExpect=3`) — `TestThreeNodeWithPartition` passes
- [x] Partition leader failover works without crash: kill the partition leader, a new leader is elected, engine starts on the new leader — `TestPartitionLeaderFailover` passes 5/5
- [x] Engine stops on the old leader (or crashed node recovers without duplicate engine) — `stopEngineIfRunning` in `handlePartitionStateInitialized`
- [x] Write on any node, read from any node (data replicates through partition Raft) — `TestReadAfterWrite` passes; unblocked by the `BootstrapExpect` wiring, lifecycle-ctx, schema-gate, and deploy-retry fixes (see Phase 1 Blocker History below)
- [x] No `panic("unimplemented")` reachable in this configuration
- [x] E2E: `TestThreeNodeWithPartition` ✅, `TestPartitionLeaderFailover` ✅, `TestReadAfterWrite` ✅ — each passes in isolation and in most suite runs; suite-level stability is gated by the formation churn item under "Remaining Work to Close Phase 1"

### 1.1 — Implement `partitionLeaderChange` callback + RPC — **DONE**

- [x] Replace `panic("unimplemented")` in `partitionLeaderChange` (`controller.go`)
- [x] Map RqLite `ServerID` (format: `zen-{nodeId}-partition-{id}`) back to ZenNode ID — `parsePartitionServerID` helper
- [x] Write `NodePartitionChange{nodeId, partitionId, State=INITIALIZED, Role=LEADER}` to base store
- [x] Write `NodePartitionChange{oldLeaderId, partitionId, State=INITIALIZED, Role=FOLLOWER}` for the old leader
- [x] Verify `state.Partitions[id].LeaderId` is updated
- [x] Replace `panic("unimplemented")` in `PartitionNodeLeaderChange` RPC (`server.go`)
- [x] Retry loop for `ClusterLeader()` lookups (handles the base-cluster-re-election race when killing a node that was both base and partition leader)

**Files:** `internal/cluster/controller/controller.go`, `internal/cluster/server/server.go`
**Tests:**
- `TestParsePartitionServerID` — 8 cases
- `TestPartitionNodeLeaderChange_WritesLeaderAndDemotesOldLeader`, `…_SameLeaderIsNoOp`, `…_FirstElection`
- `TestPartitionLeaderChange_UpdatesClusterState`, `…_EmptyServerIDIsNoOp`, `…_InvalidServerIDReturnsError`

### 1.2 — Engine lifecycle on leadership change — **DONE**

- [x] When this node becomes partition leader: create engine + script runtimes (if not already running)
- [x] When this node loses partition leadership: stop engine, nil out `Engine` field, stop runtimes
- [x] Handle engine creation/start failures (return early, nil out partial state, allow retry on next `ClusterStateChangeNotification`)
- [x] Refactored into `startEngineIfLeader` / `stopEngineIfRunning` helpers — used by both `handlePartitionStateInitializing` and `handlePartitionStateInitialized`

**Files:** `internal/cluster/controller/controller.go`
**Tests:**
- `TestEngineStartsOnRegainedPartitionLeadership`

**Also fixed (discovered during Phase 1):**
- **Bug: nil observer channel in `partition.go`** — `observerChan` was declared as a local variable, shadowing the uninitialized struct field `zpn.observerChan` that the observer goroutine was reading from. This meant no partition Raft events (including leader changes) ever reached our callbacks. Without this fix, Phase 1 would appear implemented but nothing would actually work.
- **4 controller callbacks + 4 server RPCs** replaced `panic("unimplemented")` with minimal logging stubs: `partitionAddNewNode`, `partitionShutdownNode`, `partitionRemoveNode`, `partitionResumeNode`, and server RPCs `AddPartitionNode`, `RemovePartitionNode`, `ResumePartitionNode`, `ShutdownPartitionNode`.

### Phase 1 Blocker History (all resolved)

Every blocker that previously gated Phase 1 is fixed and verified in code (2026-07-08 audit):

1. **Partition RqLite Raft replication not catching up followers** (`failed to get previous log: ... log not found`) — root cause was a hardcoded `BootstrapExpect: 1` in `internal/cluster/partition/config.go`: every node bootstrapped its own 1-node partition Raft cluster, so nothing replicated. Fixed in `b3150af2`: `GetRqLiteDefaultConfig` takes `bootstrapExpect`, wired from `Config.Raft.BootstrapExpect` (`controller.go:81`).
2. **Controller context cancelled by rapid FSM applies** — followers got stuck in `handlePartitionStateInitializing` because each FSM `Apply` cancelled the previous apply-scoped ctx. Fixed in `4e5305d5`: `ClusterStateChangeNotification` ignores the apply ctx and runs handlers on the controller lifecycle ctx (`controller.go:96-100`).
3. **Migration/engine-start race** (two layers: `no such table: process_definition`, then `no engine available`/`store not open`) — fixed in `1fd7ff73` (schema gate: `waitForSchema`/`DB.SchemaReady` before a node advertises INITIALIZED) and `2068fe49` (bounded `retryDeploy` on transient windows, re-resolving the partition leader each attempt).
4. **Deploy-then-immediate-read test anti-pattern** — accepted eventual consistency, not an engine bug; the affected tests poll via `GetFirstDefinitionKey` (`7b9f1811`).
5. **E2E harness FEEL pool hang** — harness config struct literals omitted the `Script` section, producing a zero-capacity FEEL VM pool that deadlocked on first evaluation. Fixed in `df1cc315` (harness sets script pool config).

All cross-node read-after-write E2E tests pass (`TestReadAfterWrite`, `TestCreateInstanceRoutesToPartitionLeader`, `TestJobCompletionAcrossNodes`, `TestDMNEvaluationAcrossNodes`, `TestConcurrentWritesToDifferentNodes`, `TestPartitionLeaderFailoverDuringProcessExecution`, `TestFailoverPreservesInFlightJobs`), alongside the formation/failover tests (`TestThreeNodeWithPartition`, `TestPartitionLeaderFailover`, `TestSimultaneousBaseAndPartitionLeaderFailure`).

### Remaining Work to Close Phase 1

- [x] **CI visibility (the root cause of this doc going stale):** `test/e2e/cluster/` is gated behind `//go:build cluster_e2e`, which nothing in the Makefile or CI referenced — the suite ran only by hand and silently rotted twice. FIXED 2026-07-08: `make test-e2e-cluster` (fast tier, `-short`) runs as a **blocking** step in the `go-test` CI workflow; `make test-e2e-cluster-slow` runs the full suite manually. Tests requiring unfinished phases carry a `t.Skip` with a phase reference (6 multi-partition tests skip until Phase 2).
- [x] **Leaked background goroutines after `partition.Stop()`:** the observer goroutine (5s metrics ticker) and the data-cleanup scheduler were never stopped — every stopped partition left them hammering the closed store (`store not open` log floods; a 2026-07-08 full-suite run accumulated ~27 failed queries/s by test 20, degrading later cluster formations). FIXED 2026-07-08: `Stop()` closes the observer channel and `DB.Stop()` terminates the cleanup scheduler; `TestDataCleanup` deflaked the same way.
- [x] Fast-tier deploy-then-immediate-read sites swept onto `GetFirstDefinitionKey` polling (2026-07-08: 3 sites in `stream_resilience_test.go`; `data_test.go` sites were done earlier). Remaining sites are slow-tier only (`stress_test.go`, `scaling_test.go`, `recovery_test.go`) — sweep them with Phase 6 slow-tier hardening.
- [x] `TestPartitionStateTransitions` string-enum rot fixed (2026-07-08) — it was the comparison site missed by the 2026-06-30 `helpers.go` fix. The durable cure (generating status types from `openapi/system.yaml` so drift breaks at compile time) remains a follow-up.
- [x] **Formation livelock — Phase 1.3, the last real Phase 1 stability gap (ROOT-CAUSED & FIXED 2026-07-08):** the intermittent "2-4 random tests fail at `WaitForHealthy`, stuck formations never converge" symptom was a deadlock, not slow election. Chain: (1) a follower's INITIALIZING partition-change is applied on the cluster leader first and only later replicated back into the follower's own FSM, so during that window the follower's local state still reads `JOINING` while its partition node is already running; (2) any state-change notification in that window re-entered `handlePartitionStateJoining`, whose guard only skipped on *advanced* state, not on an *already-running* partition, so it called `StartZenPartitionNode` a second time; (3) the duplicate rqlite mux listener registration panics (`listener already registered under header byte: 11`); (4) `performMemberOperations` held `partitionsMu` with a plain `Unlock()` (no `defer`), and safego recovered the panic *above* the unlock — leaving the mutex locked forever. Every later partition-state notification on that node then deadlocked, so it could never advertise INITIALIZED regardless of timeout. FIX: `handlePartitionStateJoining` returns early when `c.partitions[partitionId]` already exists (the INITIALIZING write is still re-sent first, covering the lost-write case); all `partitionsMu` critical sections in `performMemberOperations` and the join-retry goroutine now unlock via `defer` inside closures so no recovered panic can leak the lock. Regression test: `TestJoiningReentryDoesNotRestartRunningPartition` (reproduces the exact panic deterministically). Verified: two back-to-back fast-tier suite runs green, 0 panics, 0 failures (previously 2-4 failures + 1-3 panics per run); suite wall-clock dropped ~350-577s→~181s (no nodes riding the 150s deadlock timeout).
- [ ] Engine-side residue (owned by engine team, `pkg/bpmn` is outside cluster scope): `timer_manager.go` keeps polling the partition store briefly after engine stop (`store not open` noise, bounded ~150 log lines/run after the cluster-side leak fix). Related to the known timer duplicate-fire item.
- [ ] Architectural note (candidate future hardening, not a blocker): cluster state can advertise partition leadership before local engine/store serviceability holds. The deploy retry covers the write path and the Phase 1.3 fix closes the formation deadlock; other leader-routed paths could still see brief transient windows. A deeper coordination fix (state leadership reflects actual engine/store readiness) remains optional.

---

## Phase 2: Configurable Partitions

**Goal:** Allow changing `DesiredPartitions` through Raft consensus instead of hardcoding to 1.
**Dependency:** Phase 1

### 2.1 — Add `TYPE_CONFIGURATION_CHANGE` to protobuf

- [ ] Add new command type and message to `zencommand.proto`
- [ ] Run `make generate`

```protobuf
message Command {
  enum Type {
    ...
    TYPE_CONFIGURATION_CHANGE = 4;
  }
  oneof request {
    ...
    ConfigurationChange configuration_change = 4;
  }
}

message ConfigurationChange {
  uint32 desired_partitions = 1;
}
```

**Files:** `internal/cluster/command/proto/zencommand.proto`

### 2.2 — FSM: handle configuration change

- [ ] Add `applyConfigurationChange` method to FSM
- [ ] Update `state.Config.DesiredPartitions` from command

**Files:** `internal/cluster/store/fsm.go`

### 2.3 — Store: initial config from app config + write method

- [ ] Remove hardcoded `DesiredPartitions: 1` at `store.go:125`
- [ ] Read from `config.Cluster` and apply as initial Raft command on bootstrap
- [ ] Add `WriteConfigurationChange()` method to Store

**Files:** `internal/cluster/store/store.go`

### 2.4 — Server: implement `ConfigurationUpdate` RPC

- [ ] Replace `panic("unimplemented")` in `ConfigurationUpdate` (`server.go:178`) with real implementation
- [ ] Validate request, call `store.WriteConfigurationChange()`

**Files:** `internal/cluster/server/server.go`

### 2.5 — Controller: create partitions in a loop

- [ ] Fix `performLeaderOperations` to create all missing partitions, not just one per state change

```go
// Current (creates one):
if int(cs.Config.DesiredPartitions) > currentPartitionCount {
    c.assignNewPartition(ctx, currentPartitionCount+1)
}

// Fixed (creates all missing):
for currentPartitionCount < int(cs.Config.DesiredPartitions) {
    c.assignNewPartition(ctx, currentPartitionCount+1)
    currentPartitionCount++
}
```

**Files:** `internal/cluster/controller/controller.go`
**Tests:**
- `TestConfigurationChangeViaRaft` — write config change, verify all nodes see new DesiredPartitions
- `TestPartitionScaleUp` — change from 1→3, verify 3 partitions created and assigned
- `TestConfigurationUpdateRPC` — call RPC, verify config propagated
- `TestInitialConfigFromAppConfig` — bootstrap with DesiredPartitions=3, verify state

**Unblocks E2E:**
- `TestPartitionCreation`
- `TestPartitionAssignment`
- `TestMultiplePartitionsPerNode`
- `TestMaxPartitions`
- `TestIncreasePartitionCount`
- `TestListAggregatesAcrossPartitions`
- `TestConcurrentStreamsMultiplePartitions`

---

## Phase 3: Data Consistency & Routing

**Goal:** Deploys and queries return consistent results from any node.
**Dependency:** Phase 1

### 3.1 — ~~Fix read-after-write for definitions~~ (Won't fix)

**Decision:** Keep current eventual consistency (follower reads). In real usage there is always enough time between deploy and first process start — replication lag is not a practical problem. No code change needed.

**Files:** N/A

### 3.2 — Fix `LeastStressedPartition` off-by-one panic

- [ ] Fix `rand.Intn(len(c.Partitions) - 1)` → `rand.Intn(len(c.Partitions))`
- [ ] Consider replacing random selection with actual load-based selection

**Files:** `internal/cluster/state/state.go` (line 98)

### 3.3 — Fix message correlation routing across partitions

- [ ] `PublishMessage` hashes correlation key to pick a partition for subscription lookup, but subscription lives on the partition where instance was created (random)
- [ ] Fix: broadcast `FindActiveMessageSubscriptionPointer` across all partition DBs, or add a routing index

**Files:** `internal/cluster/node.go` (`PublishMessage` method)

### 3.4 — ~~Fix DMN cross-node visibility~~ (Won't fix)

**Decision:** Same as 3.1 — eventual consistency is acceptable. No code change needed.

**Files:** N/A
**Tests:**
- `TestLeastStressedPartitionSinglePartition` (3.2)
- `TestMessageCorrelationMultiPartition` (3.3)

**Unblocks E2E:**
- `TestMessageCorrelationAcrossNodes` (3.3)

---

## Phase 4: Partition Rebalancing & Node Lifecycle

**Goal:** Partitions distribute evenly across nodes. Nodes join/leave cleanly. All node lifecycle panics resolved.
**Dependency:** Phase 2

### 4.1 — Fix partition assignment to balance across nodes

- [ ] Fix `performLeaderOperations` — new nodes always assigned to partition 1 (`controller.go:124`)
- [ ] Assign unassigned nodes to partition with fewest members

**Files:** `internal/cluster/controller/controller.go`

### 4.2 — Implement shutdown notification

- [ ] Replace `panic("unimplemented")` in `partitionShutdownNode` (`controller.go`)
- [ ] Replace `panic("unimplemented")` in `ShutdownPartitionNode` RPC (`server.go:208`)
- [ ] `NotifyShutdown()` calls base cluster leader to mark this node's partitions as LEAVING
- [ ] Wait for partition handoff (or timeout)

**Files:** `internal/cluster/controller/controller.go` (line 444), `internal/cluster/server/server.go`

### 4.3 — Clean up reaped node partition entries

- [ ] When a node is reaped in `store.remove()`, also write `NodePartitionChange{State=LEAVING}` for each of its partitions
- [ ] Prevent reaped nodes from "owning" partitions in state

**Files:** `internal/cluster/store/store.go` (`remove` method)

### 4.4 — Partition handoff on node departure

- [ ] When leader detects node is SHUTDOWN, check partition quorum
- [ ] Reassign partition members from healthy nodes if needed
- [ ] Handle graceful drain of in-flight work in `handlePartitionStateLeaving`

**Files:** `internal/cluster/controller/controller.go`

### 4.5 — Implement remaining node lifecycle callbacks + RPCs

- [ ] Replace `panic("unimplemented")` in `partitionAddNewNode` (`controller.go`) — write `NodePartitionChange{State=JOINING}` for new partition member
- [ ] Replace `panic("unimplemented")` in `partitionRemoveNode` (`controller.go`) — remove node from partition membership in base state
- [ ] Replace `panic("unimplemented")` in `partitionResumeNode` (`controller.go`) — re-mark node's partition as active after heartbeat resumed
- [ ] Replace `panic("unimplemented")` in `AddPartitionNode` RPC (`server.go:197`)
- [ ] Replace `panic("unimplemented")` in `RemovePartitionNode` RPC (`server.go:200`)
- [ ] Replace `panic("unimplemented")` in `ResumePartitionNode` RPC (`server.go:204`)
- [ ] Replace `panic("unimplemented")` in `AssignPartition` RPC (`server.go:182`)
- [ ] Replace `panic("unimplemented")` in `UnassignPartition` RPC (`server.go:185`)

**Files:** `internal/cluster/controller/controller.go`, `internal/cluster/server/server.go`

**Tests:**
- `TestPartitionBalancedAssignment`
- `TestNodeDepartureReassignsPartitions`
- `TestGracefulShutdownNotifiesCluster`
- `TestReapedNodePartitionsCleanedUp`
- `TestPartitionAddNewNodeCallback`
- `TestPartitionRemoveNodeCallback`
- `TestPartitionResumeNodeCallback`
- `TestPartitionShutdownNodeCallback`

**Unblocks E2E:**
- `TestGracefulLeave`
- `TestPartitionReassignmentOnLeave`
- `TestScaleDownFromThreeToOne`
- `TestScaleUpFromOneToThree`

---

## Phase 5: Job Manager Multi-Node Support

**Goal:** Job distribution works correctly across partition leader changes and multiple partitions.
**Dependency:** Phase 1, Phase 2

### 5.1 — Enable `OnPartitionRoleChange` in job manager

- [ ] Uncomment `m.OnPartitionRoleChange(ctx)` at `manager.go:161`
- [ ] Verify job server starts/stops when partition leadership changes

**Files:** `internal/cluster/jobmanager/manager.go`

### 5.2 — Implement stream reconnection

- [ ] When `handleJobStreamRecv` gets EOF/error (`client.go:161`), backoff and reconnect
- [ ] Call `updateNodeSubs` to refresh leader info, re-subscribe to new partition leader

**Files:** `internal/cluster/jobmanager/client.go`

### 5.3 — Implement job rejection handling

- [ ] `onJobRejected` (`server.go:417`) should remove job from `distributedJobs` to unlock it
- [ ] Job becomes available again in next `distributeJobs` cycle

**Files:** `internal/cluster/jobmanager/server.go`

### 5.4 — Client-side job throttling

- [ ] Add `maxActiveJobs` per client tracking in job client
- [ ] Don't forward jobs to clients that are at capacity

**Files:** `internal/cluster/jobmanager/client.go`
**Tests:**
- `TestJobManagerPartitionLeaderChange`
- `TestJobStreamReconnection`
- `TestJobRejectionRequeue`
- `TestJobClientThrottling`

**Unblocks E2E:**
- `TestJobCompletionAcrossNodes`
- `TestJobActivateCompleteAcrossFailover`
- `TestGrpcStreamReconnectAfterPartitionLeaderFailover`

---

## Phase 6: Hardening & Slow-Tier Tests

**Goal:** Run and pass all 79 e2e tests including slow tier. Polish remaining rough edges.
**Dependency:** Phases 1-5

### 6.1 — Run and triage slow-tier e2e tests

- [ ] Run categories 5-8 (network, scaling, recovery, stress)
- [ ] Triage and fix failures

**Test categories:**
| Category | File | Tests |
|----------|------|-------|
| 5. Network | `network_test.go` | 8 |
| 6. Scaling | `scaling_test.go` | 5-7 |
| 7. Recovery | `recovery_test.go` | 7-9 |
| 8. Stress | `stress_test.go` | 8-9 |
| 9. Stream (slow subset) | `stream_resilience_test.go` | 4 |

### 6.2 — Implement backup/restore RPCs

- [ ] Replace `panic("unimplemented")` in `ClusterBackup` RPC (`server.go:170`)
- [ ] Replace `panic("unimplemented")` in `ClusterRestore` RPC (`server.go:174`)
- [ ] Replace `panic("unimplemented")` in `PartitionBackup` RPC (`server.go:188`)
- [ ] Replace `panic("unimplemented")` in `PartitionRestore` RPC (`server.go:191`)

**Files:** `internal/cluster/server/server.go`
**Needed by:** Category 7 recovery tests

### 6.3 — Parallelize cross-partition queries

- [ ] `GetJobs` (`node.go:864`) — query partitions in goroutines instead of sequentially
- [ ] `GetProcessInstances` (`node.go:940`) — same

**Files:** `internal/cluster/node.go`

### 6.4 — Incremental migrations

- [ ] Track applied migrations, only run missing ones
- [ ] Current code at `controller.go:374` re-applies all migrations every engine start

**Files:** `internal/cluster/controller/controller.go`

---

## Phase 7: Voter-Only Nodes

**Goal:** Support nodes that participate in Raft quorum (base cluster + all partition groups) but never become leaders and never run engine workload. Enables witness/arbiter deployments — e.g., 2 workers + 1 voter for 3-node quorum with 2 active workloads.
**Dependency:** Phase 4 (partition assignment and rebalancing must respect node roles).

**Model:** voter-only node joins both Raft tiers as a full voter (replicates logs, counts toward quorum). In standard Raft, voters must hold the log — a "quorum-only no-data" node would require forking Raft with arbiter semantics (see Risks). Cost: disk + replication bandwidth on voter nodes. Gain: no engine CPU, no job traffic.

### 7.1 — Node role config + state

- [ ] Add `Role: voter|worker` field to app cluster config (`conf/`)
- [ ] Default to `worker` when unset (backward compat with existing deployments)
- [ ] Add `Role` enum to `state.Node` (`VOTER`, `WORKER`)
- [ ] Add `node_role` field to `NodeChange` protobuf message
- [ ] FSM applies role to `state.Nodes[id].Role`
- [ ] Surface role via `ClusterState()` for downstream decisions

**Files:** `conf/`, `internal/cluster/state/state.go`, `internal/cluster/command/proto/zencommand.proto`, `internal/cluster/store/fsm.go`, `internal/cluster/store/store.go`

### 7.2 — Base cluster: voter-only declines leadership

- [ ] Subscribe to base cluster leadership changes (existing `raft.LeaderObservation` channel in `Store`)
- [ ] If self is voter-only and self became leader: immediately call `raft.LeadershipTransfer()` to a worker peer
- [ ] Pick transfer target from `ClusterState().Nodes` filtered to `Role=WORKER`
- [ ] If no worker peer is available: log warning, retain leadership as fail-safe (cluster is unhealthy but not stuck)
- [ ] Accept a brief leadership window during the transfer — no forking Raft

**Files:** `internal/cluster/store/store.go`

### 7.3 — Partition membership: voters join groups, never lead

- [ ] `performLeaderOperations` and rebalancing MUST add voter-only nodes to partition Raft groups — they count toward partition quorum
- [ ] When selecting a partition leader candidate in `NodePartitionChange{Role=LEADER}` writes, filter eligible nodes to `Role=WORKER`
- [ ] Initial partition assignment: distribute members across both worker + voter nodes; elect leader from workers only
- [ ] Rebalancing on node join/leave: preserve invariant that voters are members but never leaders

**Files:** `internal/cluster/controller/controller.go`

### 7.4 — Partition Raft: voter-only declines partition leadership

- [ ] In `partition.observe()`, on `raft.LeaderObservation` where self is the partition leader and self is voter-only: call partition Raft `LeadershipTransfer()` to a worker peer
- [ ] Target selection: query `ClusterState` for other partition members filtered to `Role=WORKER`
- [ ] If no worker peer in the partition: log and retain (defensive; should be prevented by 7.3 and 7.6)

**Files:** `internal/cluster/partition/partition.go`

### 7.5 — Engine lifecycle guard (defense in depth)

- [ ] In `handlePartitionStateInitialized`: before starting engine, check `self.Role`. If voter-only, skip engine start even if `Role=LEADER` is set
- [ ] Symmetric guard when deciding whether to stop engine — a voter should never have a running engine in the first place
- [ ] Guards against races between 7.4's transfer and engine-start callbacks

**Files:** `internal/cluster/controller/controller.go`

### 7.6 — Quorum safety validation

- [ ] On node join and config change: validate that the resulting topology has a reachable leader for every partition (≥1 worker per partition)
- [ ] Reject configs where losing any single worker would leave a partition with no leader-eligible node
- [ ] Warn (not reject) if voter count ≥ worker count across the cluster (unusual but not invalid)

**Files:** `internal/cluster/controller/controller.go`, `internal/cluster/store/store.go`

### 7.7 — Tests

**Unit:**
- [ ] `TestVoterOnlyJoinsPartitionGroup` — voter is added as partition Raft member after join
- [ ] `TestVoterOnlyNeverBecomesBaseClusterLeader` — leadership transfer fires on election
- [ ] `TestVoterOnlyNeverBecomesPartitionLeader` — partition leadership transfer fires on election
- [ ] `TestVoterOnlyDoesNotRunEngine` — engine guard prevents start even with LEADER role
- [ ] `TestPartitionAssignmentFiltersLeaderToWorkers` — voter never gets `Role=LEADER` in `NodePartitionChange`
- [ ] `TestQuorumValidationRejectsNoWorkerPartition` — config validation fails when all members are voters

**E2E (unblocks / new):**
- [ ] `TestVoterOnlyTopology3Node` — 2 workers + 1 voter cluster forms, partition has all 3 as members, only workers lead
- [ ] `TestTwoWorkerOneVoterSurvivesWorkerFailure` — kill a worker; partition still has quorum (voter + remaining worker); failover to remaining worker
- [ ] `TestPartitionQuorumWithVoter` — writes + reads succeed through failover

**Files:** `internal/cluster/store/store_test.go`, `internal/cluster/controller/controller_test.go`, `internal/cluster/partition/partition_test.go`, `test/e2e/` (new voter topology test file)

---

## Panic Tracker

All `panic("unimplemented")` calls and the phase where each gets resolved:

| Location | Function/RPC | Resolved In | Status |
|----------|-------------|-------------|--------|
| `controller.go` | `partitionLeaderChange` | Phase 1.1 | ✅ Done (full impl) |
| `controller.go` | `partitionAddNewNode` | Phase 4.5 | ✅ Phase 1 logging stub; full impl Phase 4.5 |
| `controller.go` | `partitionShutdownNode` | Phase 4.2 | ✅ Phase 1 logging stub; full impl Phase 4.2 |
| `controller.go` | `partitionRemoveNode` | Phase 4.5 | ✅ Phase 1 logging stub; full impl Phase 4.5 |
| `controller.go` | `partitionResumeNode` | Phase 4.5 | ✅ Phase 1 logging stub; full impl Phase 4.5 |
| `server.go` | `ClusterBackup` | Phase 6.2 | ⏳ still panics |
| `server.go` | `ClusterRestore` | Phase 6.2 | ⏳ still panics |
| `server.go` | `ConfigurationUpdate` | Phase 2.4 | ⏳ still panics |
| `server.go` | `AssignPartition` | Phase 4.5 | ⏳ still panics |
| `server.go` | `UnassignPartition` | Phase 4.5 | ⏳ still panics |
| `server.go` | `PartitionBackup` | Phase 6.2 | ⏳ still panics |
| `server.go` | `PartitionRestore` | Phase 6.2 | ⏳ still panics |
| `server.go` | `PartitionNodeLeaderChange` | Phase 1.1 | ✅ Done (full impl) |
| `server.go` | `AddPartitionNode` | Phase 4.5 | ✅ Phase 1 no-op stub; full impl Phase 4.5 |
| `server.go` | `RemovePartitionNode` | Phase 4.5 | ✅ Phase 1 no-op stub; full impl Phase 4.5 |
| `server.go` | `ResumePartitionNode` | Phase 4.5 | ✅ Phase 1 no-op stub; full impl Phase 4.5 |
| `server.go` | `ShutdownPartitionNode` | Phase 4.2 | ✅ Phase 1 no-op stub; full impl Phase 4.2 |

All panics reachable in the 3-node, 1-partition scenario are removed. The 7 remaining `⏳` server panics fire only on operations not exercised in Phase 1 (backup/restore, config change, rebalancing).

---

## Risks & Open Questions

| # | Question | Affects | Status |
|---|----------|---------|--------|
| 1 | RqLite ServerID → ZenNode ID mapping reliability | Phase 1.1 | Open |
| 2 | Read consistency strategy: leader reads vs replication wait | Phase 3.1 | **Resolved** — keep eventual consistency, no change needed |
| 3 | Message correlation routing: broadcast vs routing index | Phase 3.3 | Open |
| 4 | Partition count reduction (scale DOWN) — data migration needed? | Phase 4 | Out of scope for now |
| 5 | Concurrent partition creation throttling | Phase 2.5 | Open |
| 6 | Voter-only nodes: replicate data or fork Raft for arbiter semantics? | Phase 7 | **Resolved** — replicate data (standard Raft safety). Arbiter-style is a separate, out-of-scope project. |
| 7 | Voter-only leadership avoidance: transfer-on-win vs. Raft fork? | Phase 7.2, 7.4 | **Resolved** — transfer-on-win. Accepts brief leadership window, avoids forking Raft. |

---

## Files Reference

All changes stay within allowed scope (`internal/cluster/**`, `test/e2e/`, `docs/`, `conf/`).

| File | Phases |
|------|--------|
| `controller/controller.go` | 1.1, 1.2, 2.5, 4.1, 4.2, 4.4, 4.5, 6.4, 7.3, 7.5, 7.6 |
| `server/server.go` | 1.1, 2.4, 4.2, 4.5, 6.2 |
| `store/store.go` | 2.3, 4.3, 7.1, 7.2, 7.6 |
| `store/fsm.go` | 2.2, 7.1 |
| `command/proto/zencommand.proto` | 2.1, 7.1 |
| `state/state.go` | 3.2, 7.1 |
| `partition/partition.go` | 7.4 |
| `node.go` | 3.3, 6.3 |
| `jobmanager/manager.go` | 5.1 |
| `jobmanager/client.go` | 5.2, 5.4 |
| `jobmanager/server.go` | 5.3 |
| `conf/` | 7.1 |
