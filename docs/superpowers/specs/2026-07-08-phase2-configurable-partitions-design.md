# Phase 2: Configurable Partitions — Design

> Date: 2026-07-08
> Status: Approved (pending user review of this document)
> Supersedes: Phase 2 section of `docs/cluster-implementation-plan.md` where they conflict (plan will be synced on implementation commits)

## Goal

Make `DesiredPartitions` changeable at runtime through Raft consensus instead of hardcoded to 1, with:

- initial value seeded from app configuration on fresh bootstrap only,
- throttled, level-triggered partition creation that actually forms working Raft groups (full replication),
- a `ConfigurationUpdate` gRPC endpoint with increase-only validation,
- message-subscription-pointer consistency across partition-count changes (rebuild-on-scale-up).

## Decisions (locked during brainstorming)

| # | Question | Decision |
|---|----------|----------|
| D1 | Where to seed initial `DesiredPartitions` | Controller leader reconcile, only when applied value is 0/unset |
| D2 | Convergence / stall safety | Leader-gated periodic reconcile ticker; one partition created per pass (prior-art: K8s resync, TiKV PD, MongoDB balancer) |
| D3 | RPC routing on follower | Forward to leader via existing `ClusterLeader()` client |
| D4 | Validation | Increase-only, `1 ≤ n ≤ 122` (mux single-byte partition-id limit) |
| D5 | Replication scope in Phase 2 | Full replication: every partition gets **all** started nodes as members (RF = cluster size); RF<N placement stays Phase 4 |
| D6 | Pointer staleness on scale-up | Keep `hash % N` addressing; rebuild pointers after formation using existing `RebuildMessageSubscriptionPointers` machinery, tracked by a replicated `RoutingPartitions` marker. No broadcast lookup. |
| D7 | App-config scope exception | `DesiredPartitions` field added to `internal/config/config.go` (user-approved exception to cluster-scope boundary) |
| D8 | Operator-facing API | REST admin endpoint in `openapi/system.yaml` following the backup/restore precedent; cluster gRPC `ConfigurationUpdate` remains the internal transport (user-approved scope exception for `internal/rest/` + `openapi/system.yaml`) |

## Non-goals (explicit boundaries)

- **Scale-down** — rejected by validation; no data-migration path exists (plan Risk #4).
- **Replicating to nodes that join after a partition was created** — Phase 4 node lifecycle (add-node-to-partition RPCs are stubs today).
- **RF < cluster-size placement / balancing** — Phase 4.1.
- **Public workload gRPC (:9090) exposure** — scale-up is a cluster admin operation; the REST admin API (`system.yaml`) is the operator surface (D8), not the workload API.
- **Voter-only awareness** — Phase 7.

## Design

### 1. Protobuf and configuration changes

**Command proto** (`internal/cluster/command/proto/zencommand.proto`):

```protobuf
message Command {
  enum Type {
    ...
    TYPE_CONFIGURATION_CHANGE = 5;  // 4 is taken by TYPE_CLUSTER_MAINTENANCE_CHANGE
  }
  oneof request {
    ...
    ConfigurationChange configuration_change = 5;
  }
}

// ConfigurationChange updates replicated cluster configuration.
// Zero-valued fields are treated as "not set" and left unchanged by the FSM,
// so one command type serves seeding, RPC updates, and rebuild completion.
message ConfigurationChange {
  uint32 desired_partitions = 1;
  uint32 routing_partitions = 2;  // the N that message pointers are currently hashed by
}
```

**gRPC service proto** (`internal/cluster/proto/zen_cluster.proto`) — fill the empty stubs:

```protobuf
message ConfigurationUpdateRequest  { uint32 desired_partitions = 1; }
message ConfigurationUpdateResponse { uint32 desired_partitions = 1; }  // echoes applied value
```

**App config** (`internal/config/config.go`, scope exception D7):

- `DesiredPartitions uint32` on `config.Cluster`, env `CLUSTER_DESIRED_PARTITIONS`, env-default `1`.
- `validate()` rejects an explicit `0` and values above 122.

**State** (`internal/cluster/state/state.go`):

- `ClusterConfig` gains `RoutingPartitions uint32 \`json:"routingPartitions"\``. JSON-missing decodes to 0 — backward compatible with existing snapshots (see §6 upgrade path).

Two `make generate` runs (one per proto tree).

### 2. FSM and store

- `fsm.go`: `applyConfigurationChange` (clone of `applyMaintenanceChange`): deep-copy state, set `Config.DesiredPartitions` and/or `Config.RoutingPartitions` for non-zero fields, swap state. New `Apply` switch case.
- `store.go`:
  - Remove hardcoded `DesiredPartitions: 1` from `New()` — initial in-memory value becomes `0` = "unset".
  - Store `Config` gains `DesiredPartitions uint32`, seeded from `config.Cluster` in `DefaultConfig()` (transport for the app-config value; the store does not act on it).
  - `WriteConfigurationChange(change *proto.ConfigurationChange) error` — clone of `WriteMaintenanceChange`.
- Controller `ControlledStore` interface gains `WriteConfigurationChange`.

### 3. Seeding (D1)

In `performLeaderOperations`, before partition reconciliation:

```
if cs.Config.DesiredPartitions == 0 {
    write ConfigurationChange{ desired_partitions: configuredValue }
    return  // next reconcile pass acts on the applied value
}
```

- **Restart-safety invariant:** a runtime RPC change persists in the Raft log/snapshot, so after restart the applied value is > 0 and app-config never clobbers it. The `== 0` guard is the entire mechanism.
- Leader-only; if nodes' app configs disagree, the current leader's value wins (documented).
- Concurrent duplicate seeds (edge notification + ticker) write the same value — idempotent; single-flight (§4, R3) makes them rare.

### 4. Convergence: reconcile ticker + full-replication membership (D2, D5)

**Membership on creation** — replaces both the single-node `assignNewPartition` placement and the hardcoded "0-partition nodes → partition 1" loop in `performLeaderOperations`:

```
started  = nodes with State == STARTED
if len(started) < BootstrapExpect: defer (log, retry next pass)     // R2
current  = len(cs.Partitions)                                       // partitions with elected leaders
if cs.Config.DesiredPartitions > current:
    next = current + 1
    if !cs.AnyNodeHasPartition(next):
        for node in started: assignPartition(next, node.Id)          // full replication
```

- **One partition per pass** (throttle, unchanged in spirit from today): `cs.Partitions[next]` only appears once partition `next` elects a leader (`FsmApplyPartitionChange` writes it on `Role=LEADER`), so partition N+2 never starts forming before N+1 has a leader. Matches the industry pattern of capped in-flight topology operations.
- **Full replication**: every started node is assigned as a member, so each partition's rqlite group reuses the existing `BootstrapExpect` coordination exactly as partition 1 does today. No partition-layer changes.
- **R2 degraded-cluster guard**: partitions inherit the global `BootstrapExpect` (`controller.go:81` → `partition.go:382`); creating a partition while a node is down would leave the group below bootstrap-expect and wedge it for the 120 s timeout. The guard defers creation until all expected nodes are healthy; the ticker retries.
- The now-obsolete "assign node with zero partitions to partition 1" loop is deleted; the general rule (every started node joins every partition at creation) subsumes it for Phase 2 topologies. Nodes joining an already-formed cluster receive partitions in Phase 4.

**Reconcile ticker** (stall safety, D2):

- Started in `Controller.Start()` via `safego` on `c.ctx`; stopped by context cancellation in `Stop()`.
- Every `ReconcileInterval` (new controller config knob, default 2 s): invoke the **full** `ClusterStateChangeNotification` path — leader ops when leader, member ops always. Member handlers are re-entrant-safe since the Phase 1.3 fix; level-triggering them guards follower-side join/init stalls too, not just leader-side creation stalls.
- **R3 single-flight**: an atomic in-progress flag around `ClusterStateChangeNotification`'s body; concurrent invocations (per-apply FSM goroutines + ticker) coalesce instead of stacking. Prevents duplicate reconcile passes racing.

### 5. `ConfigurationUpdate` RPC (D3, D4)

`server.go` (currently returns `codes.Unimplemented`):

1. **Validate** (R1 — against **`Config.DesiredPartitions`**, never against formed-partition count, which lags desired during scale-up):
   - `n < 1` or `n > 122` → `InvalidArgument` (122 = mux single-byte partition-id limit).
   - `n < desired` → `FailedPrecondition` ("scale-down not supported").
   - `n == desired` → idempotent success, echo current value.
2. **Route**: if `IsLeader()` → `store.WriteConfigurationChange({desired_partitions: n})`; else forward the same request via `client.ClusterLeader()`. Recursion terminates at the leader; leadership churn mid-call is bounded by the request context deadline and surfaces as `NotLeader`/`Unavailable` (R7).
3. Respond with the applied value.

Reconciliation then converges via §4; pointer rebuild via §6.

### 5b. REST admin endpoint (D8)

Operator-facing surface, following the backup/restore precedent (`system.yaml` → REST handler → `ZenNode`):

- `openapi/system.yaml`: `PUT /cluster/config`, operationId `updateClusterConfiguration`, request body `{ "desiredPartitions": <uint32> }`, 200 response echoes the applied cluster config.
- `internal/rest/` handler (new `cluster_config.go`, modeled on `cluster_backup.go`): parse/decode → `s.node.UpdateClusterConfiguration(ctx, n)` → map errors: `InvalidArgument` → 400, `FailedPrecondition` (scale-down) → 409, no leader/unavailable → 503.
- `internal/cluster/node.go`: `UpdateClusterConfiguration(ctx, n)` obtains `client.ClusterLeader()` and calls the `ConfigurationUpdate` gRPC — **all validation stays in the server RPC implementation** (single source of truth); the REST layer only translates transport and status codes. Works identically whether the receiving node is leader or follower.

### 6. Pointer consistency across scale-up (D6)

**Background (verified against code):** message-subscription pointers are a rebuildable routing index. Save (`partition_persistence.go:1766`) and lookup (`node.go:779`) both route by `GetPartitionIdForMessageSubscriptionPointer` = `hash % len(Partitions) + 1`, so addressing is exact at any stable N. Authoritative subscriptions never move — their zenflake keys encode their home partition (`node.go:829`). The only inconsistency is pointers written under a previous N being mis-addressed after N changes; misses degrade gracefully (definition-level fallback / NotFound, `node.go:790-802`). No data loss is possible.

**Mechanism** — leader reconcile, after formation completes:

```
if len(cs.Partitions) == cs.Config.DesiredPartitions
   && cs.Config.RoutingPartitions != uint32(len(cs.Partitions)):
    pointers = collect from all partition leaders            // existing PlanPointerRebuild input path
    if pointers empty:                                       // R4: fresh formation / upgrade no-op
        write ConfigurationChange{ routing_partitions: N }   // no gate
    else:
        set the existing Restoring flag (WriteMaintenanceChange), exactly as restore does
        re-plan against N (PlanPointerRebuild) and
        RebuildMessageSubscriptionPointers on each partition leader
        write ConfigurationChange{ routing_partitions: N }
        clear the Restoring flag
```

- **Why the full `Restoring` flag and not a narrower gate**: the flag stops engines and rejects
  client operations, which quiesces subscription creation for the duration of the rebuild.
  Collecting pointers while engines keep writing new ones would lose the concurrent writes in
  the wipe-and-reinsert. Restore already exercises exactly this stop→work→start cycle
  (`TestRestoringFlagStopsEngines`), so no new gating mechanism is introduced. The zero-pointer
  fast path means fresh clusters never pay this cost; scale-up with live data is a rare,
  admin-initiated maintenance event.

- **Ordering guard**: fires only when `len(Partitions) == DesiredPartitions` — never against a half-formed topology.
- **Crash safety**: "rebuild pending" is derived from replicated state (`RoutingPartitions != len(Partitions)`), not node memory; a new leader after failover re-detects and re-runs. Rebuild is wipe-and-reinsert, hence idempotent.
- **R4**: the zero-pointer fast path means fresh bootstraps and upgrades never see a maintenance gate — no e2e `WaitForHealthy` flake.
- **R5 upgrade path self-heals**: an existing deployment's snapshot has `DesiredPartitions=1`, `RoutingPartitions` absent → 0; first leader reconcile fires the rule at N=1 (content-identical re-plan, gate-skipped when no pointers) and writes `RoutingPartitions=1`. No migration step.
- **R6 formation window**: during scale-up, save and lookup agree with each other at every instant (both use current `len`); only pointers written under a previous N miss until the rebuild completes. Accepted and documented; the gate covers only the rebuild step.
- **R7 sequential changes compose**: a second increase mid-formation just raises `DesiredPartitions`; the rebuild waits until formation reaches the final N and runs once.

**Phase 3.3 consequence:** 3.3's premise ("subscription lives on the partition where the instance was created; lookup can't find it") is stale — the pointer-indirection mechanism already provides exact addressing at stable N, and this section closes the scale-up case. The implementation plan will mark 3.3 resolved-by-Phase-2 pending verification that `TestMessageCorrelationAcrossNodes` passes un-skipped.

### 7. Bundled fix

`state.LeastStressedPartition` (`state.go:103`): `rand.Intn(len(c.Partitions) - 1)` → `rand.Intn(len(c.Partitions))`. Panics today when `len == 1` (`rand.Intn(0)`) and can never select the last map-iteration slot otherwise. Listed as Phase 3.2; becomes reachable-by-default with multi-partition routing, so it ships here.

### 8. Testing (TDD — tests written first per task)

**Unit:**

| Area | Tests |
|------|-------|
| store | `TestWriteConfigurationChange`, `TestConfigNotReseededAfterRuntimeChange` |
| fsm | `TestApplyConfigurationChange` (desired only, routing only, both, zero-field no-op) |
| controller | `TestConfigSeedOnlyWhenUnset`, `TestPartitionScaleUpOnePerPass`, `TestScaleUpAssignsAllStartedNodes`, `TestScaleUpDeferredWhenNodeDown` (R2), `TestReconcileTickerDrivesConvergence`, `TestReconcileSingleFlight` (R3), `TestPointerRebuildTriggeredAfterScaleUp`, `TestPointerRebuildWaitsForFormation`, `TestFreshFormationSkipsRebuildGate` (R4), `TestPointerRebuildPendingSurvivesLeaderChange` |
| server | `TestConfigurationUpdateForwardsToLeader`, `TestConfigUpdateValidatesAgainstDesired` (R1), `TestConfigurationUpdateRejectsDecrease`, `TestConfigurationUpdateRejectsOutOfRange`, `TestConfigurationUpdateIdempotentSameValue` |
| rest | `TestUpdateClusterConfigurationEndpoint` (status-code mapping: 200/400/409/503) |
| state | `TestLeastStressedPartitionSinglePartition` |

**E2E** (`test/e2e/cluster/`):

- Wire the currently-dead `WithPartitions` harness option into per-node `config.Cluster.DesiredPartitions`.
- Un-skip: `TestPartitionCreation`, `TestPartitionAssignment`, `TestMultiplePartitionsPerNode`, `TestMaxPartitions` (slow tier), `TestIncreasePartitionCount` (implement its TODO via the REST admin endpoint, §5b), multi-partition-gated tests in `data_test.go`, `stream_resilience_test.go`, `backup_restore_test.go`.
- New: `TestMessageCorrelationAfterScaleUp` (deploy + subscribe at N=1, scale to 3, publish after rebuild → correlates).
- Verify `TestMessageCorrelationAcrossNodes` (listed under 3.3) passes; if so, un-skip and mark 3.3 resolved.

### 9. Documentation sync (same commits, per project rule)

`docs/cluster-implementation-plan.md`:

- Phase 2 section rewritten to match this design (enum = 5, second proto file, seeding location, full-replication decision, ticker, pointer rebuild).
- Phase 3.3 marked resolved-by-Phase-2 (stale premise documented) once its e2e verification passes.
- Phase 3.2 checked off (bundled here).
- Panic Tracker: `ConfigurationUpdate`, `AssignPartition`, `UnassignPartition` rows corrected — they return `codes.Unimplemented`, they do not panic; stale line numbers refreshed.
- Risk #5 (creation throttling) marked resolved (one-per-pass + ticker); dependency note updated (Phase 2 no longer strictly precedes 3.3).
- `> Last updated:` bumped.

## Files touched

| File | Change |
|------|--------|
| `internal/cluster/command/proto/zencommand.proto` | `TYPE_CONFIGURATION_CHANGE = 5`, `ConfigurationChange` message |
| `internal/cluster/proto/zen_cluster.proto` | `ConfigurationUpdateRequest/Response` fields |
| `internal/config/config.go` | `DesiredPartitions` + validation (scope exception D7) |
| `internal/cluster/state/state.go` | `RoutingPartitions` field; `LeastStressedPartition` fix |
| `internal/cluster/store/fsm.go` | `applyConfigurationChange` |
| `internal/cluster/store/store.go` | remove hardcode; config plumb; `WriteConfigurationChange` |
| `internal/cluster/controller/controller.go` | seeding, membership rewrite, ticker, single-flight, rebuild trigger |
| `internal/cluster/server/server.go` | `ConfigurationUpdate` implementation |
| `internal/cluster/node.go` | `UpdateClusterConfiguration` method (REST → leader RPC) |
| `openapi/system.yaml` | `PUT /cluster/config` admin operation (scope exception D8) |
| `internal/rest/cluster_config.go` | REST handler + status-code mapping (scope exception D8) |
| `test/e2e/cluster/*` | harness wiring, un-skips, new tests |
| `docs/cluster-implementation-plan.md` | sync per §9 |

## Risks

| Risk | Mitigation |
|------|-----------|
| Ticker reintroduces formation churn (Phase 1.3 class) | One-partition-per-pass throttle unchanged; single-flight; re-entrant-safe handlers (1.3 fix); interval tunable; e2e suite ×2 back-to-back as regression gate |
| Rebuild gate blocks traffic unexpectedly | Gate only during rebuild step; zero-pointer fast path skips it entirely; scale-up is a rare admin op |
| Leader app-config divergence seeds "wrong" value | Documented leader-wins semantics; value is correctable at runtime via RPC |
| 122-partition formation time in `TestMaxPartitions` | Slow tier only; sequential formation is by design (throttle) |
