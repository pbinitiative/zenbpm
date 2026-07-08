# Phase 2: Configurable Partitions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `DesiredPartitions` changeable at runtime through Raft consensus, seeded from app config on fresh bootstrap, with full-replication partition creation, an operator REST endpoint, and message-pointer rebuild on scale-up.

**Architecture:** A new `ConfigurationChange` Raft command carries `desired_partitions`/`routing_partitions`. The controller's leader reconcile seeds the config when unset, creates partitions one-per-pass with all started nodes as members, and rebuilds message-subscription pointers after formation (tracked by the replicated `RoutingPartitions` marker). A `ConfigurationUpdate` gRPC RPC (validate-on-leader, forward-from-follower) is the internal transport; a `PUT /system/v1/cluster/config` REST endpoint is the operator surface. A leader-gated reconcile ticker makes convergence level-triggered.

**Tech Stack:** Go, HashiCorp Raft, protobuf (edition 2023, `ptr.To` for presence), rqlite partitions, chi REST router, testify.

**Spec:** `docs/superpowers/specs/2026-07-08-phase2-configurable-partitions-design.md` (decisions D1–D8, hardenings R1–R8).

## Global Constraints

- Conventional Commits, **single-line**, `(cluster)` scope where applicable. **Never** add a `Co-Authored-By: Claude` trailer or any AI mention.
- TDD: write the failing test before the implementation, run it red, implement, run it green.
- Never hand-edit generated files (`*.pb.go`, `zz_generated.deepcopy.go`); run `make generate` after proto/state changes.
- Doc-sync rule: any commit that changes something `docs/cluster-implementation-plan.md` tracks MUST update that file in the same commit and bump its `> Last updated:` line. Task steps call this out explicitly.
- Allowed scope: `internal/cluster/**`, `test/e2e/`, `docs/`, `conf/`, **plus user-approved exceptions**: `internal/config/config.go` (D7), `openapi/system.yaml` + `internal/rest/` (D8), `internal/cluster/node.go` (already cluster scope).
- Deviation from spec §2 (noted during planning): the spec plumbs `DesiredPartitions` into the store `Config`; nothing consumes it there — the controller reads its own `config.Cluster`. YAGNI: skip the store-Config plumb.
- After each task: `go build ./...` must pass. Fast e2e tier (`make test-e2e-cluster`) is run at the milestones marked below (it takes ~3 min).
- Partition-id upper bound is **122** (network mux single-byte header constraint; see `TestMaxPartitions`).

---

### Task 1: Command proto + state field (codegen only)

**Files:**
- Modify: `internal/cluster/command/proto/zencommand.proto`
- Modify: `internal/cluster/state/state.go` (ClusterConfig)
- Generated: `internal/cluster/command/proto/zencommand.pb.go`, `internal/cluster/state/zz_generated.deepcopy.go`

**Interfaces:**
- Produces: `proto.Command_TYPE_CONFIGURATION_CHANGE`, `proto.ConfigurationChange{DesiredPartitions, RoutingPartitions uint32}`, `proto.Command_ConfigurationChange` oneof wrapper, `state.ClusterConfig.RoutingPartitions uint32`.

- [ ] **Step 1: Edit `zencommand.proto`** — add to the `Command` enum/oneof (slots `4` are taken by maintenance change; use `5`) and append the message:

```protobuf
message Command {
  enum Type {
    TYPE_UNKNOWN = 0;
    TYPE_NOOP = 1;
    TYPE_NODE_CHANGE = 2;
    TYPE_NODE_PARTITION_CHANGE = 3;
    TYPE_CLUSTER_MAINTENANCE_CHANGE = 4;
    TYPE_CONFIGURATION_CHANGE = 5;
  }
  Type type = 1;
  oneof request {
    NodeChange node_change = 2;
    NodePartitionChange node_partition_change = 3;
    ClusterMaintenanceChange cluster_maintenance_change = 4;
    ConfigurationChange configuration_change = 5;
  }
}
```

and at the end of the file:

```protobuf
// ConfigurationChange updates replicated cluster configuration.
// Zero-valued fields are treated as "not set" and left unchanged by the FSM,
// so one command type serves seeding, RPC updates, and rebuild completion.
message ConfigurationChange {
  uint32 desired_partitions = 1;
  uint32 routing_partitions = 2; // the N that message pointers are currently hashed by
}
```

- [ ] **Step 2: Edit `internal/cluster/state/state.go`** — extend `ClusterConfig`:

```go
// +k8s:deepcopy-gen=true
type ClusterConfig struct {
	DesiredPartitions uint32 `json:"desiredPartitions"`
	// RoutingPartitions is the partition count that message-subscription
	// pointers are currently hashed by. When it differs from the number of
	// formed partitions, a pointer rebuild is pending (see controller).
	// JSON-missing decodes to 0, keeping old snapshots compatible.
	RoutingPartitions uint32 `json:"routingPartitions"`
}
```

- [ ] **Step 3: Run codegen and build**

Run: `make generate && go build ./...`
Expected: success; `zencommand.pb.go` and `zz_generated.deepcopy.go` regenerate. Do not hand-edit them.

- [ ] **Step 4: Commit**

```bash
git add internal/cluster/command/proto/ internal/cluster/state/
git commit -m "feat(cluster): add ConfigurationChange raft command and RoutingPartitions state field"
```

---

### Task 2: FSM handler + `store.WriteConfigurationChange`

**Files:**
- Modify: `internal/cluster/store/fsm.go`
- Modify: `internal/cluster/store/store.go`
- Test: `internal/cluster/store/store_test.go`

**Interfaces:**
- Consumes: Task 1 proto types.
- Produces: `func (s *Store) WriteConfigurationChange(change *proto.ConfigurationChange) error`; FSM applies `TYPE_CONFIGURATION_CHANGE` into `state.Config`.

- [ ] **Step 1: Write the failing test** — append to `internal/cluster/store/store_test.go`, modeled exactly on `TestWriteMaintenanceChange` (same file, line ~544):

```go
func TestWriteConfigurationChange(t *testing.T) {
	c := config.Cluster{
		Raft:   config.ClusterRaft{Dir: t.TempDir()},
		NodeId: random.String(),
	}
	s, ln := newMustTestStore(t, c)
	defer s.Close(true)
	defer ln.Close()
	if err := s.Open(); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}
	if err := s.Bootstrap(&state.Node{
		Id:         s.raftID,
		Addr:       s.Addr(),
		Partitions: map[uint32]state.NodePartition{},
	}); err != nil {
		t.Fatalf("failed to bootstrap single-node store: %s", err)
	}
	if _, err := s.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("failed to wait for leader: %s", err)
	}

	// Set DesiredPartitions only; RoutingPartitions must stay unchanged (zero-field = not set).
	if err := s.WriteConfigurationChange(&proto.ConfigurationChange{DesiredPartitions: ptr.To(uint32(3))}); err != nil {
		t.Fatalf("WriteConfigurationChange(desired=3) returned error: %s", err)
	}
	testPoll(t, func() bool {
		cs := s.ClusterState()
		return cs.Config.DesiredPartitions == 3 && cs.Config.RoutingPartitions == 0
	}, 50*time.Millisecond, 5*time.Second)

	// Set RoutingPartitions only; DesiredPartitions must stay 3.
	if err := s.WriteConfigurationChange(&proto.ConfigurationChange{RoutingPartitions: ptr.To(uint32(3))}); err != nil {
		t.Fatalf("WriteConfigurationChange(routing=3) returned error: %s", err)
	}
	testPoll(t, func() bool {
		cs := s.ClusterState()
		return cs.Config.DesiredPartitions == 3 && cs.Config.RoutingPartitions == 3
	}, 50*time.Millisecond, 5*time.Second)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/cluster/store/... -run TestWriteConfigurationChange -v`
Expected: FAIL — `s.WriteConfigurationChange undefined`.

- [ ] **Step 3: Implement.** In `internal/cluster/store/store.go`, add below `WriteMaintenanceChange` (line ~195):

```go
// WriteConfigurationChange replicates a cluster configuration change
// (desired/routing partition counts) through the raft log. Zero-valued
// fields are left unchanged by the FSM.
func (s *Store) WriteConfigurationChange(change *proto.ConfigurationChange) error {
	command := &proto.Command{
		Type: proto.Command_TYPE_CONFIGURATION_CHANGE.Enum(),
		Request: &proto.Command_ConfigurationChange{
			ConfigurationChange: change,
		},
	}
	b, err := pb.Marshal(command)
	if err != nil {
		return fmt.Errorf("failed to marshal ConfigurationChange message before applying to log: %w", err)
	}
	f := s.raft.Apply(b, s.cfg.RaftTimeout)
	if f.Error() != nil && f.Response() != nil {
		return fmt.Errorf("failed to apply ConfigurationChange message to raft log: %w", f.Error())
	}
	return nil
}
```

In `internal/cluster/store/fsm.go`, add the switch case in `Apply` (after the maintenance case, line ~58):

```go
	case proto.Command_TYPE_CONFIGURATION_CHANGE:
		res = f.applyConfigurationChange(command.GetConfigurationChange())
```

and the handler below `applyMaintenanceChange` (line ~129):

```go
func (f *FSM) applyConfigurationChange(cmd *proto.ConfigurationChange) interface{} {
	f.store.stateMu.Lock()
	defer f.store.stateMu.Unlock()
	newState := *f.store.state.DeepCopy()
	if cmd.GetDesiredPartitions() != 0 {
		newState.Config.DesiredPartitions = cmd.GetDesiredPartitions()
	}
	if cmd.GetRoutingPartitions() != 0 {
		newState.Config.RoutingPartitions = cmd.GetRoutingPartitions()
	}
	f.store.state = newState
	return nil
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/cluster/store/... -v`
Expected: `TestWriteConfigurationChange` PASS, all existing store tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/cluster/store/
git commit -m "feat(cluster): apply ConfigurationChange commands through the FSM"
```

---

### Task 3: App config fields + validation

**Files:**
- Modify: `internal/config/config.go` (scope exception D7)
- Test: `internal/config/config_test.go` (create if absent, package `config`)

**Interfaces:**
- Produces: `config.Cluster.DesiredPartitions uint32` (env `CLUSTER_DESIRED_PARTITIONS`, default 1), `config.Cluster.ReconcileInterval time.Duration` (env `CLUSTER_RECONCILE_INTERVAL`, default 2s); `validate()` rejects `DesiredPartitions == 0` or `> 122`.

- [ ] **Step 1: Write the failing test** in `internal/config/config_test.go`:

```go
func TestValidateDesiredPartitionsBounds(t *testing.T) {
	base := func() Config {
		return Config{Cluster: Cluster{
			NodeId: "n1", Addr: "localhost:8090", Adv: "localhost:8090",
			Raft: ClusterRaft{Dir: t.TempDir(), BootstrapExpect: 1},
		}}
	}
	c := base()
	c.Cluster.DesiredPartitions = 0
	assert.Error(t, c.validate(), "0 desired partitions must be rejected")

	c = base()
	c.Cluster.DesiredPartitions = 123
	assert.Error(t, c.validate(), "counts above 122 must be rejected (mux byte limit)")

	c = base()
	c.Cluster.DesiredPartitions = 3
	assert.NoError(t, c.validate())
}
```

Add imports `testing`, `github.com/stretchr/testify/assert` as needed.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config/... -run TestValidateDesiredPartitionsBounds -v`
Expected: FAIL — `c.Cluster.DesiredPartitions undefined`.

- [ ] **Step 3: Implement.** In the `Cluster` struct (`config.go:34`), after `Script`:

```go
	// Number of engine data partitions the cluster should run. Applied once on
	// fresh bootstrap; changed at runtime via the cluster configuration API.
	DesiredPartitions uint32 `yaml:"desiredPartitions" json:"desiredPartitions" env:"CLUSTER_DESIRED_PARTITIONS" env-default:"1"`
	// How often the controller re-runs its reconcile loop as a level-triggered
	// safety net (partition creation, config seeding, pointer rebuild).
	ReconcileInterval time.Duration `yaml:"reconcileInterval" json:"reconcileInterval" env:"CLUSTER_RECONCILE_INTERVAL" env-default:"2s"`
```

In `validate()` (after the bootstrap-policy checks, ~line 150):

```go
	if c.Cluster.DesiredPartitions == 0 {
		return errors.New("cluster.desiredPartitions must be at least 1")
	}
	if c.Cluster.DesiredPartitions > 122 {
		return errors.New("cluster.desiredPartitions must be at most 122 (network mux partition-id limit)")
	}
```

**Caution:** many callers build `config.Cluster` as struct literals (harness comment at `test/e2e/cluster/harness.go:298`: env-defaults are skipped) — those get `DesiredPartitions: 0`. `validate()` only runs for file/env-loaded configs, and the controller seeding (Task 4) treats a zero *configured* value as 1. This keeps struct-literal users working.

- [ ] **Step 4: Run tests**

Run: `go test ./internal/config/... -v && go build ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/config/
git commit -m "feat(cluster): add DesiredPartitions and ReconcileInterval to cluster config"
```

---

### Task 4: Controller seeding + remove store hardcode (MILESTONE: fast e2e)

**Files:**
- Modify: `internal/cluster/controller/controller.go`
- Modify: `internal/cluster/store/store.go:124-130`
- Modify: `internal/cluster/controller/controller_test.go` (fake store + new tests)
- Modify: `docs/cluster-implementation-plan.md` (same commit)

**Interfaces:**
- Consumes: `Store.WriteConfigurationChange` (Task 2), `config.Cluster.DesiredPartitions` (Task 3).
- Produces: `ControlledStore` interface gains `WriteConfigurationChange(change *proto.ConfigurationChange) error`; `performLeaderOperations` seeds config when `cs.Config.DesiredPartitions == 0` and returns.

- [ ] **Step 1: Write the failing tests** in `controller_test.go`. First extend the fake — add to `ControllerTestStore` (it already implements `ControlledStore`; tests are in-package so unexported fields are accessible):

```go
// configWrites records ConfigurationChange writes and applies them to the
// fake state the way the real FSM would (zero field = leave unchanged).
func (c *ControllerTestStore) WriteConfigurationChange(change *proto.ConfigurationChange) error {
	c.configWrites = append(c.configWrites, change)
	if change.GetDesiredPartitions() != 0 {
		c.clusterState.Config.DesiredPartitions = change.GetDesiredPartitions()
	}
	if change.GetRoutingPartitions() != 0 {
		c.clusterState.Config.RoutingPartitions = change.GetRoutingPartitions()
	}
	return nil
}
```

with the struct gaining `configWrites []*proto.ConfigurationChange`. Then the tests (no mux needed — set unexported fields directly and call `performLeaderOperations`):

```go
func TestConfigSeedOnlyWhenUnset(t *testing.T) {
	tStore := &ControllerTestStore{
		id: "n1", leader: true,
		clusterState: state.Cluster{
			Config:     state.ClusterConfig{DesiredPartitions: 0},
			Partitions: map[uint32]state.Partition{},
			Nodes:      map[string]state.Node{},
		},
	}
	c, err := NewController(nil, config.Cluster{NodeId: "n1", DesiredPartitions: 2})
	assert.NoError(t, err)
	c.store = tStore

	c.performLeaderOperations(t.Context())

	assert.Len(t, tStore.configWrites, 1, "leader must seed config exactly once")
	assert.Equal(t, uint32(2), tStore.configWrites[0].GetDesiredPartitions())
	assert.Equal(t, uint32(2), tStore.clusterState.Config.DesiredPartitions)

	// second pass: value now set — no reseed
	c.performLeaderOperations(t.Context())
	assert.Len(t, tStore.configWrites, 1, "seed must not repeat once value is set")
}

func TestConfigNotReseededAfterRuntimeChange(t *testing.T) {
	// Simulates restart after a runtime RPC change: applied value (3) differs
	// from app config (1). App config must NOT clobber it.
	tStore := &ControllerTestStore{
		id: "n1", leader: true,
		clusterState: state.Cluster{
			Config:     state.ClusterConfig{DesiredPartitions: 3},
			Partitions: map[uint32]state.Partition{},
			Nodes:      map[string]state.Node{},
		},
	}
	c, err := NewController(nil, config.Cluster{NodeId: "n1", DesiredPartitions: 1})
	assert.NoError(t, err)
	c.store = tStore

	c.performLeaderOperations(t.Context())
	assert.Empty(t, tStore.configWrites, "applied config must win over app config")
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/cluster/controller/... -run 'TestConfigSeed|TestConfigNotReseeded' -v`
Expected: FAIL — `WriteConfigurationChange` missing from `ControlledStore` usage / no seeding happens (first test asserts 1 write, gets 0).

- [ ] **Step 3: Implement.** In `controller.go`:

(a) extend the interface (line ~64):

```go
type ControlledStore interface {
	ID() string
	Addr() string
	IsLeader() bool
	Role() proto.Role
	ClusterState() state.Cluster
	WritePartitionChange(change *proto.NodePartitionChange) error
	WriteConfigurationChange(change *proto.ConfigurationChange) error
}
```

(b) seed at the top of `performLeaderOperations` (line ~124, right after `cs := c.store.ClusterState()`):

```go
	// Seed replicated config from app config on fresh bootstrap only. A runtime
	// RPC change persists in the raft log/snapshot, so after restart the applied
	// value is >0 and app config never clobbers it (the ==0 guard is the entire
	// restart-safety mechanism). The current leader's app config wins.
	if cs.Config.DesiredPartitions == 0 {
		desired := c.Config.DesiredPartitions
		if desired == 0 {
			desired = 1 // struct-literal configs skip env-defaults
		}
		if err := c.store.WriteConfigurationChange(&proto.ConfigurationChange{
			DesiredPartitions: ptr.To(desired),
		}); err != nil {
			c.logger.Error(fmt.Sprintf("failed to seed cluster configuration: %s", err))
		}
		return // act on the applied value on the next pass
	}
```

(c) remove the hardcode in `store.go` `New()` (line ~124-130):

```go
		state: state.Cluster{
			Config:     state.ClusterConfig{},
			Partitions: map[uint32]state.Partition{},
			Nodes:      map[string]state.Node{},
		},
```

- [ ] **Step 4: Run tests — including for hardcode fallout**

Run: `go test ./internal/cluster/controller/... ./internal/cluster/store/... -v`
Expected: new tests PASS. `TestControllerCanStartNewPartitions` still passes (its fake starts at `DesiredPartitions: 1`, so no seed path). If any store test asserted `DesiredPartitions == 1` on a fresh store, update it to expect `0` and note the seeding now owns initialization.

- [ ] **Step 5: MILESTONE — fast e2e tier (formation now depends on seeding)**

Run: `make test-e2e-cluster`
Expected: PASS (single-partition formation works through seed → reconcile).

- [ ] **Step 6: Doc-sync + commit.** In `docs/cluster-implementation-plan.md`: check off 2.2 and 2.3 items (FSM handler, hardcode removal, initial config, `WriteConfigurationChange`), correct 2.1 text (enum is `5`, not `4`), note tasks 2.1–2.3 done, bump `> Last updated:`.

```bash
git add internal/cluster/ docs/cluster-implementation-plan.md
git commit -m "feat(cluster): seed DesiredPartitions from app config on fresh bootstrap"
```

---

### Task 5: Reconcile single-flight + leader ticker

**Files:**
- Modify: `internal/cluster/controller/controller.go`
- Test: `internal/cluster/controller/controller_test.go`

**Interfaces:**
- Consumes: `config.Cluster.ReconcileInterval` (Task 3).
- Produces: `Controller.reconcileInFlight atomic.Bool`; ticker goroutine started in `Start()`, stopped by ctx cancel. `ClusterStateChangeNotification` coalesces concurrent callers (drop, not queue — the ticker is the level-triggered backstop for dropped edges).

- [ ] **Step 1: Write the failing tests**:

```go
func TestReconcileSingleFlight(t *testing.T) {
	tStore := &ControllerTestStore{
		id: "n1", leader: false, // follower: skips leader ops, still runs hooks
		clusterState: state.Cluster{
			Config:     state.ClusterConfig{DesiredPartitions: 1},
			Partitions: map[uint32]state.Partition{},
			Nodes:      map[string]state.Node{},
		},
	}
	c, err := NewController(nil, config.Cluster{NodeId: "n1"})
	assert.NoError(t, err)
	c.store = tStore
	c.handleClusterChanges = true

	entered := make(chan struct{})
	release := make(chan struct{})
	var runs atomic.Int32
	c.AddClusterStateChangeHook(func(ctx context.Context) {
		runs.Add(1)
		entered <- struct{}{}
		<-release
	})

	go c.ClusterStateChangeNotification(t.Context())
	<-entered // first call is inside the hook, holding the flight slot

	// second call must be dropped, not queued
	c.ClusterStateChangeNotification(t.Context())
	close(release)

	testPoll(t, func() bool { return runs.Load() == 1 }, 20*time.Millisecond, 2*time.Second,
		"concurrent notification must coalesce to one run, got %d", runs.Load())
}

func TestReconcileTickerDrivesConvergence(t *testing.T) {
	// Same harness as TestControllerCanStartNewPartitions, but nobody calls
	// ClusterStateChangeNotification manually — the ticker must drive the
	// partition creation on its own.
	mux, ln, err := network.NewNodeMux("")
	assert.NoError(t, err)
	go func() { _ = mux.Serve() }()
	addr := ln.Addr().String()
	_, port, err := net.SplitHostPort(addr)
	assert.NoError(t, err)

	tStore := &ControllerTestStore{
		id: "test-node-1", addr: fmt.Sprintf("127.0.0.1:%s", port),
		clusterState: state.Cluster{
			Config:     state.ClusterConfig{DesiredPartitions: 1},
			Partitions: map[uint32]state.Partition{},
			Nodes:      map[string]state.Node{},
		},
		leader: true,
	}
	srvLn := network.NewZenBpmClusterListener(mux)
	srv := server.New(srvLn, tStore, nil, nil, nil)
	assert.NoError(t, srv.Open())
	clientMgr := client.NewClientManager(tStore)

	controller, err := NewController(mux, config.Cluster{
		NodeId: tStore.id, Addr: tStore.addr, Adv: tStore.addr,
		ReconcileInterval: 100 * time.Millisecond,
		Raft: config.ClusterRaft{
			Dir: t.TempDir(), JoinAttempts: 2, JoinInterval: 100 * time.Millisecond,
			JoinAddresses: []string{tStore.addr}, BootstrapExpect: 1,
			BootstrapExpectTimeout: 1 * time.Second,
		},
	})
	assert.NoError(t, err)
	tStore.clusterState.Nodes[tStore.id] = state.Node{
		Id: tStore.id, Addr: tStore.addr, Suffrage: raft.Voter,
		State: state.NodeStateStarted, Role: state.RoleLeader,
		Partitions: map[uint32]state.NodePartition{},
	}
	assert.NoError(t, controller.Start(t.Context(), tStore, clientMgr))
	defer controller.Stop()

	testPoll(t, func() bool {
		s := controller.store.ClusterState()
		return tStore.id == s.Partitions[1].LeaderId &&
			state.NodePartitionStateInitialized == s.Nodes[tStore.id].Partitions[1].State
	}, 100*time.Millisecond, 10*time.Second, "ticker did not drive partition creation")
}
```

Add `"sync/atomic"` to the test imports.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/cluster/controller/... -run 'TestReconcileSingleFlight|TestReconcileTicker' -v`
Expected: `TestReconcileSingleFlight` FAILs (runs == 2); `TestReconcileTicker` FAILs by timeout — note `Start()` currently calls `ClusterStateChangeNotification` once, which would create the partition; **the ticker test only proves its point after Step 3 keeps that initial call but the test would pass vacuously.** To make it honestly red first: the test sets `DesiredPartitions: 1` but registers the node **after** `Start` returns... it registers before. Adjust: move the `tStore.clusterState.Nodes[...] = ...` assignment to **after** `controller.Start(...)` (as written above it is before — swap those two statements when writing the test). Then the initial in-`Start` notification sees no nodes and does nothing; only a later tick can create the partition. Verify red: timeout.

- [ ] **Step 3: Implement** in `controller.go`:

(a) struct fields:

```go
	reconcileInFlight atomic.Bool
```

(add `"sync/atomic"` import).

(b) single-flight in `ClusterStateChangeNotification` (line ~96), after the `handleClusterChanges` guard:

```go
	// Coalesce concurrent invocations (per-apply FSM goroutines + ticker).
	// Dropped edges are re-covered by the periodic ticker, so drop—don't queue.
	if !c.reconcileInFlight.CompareAndSwap(false, true) {
		return
	}
	defer c.reconcileInFlight.Store(false)
```

(c) ticker at the end of `Start()` (before the final `ClusterStateChangeNotification(ctx)` call is fine too — keep that call):

```go
	interval := c.Config.ReconcileInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	safego.Go("controller-reconcile-ticker", c.logger, func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				c.ClusterStateChangeNotification(ctx)
			}
		}
	})
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/cluster/controller/... -v`
Expected: all PASS (including existing tests — the single-flight must not break `TestControllerCanStartNewPartitions`, whose calls are sequential).

- [ ] **Step 5: Commit** (doc-sync: mark plan Risk #5 resolved — one-per-pass + ticker; bump Last updated)

```bash
git add internal/cluster/controller/ docs/cluster-implementation-plan.md
git commit -m "feat(cluster): add level-triggered reconcile ticker with single-flight guard"
```

---

### Task 6: Full-replication partition creation + degraded-cluster guard

**Files:**
- Modify: `internal/cluster/controller/controller.go:126-186`
- Test: `internal/cluster/controller/controller_test.go`
- Modify: `docs/cluster-implementation-plan.md` (same commit)

**Interfaces:**
- Produces: rewritten partition block of `performLeaderOperations`; `assignNewPartition` now assigns **all started nodes**; the "0-partition nodes → partition 1" fallback loop is deleted.

- [ ] **Step 1: Write the failing tests.** The fake's `WritePartitionChange` already applies changes to fake state; additionally record calls — add field `partitionWrites []*proto.NodePartitionChange` to `ControllerTestStore` and append at the top of its existing `WritePartitionChange`.

```go
func TestScaleUpAssignsAllStartedNodes(t *testing.T) {
	nodes := map[string]state.Node{}
	for _, id := range []string{"n1", "n2", "n3"} {
		nodes[id] = state.Node{Id: id, State: state.NodeStateStarted,
			Partitions: map[uint32]state.NodePartition{}}
	}
	tStore := &ControllerTestStore{
		id: "n1", leader: true,
		clusterState: state.Cluster{
			Config:     state.ClusterConfig{DesiredPartitions: 1},
			Partitions: map[uint32]state.Partition{},
			Nodes:      nodes,
		},
	}
	c, err := NewController(nil, config.Cluster{NodeId: "n1",
		Raft: config.ClusterRaft{BootstrapExpect: 3}})
	assert.NoError(t, err)
	c.store = tStore

	c.performLeaderOperations(t.Context())

	assert.Len(t, tStore.partitionWrites, 3, "every started node must join the new partition")
	for _, w := range tStore.partitionWrites {
		assert.Equal(t, uint32(1), w.GetPartitionId())
		assert.Equal(t, proto.NodePartitionState_NODE_PARTITION_STATE_JOINING, w.GetState())
	}
}

func TestScaleUpDeferredWhenNodeDown(t *testing.T) {
	tStore := &ControllerTestStore{
		id: "n1", leader: true,
		clusterState: state.Cluster{
			Config:     state.ClusterConfig{DesiredPartitions: 1},
			Partitions: map[uint32]state.Partition{},
			Nodes: map[string]state.Node{
				"n1": {Id: "n1", State: state.NodeStateStarted, Partitions: map[uint32]state.NodePartition{}},
				"n2": {Id: "n2", State: state.NodeStateShutdown, Partitions: map[uint32]state.NodePartition{}},
			},
		},
	}
	c, err := NewController(nil, config.Cluster{NodeId: "n1",
		Raft: config.ClusterRaft{BootstrapExpect: 2}})
	assert.NoError(t, err)
	c.store = tStore

	c.performLeaderOperations(t.Context())

	assert.Empty(t, tStore.partitionWrites,
		"partition creation must defer while started nodes < BootstrapExpect (would wedge below bootstrap-expect)")
}

func TestPartitionScaleUpOnePerPass(t *testing.T) {
	tStore := &ControllerTestStore{
		id: "n1", leader: true,
		clusterState: state.Cluster{
			Config:     state.ClusterConfig{DesiredPartitions: 3},
			Partitions: map[uint32]state.Partition{},
			Nodes: map[string]state.Node{
				"n1": {Id: "n1", State: state.NodeStateStarted, Partitions: map[uint32]state.NodePartition{}},
			},
		},
	}
	c, err := NewController(nil, config.Cluster{NodeId: "n1",
		Raft: config.ClusterRaft{BootstrapExpect: 1}})
	assert.NoError(t, err)
	c.store = tStore

	// pass 1: only partition 1 is assigned (JOINING) — partition 2 must wait
	c.performLeaderOperations(t.Context())
	for _, w := range tStore.partitionWrites {
		assert.Equal(t, uint32(1), w.GetPartitionId(), "no partition beyond 1 before 1 has a leader")
	}

	// pass 2 without leader election: still nothing new (throttle holds)
	writesBefore := len(tStore.partitionWrites)
	c.performLeaderOperations(t.Context())
	assert.Equal(t, writesBefore, len(tStore.partitionWrites))

	// simulate partition 1 electing a leader → next pass assigns partition 2
	tStore.clusterState.Partitions[1] = state.Partition{Id: 1, LeaderId: "n1"}
	c.performLeaderOperations(t.Context())
	last := tStore.partitionWrites[len(tStore.partitionWrites)-1]
	assert.Equal(t, uint32(2), last.GetPartitionId())
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/cluster/controller/... -run 'TestScaleUp|TestPartitionScaleUpOnePerPass' -v`
Expected: FAIL — current code assigns to one least-stressed node and re-assigns 0-partition nodes to partition 1 (write counts/targets differ).

- [ ] **Step 3: Implement.** Replace the partition block in `performLeaderOperations` (current lines 126-138, both the count check and the 0-partition fallback loop):

```go
	// Partition reconciliation: one new partition per pass (throttle — the next
	// one starts only after this one elected a leader and appeared in
	// cs.Partitions), every started node becomes a member (full replication,
	// RF = cluster size; RF<N placement is Phase 4).
	currentPartitionCount := len(cs.Partitions)
	if int(cs.Config.DesiredPartitions) > currentPartitionCount {
		started := make([]state.Node, 0, len(cs.Nodes))
		for _, n := range cs.Nodes {
			if n.State == state.NodeStateStarted {
				started = append(started, n)
			}
		}
		// Guard (R2): each partition group inherits the global BootstrapExpect;
		// forming one while a member is down would leave it below bootstrap-expect
		// and wedge it until the 120s timeout. Defer — the ticker retries.
		if len(started) < c.Config.Raft.BootstrapExpect {
			c.logger.Warn(fmt.Sprintf(
				"deferring creation of partition %d: %d started nodes < bootstrapExpect %d",
				currentPartitionCount+1, len(started), c.Config.Raft.BootstrapExpect))
		} else {
			next := currentPartitionCount + 1
			if !cs.AnyNodeHasPartition(next) {
				for _, n := range started {
					c.assignPartition(ctx, uint32(next), n.Id)
				}
			}
		}
	}
```

Delete `assignNewPartition` (no longer called) and the old `for _, node := range cs.Nodes { if len(node.Partitions) == 0 { c.assignPartition(ctx, 1, node.Id) } }` loop. Keep `assignPartition` unchanged.

- [ ] **Step 4: Run tests**

Run: `go test ./internal/cluster/controller/... -v`
Expected: PASS, including `TestControllerCanStartNewPartitions` (1 started node, BootstrapExpect 1 — new code assigns partition 1 to it; on desired=2 it assigns partition 2 the same way).

- [ ] **Step 5: MILESTONE — fast e2e**

Run: `make test-e2e-cluster`
Expected: PASS — 3-node formation now goes through "all started nodes join partition 1" (same observable behavior as the old fallback loop, now by design).

- [ ] **Step 6: Doc-sync + commit.** Update `docs/cluster-implementation-plan.md`: rewrite the 2.5 block (the plan's naive for-loop is superseded by one-per-pass + full replication + R2 guard — reference the spec), bump Last updated.

```bash
git add internal/cluster/controller/ docs/cluster-implementation-plan.md
git commit -m "feat(cluster): create partitions one per pass with all started nodes as members"
```

---

### Task 7: `ConfigurationUpdate` gRPC — service proto, validation, forwarding

**Files:**
- Modify: `internal/cluster/proto/zen_cluster.proto:866-869`
- Modify: `internal/cluster/server/server.go` (`StoreService` interface + RPC at line ~180)
- Test: `internal/cluster/server/server_test.go`
- Modify: `docs/cluster-implementation-plan.md` (same commit)

**Interfaces:**
- Consumes: `Store.WriteConfigurationChange` (Task 2), `client.ClientManager.ClusterLeader()`.
- Produces: `ConfigurationUpdateRequest{DesiredPartitions uint32}`, `ConfigurationUpdateResponse{DesiredPartitions uint32}`; `StoreService` gains `IsLeader() bool` and `WriteConfigurationChange(...)`. Follower forwards to leader; **all validation happens on the leader** (follower state may lag).

- [ ] **Step 1: Edit `zen_cluster.proto`** — replace the empty stubs:

```protobuf
message ConfigurationUpdateRequest {
  uint32 desired_partitions = 1;
}
message ConfigurationUpdateResponse {
  uint32 desired_partitions = 1; // the applied (current) value
}
```

Run: `make generate && go build ./...` — expected: success (RPC still returns Unimplemented).

- [ ] **Step 2: Write the failing test.** In `server_test.go`, extend the in-package `testStore` fake with whatever `StoreService` now requires (compile errors guide you):

```go
func (t *testStore) IsLeader() bool          { return t.isLeader }
func (t *testStore) ClusterState() state.Cluster { return t.clusterState }
func (t *testStore) WriteConfigurationChange(change *protoc.ConfigurationChange) error {
	t.configChange = change
	if change.GetDesiredPartitions() != 0 {
		t.clusterState.Config.DesiredPartitions = change.GetDesiredPartitions()
	}
	return nil
}
```

(add fields `isLeader bool`, `clusterState state.Cluster`, `configChange *protoc.ConfigurationChange`; if the fake already has `ClusterState`, keep the existing one and only add what's missing). Then, reusing the gRPC plumbing pattern from `TestServer` (mux + `network.NewZenBpmClusterListener` + `proto.NewZenServiceClient`):

```go
func TestConfigurationUpdateOnLeader(t *testing.T) {
	ctx := t.Context()
	mux, _, err := network.NewNodeMux("")
	require.NoError(t, err)
	cLn := network.NewZenBpmClusterListener(mux)
	tStore := &testStore{isLeader: true, clusterState: state.Cluster{
		Config:     state.ClusterConfig{DesiredPartitions: 1},
		Partitions: map[uint32]state.Partition{1: {Id: 1, LeaderId: "n1"}},
		Nodes:      map[string]state.Node{},
	}}
	srv := New(cLn, tStore, nil, nil, nil)
	require.NoError(t, srv.Open())
	zsc := newTestZenClient(t, cLn) // extract the grpc dial block from TestServer into this helper

	// invalid: zero
	_, err = zsc.ConfigurationUpdate(ctx, &proto.ConfigurationUpdateRequest{DesiredPartitions: ptr.To(uint32(0))})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	// invalid: above mux limit
	_, err = zsc.ConfigurationUpdate(ctx, &proto.ConfigurationUpdateRequest{DesiredPartitions: ptr.To(uint32(123))})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	// decrease below desired: FailedPrecondition (validated against Desired, not formed count)
	tStore.clusterState.Config.DesiredPartitions = 3
	_, err = zsc.ConfigurationUpdate(ctx, &proto.ConfigurationUpdateRequest{DesiredPartitions: ptr.To(uint32(2))})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	// idempotent same value: success, no write
	resp, err := zsc.ConfigurationUpdate(ctx, &proto.ConfigurationUpdateRequest{DesiredPartitions: ptr.To(uint32(3))})
	require.NoError(t, err)
	require.Equal(t, uint32(3), resp.GetDesiredPartitions())
	require.Nil(t, tStore.configChange)

	// increase: success, write recorded
	resp, err = zsc.ConfigurationUpdate(ctx, &proto.ConfigurationUpdateRequest{DesiredPartitions: ptr.To(uint32(5))})
	require.NoError(t, err)
	require.Equal(t, uint32(5), resp.GetDesiredPartitions())
	require.Equal(t, uint32(5), tStore.configChange.GetDesiredPartitions())
}

func TestConfigurationUpdateForwardsToLeader(t *testing.T) {
	// leader server
	ctx := t.Context()
	muxL, _, err := network.NewNodeMux("")
	require.NoError(t, err)
	lnL := network.NewZenBpmClusterListener(muxL)
	leaderStore := &testStore{isLeader: true, clusterState: state.Cluster{
		Config: state.ClusterConfig{DesiredPartitions: 1},
		Partitions: map[uint32]state.Partition{}, Nodes: map[string]state.Node{},
	}}
	require.NoError(t, New(lnL, leaderStore, nil, nil, nil).Open())

	// follower server whose client manager resolves the leader to lnL's address
	muxF, _, err := network.NewNodeMux("")
	require.NoError(t, err)
	lnF := network.NewZenBpmClusterListener(muxF)
	followerStore := &testStore{isLeader: false, leaderAddr: lnL.Addr().String(), leaderID: "leader-1"}
	clientMgr := client.NewClientManager(followerStore)
	require.NoError(t, New(lnF, followerStore, nil, nil, clientMgr).Open())
	zsc := newTestZenClient(t, lnF)

	resp, err := zsc.ConfigurationUpdate(ctx, &proto.ConfigurationUpdateRequest{DesiredPartitions: ptr.To(uint32(4))})
	require.NoError(t, err)
	require.Equal(t, uint32(4), resp.GetDesiredPartitions())
	require.Equal(t, uint32(4), leaderStore.configChange.GetDesiredPartitions(),
		"write must land on the leader store")
}
```

The fake needs `leaderAddr`/`leaderID` fields returned by its `LeaderWithID()` (`client.ClientStore` requirement — check `client.NewClientManager`'s parameter interface and implement exactly what it needs; the controller test's fake at `controller_test.go` shows the pattern).

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test ./internal/cluster/server/... -run TestConfigurationUpdate -v`
Expected: FAIL — RPC returns `codes.Unimplemented`.

- [ ] **Step 4: Implement.** In `server.go`: extend `StoreService`:

```go
type StoreService interface {
	Notify(nr *proto.NotifyRequest) error
	Join(jr *proto.JoinRequest) error
	WriteNodeChange(change *protoc.NodeChange) error
	ClusterState() state.Cluster
	WritePartitionChange(change *protoc.NodePartitionChange) error
	WriteMaintenanceChange(change *protoc.ClusterMaintenanceChange) error
	WriteConfigurationChange(change *protoc.ConfigurationChange) error
	IsLeader() bool
}
```

Replace the stub (line ~180):

```go
const maxPartitions = 122 // network mux single-byte partition-id limit

func (s *Server) ConfigurationUpdate(ctx context.Context, req *proto.ConfigurationUpdateRequest) (*proto.ConfigurationUpdateResponse, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	// Followers forward; validation runs on the leader only, against
	// authoritative (non-lagging) state. Recursion terminates at the leader;
	// leadership churn is bounded by the caller's context deadline.
	if !s.store.IsLeader() {
		leader, err := s.client.ClusterLeader()
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "no cluster leader available: %s", err)
		}
		return leader.ConfigurationUpdate(ctx, req)
	}

	n := req.GetDesiredPartitions()
	if n < 1 || n > maxPartitions {
		return nil, status.Errorf(codes.InvalidArgument,
			"desiredPartitions must be between 1 and %d, got %d", maxPartitions, n)
	}
	desired := s.store.ClusterState().Config.DesiredPartitions
	if n < desired {
		return nil, status.Errorf(codes.FailedPrecondition,
			"scale-down from %d to %d partitions is not supported", desired, n)
	}
	if n == desired {
		return &proto.ConfigurationUpdateResponse{DesiredPartitions: ptr.To(n)}, nil
	}
	if err := s.store.WriteConfigurationChange(&protoc.ConfigurationChange{DesiredPartitions: ptr.To(n)}); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to write configuration change: %s", err)
	}
	return &proto.ConfigurationUpdateResponse{DesiredPartitions: ptr.To(n)}, nil
}
```

Any other fake implementing `StoreService` (e.g. `ControllerTestStore` if it's passed to `server.New` in controller tests — it is) needs `IsLeader` (already has) and `WriteConfigurationChange` (added in Task 4) plus `WriteMaintenanceChange` if missing — compile errors will list them; implement as recording no-ops.

- [ ] **Step 5: Run tests**

Run: `go test ./internal/cluster/server/... ./internal/cluster/controller/... -v`
Expected: PASS.

- [ ] **Step 6: Doc-sync + commit.** Plan file: check off 2.4; fix the Panic Tracker rows for `ConfigurationUpdate` (it returned `codes.Unimplemented`, not a panic — correct the row and mark Done); bump Last updated.

```bash
git add internal/cluster/proto/ internal/cluster/server/ internal/cluster/controller/ docs/cluster-implementation-plan.md
git commit -m "feat(cluster): implement ConfigurationUpdate RPC with leader validation and forwarding"
```

---

### Task 8: REST admin endpoint `PUT /system/v1/cluster/config`

**Files:**
- Modify: `openapi/system.yaml`
- Modify: `internal/cluster/node.go` (new method)
- Create: `internal/rest/cluster_config.go`
- Modify: `internal/rest/server.go:88-91` (route)
- Test: `internal/rest/cluster_config_test.go`
- Modify: `docs/cluster-implementation-plan.md` (same commit)

**Interfaces:**
- Consumes: `ConfigurationUpdate` RPC (Task 7), `node.client.ClusterLeader()`.
- Produces: `func (node *ZenNode) UpdateClusterConfiguration(ctx context.Context, desiredPartitions uint32) (uint32, error)`; REST handler mapping gRPC codes → 400/409/503.

- [ ] **Step 1: Write the failing test** — pure status-mapping test in `internal/rest/cluster_config_test.go`:

```go
package rest

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestConfigUpdateStatusMapping(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{"invalid argument", status.Error(codes.InvalidArgument, "bad n"), http.StatusBadRequest},
		{"scale down", status.Error(codes.FailedPrecondition, "no scale-down"), http.StatusConflict},
		{"no leader", status.Error(codes.Unavailable, "no leader"), http.StatusServiceUnavailable},
		{"other", status.Error(codes.Internal, "boom"), http.StatusInternalServerError},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, configUpdateStatusCode(tc.err))
		})
	}
}
```

Run: `go test ./internal/rest/... -run TestConfigUpdateStatusMapping -v` — expected FAIL: `configUpdateStatusCode` undefined.

- [ ] **Step 2: Implement `ZenNode` method** in `internal/cluster/node.go` (near `PublishMessage`):

```go
// UpdateClusterConfiguration changes the desired partition count through the
// cluster leader. All validation lives in the ConfigurationUpdate RPC (single
// source of truth); this method only routes. Returns the applied value.
func (node *ZenNode) UpdateClusterConfiguration(ctx context.Context, desiredPartitions uint32) (uint32, error) {
	leader, err := node.client.ClusterLeader()
	if err != nil {
		return 0, status.Errorf(codes.Unavailable, "no cluster leader available: %s", err)
	}
	resp, err := leader.ConfigurationUpdate(ctx, &proto.ConfigurationUpdateRequest{
		DesiredPartitions: ptr.To(desiredPartitions),
	})
	if err != nil {
		return 0, err
	}
	return resp.GetDesiredPartitions(), nil
}
```

(match the file's existing imports: `proto` is `zenproto`/`proto` per the file's alias for `internal/cluster/proto` — check the import block and use its alias; add `google.golang.org/grpc/codes`/`status` if absent.)

- [ ] **Step 3: Implement handler** — create `internal/rest/cluster_config.go`:

```go
package rest

import (
	"encoding/json"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type clusterConfigUpdateRequest struct {
	DesiredPartitions uint32 `json:"desiredPartitions"`
}

type clusterConfigUpdateResponse struct {
	DesiredPartitions uint32 `json:"desiredPartitions"`
}

// configUpdateStatusCode maps gRPC validation/routing errors from the
// ConfigurationUpdate RPC onto HTTP status codes.
func configUpdateStatusCode(err error) int {
	switch status.Code(err) {
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.FailedPrecondition:
		return http.StatusConflict
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

// handleClusterConfigUpdate implements PUT /system/v1/cluster/config.
func (s *Server) handleClusterConfigUpdate(w http.ResponseWriter, r *http.Request) {
	var req clusterConfigUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	applied, err := s.node.UpdateClusterConfiguration(r.Context(), req.DesiredPartitions)
	if err != nil {
		http.Error(w, err.Error(), configUpdateStatusCode(err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(clusterConfigUpdateResponse{DesiredPartitions: applied})
}
```

Register the route in `internal/rest/server.go` inside the existing `/system` block (line ~88):

```go
		r.Put("/v1/cluster/config", s.handleClusterConfigUpdate)
```

- [ ] **Step 4: Document in `openapi/system.yaml`** — add after the `/system/v1/cluster/restore` path, following the file's existing style:

```yaml
  /system/v1/cluster/config:
    put:
      operationId: updateClusterConfiguration
      summary: Update replicated cluster configuration (desired partition count)
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              required: [desiredPartitions]
              properties:
                desiredPartitions:
                  type: integer
                  minimum: 1
                  maximum: 122
      responses:
        "200":
          description: Applied configuration
          content:
            application/json:
              schema:
                type: object
                properties:
                  desiredPartitions:
                    type: integer
        "400": { description: Invalid desiredPartitions value }
        "409": { description: Scale-down not supported }
        "503": { description: No cluster leader available }
```

- [ ] **Step 5: Run tests and build**

Run: `go test ./internal/rest/... -v && go build ./...`
Expected: PASS.

- [ ] **Step 6: Doc-sync + commit.** Plan file: add REST endpoint note to Phase 2 section (D8); bump Last updated.

```bash
git add openapi/system.yaml internal/rest/ internal/cluster/node.go docs/cluster-implementation-plan.md
git commit -m "feat(cluster): expose cluster configuration update via REST admin endpoint"
```

---

### Task 9: Pointer rebuild on scale-up

**Files:**
- Modify: `internal/cluster/backup/coordinator.go` (extract shared helpers)
- Modify: `internal/cluster/controller/controller.go`
- Test: `internal/cluster/controller/controller_test.go`
- Modify: `docs/cluster-implementation-plan.md` (same commit)

**Interfaces:**
- Consumes: `backup.PlanPointerRebuild`, `backup.ClientProvider`, `ListActiveMessageSubscriptions`/`RebuildMessageSubscriptionPointers` RPCs, `RoutingPartitions` (Task 1), `WriteMaintenanceChange`.
- Produces: `backup.CollectSubscriptionRows(ctx, clients ClientProvider, cs state.Cluster) ([]*proto.MessageSubscriptionRow, error)` and `backup.ApplyPointerRebuild(ctx, clients ClientProvider, cs state.Cluster, rows []*proto.MessageSubscriptionRow) (rebuilt int, conflicts []PointerConflict, err error)`; controller `maybeRebuildPointers(ctx, cs)` called at the end of `performLeaderOperations`; `ControlledStore` gains `WriteMaintenanceChange`.

- [ ] **Step 1: Extract shared helpers (refactor, no behavior change).** In `backup/coordinator.go`, split the body of `reconcile` (lines ~233-267): the collect loop becomes `CollectSubscriptionRows`, the plan+rebuild loops become `ApplyPointerRebuild`; `reconcile` calls both and fills the report:

```go
// CollectSubscriptionRows lists active message subscriptions from every
// partition leader. Shared by restore reconcile and scale-up pointer rebuild.
func CollectSubscriptionRows(ctx context.Context, clients ClientProvider, cs state.Cluster) ([]*proto.MessageSubscriptionRow, error) {
	var all []*proto.MessageSubscriptionRow
	for id := range cs.Partitions {
		leader, err := clients.PartitionLeader(id)
		if err != nil {
			return nil, fmt.Errorf("pointer scan: failed to get leader for partition %d: %w", id, err)
		}
		resp, err := leader.ListActiveMessageSubscriptions(ctx, &proto.ListActiveMessageSubscriptionsRequest{PartitionId: ptr.To(id)})
		if err != nil {
			return nil, fmt.Errorf("pointer scan on partition %d failed: %w", id, err)
		}
		all = append(all, resp.GetRows()...)
	}
	return all, nil
}

// ApplyPointerRebuild re-plans the given subscription rows against the current
// topology and rewrites every partition's pointer table (wipe + reinsert; a
// partition with zero winning rows still gets a call to clear stale pointers).
func ApplyPointerRebuild(ctx context.Context, clients ClientProvider, cs state.Cluster, rows []*proto.MessageSubscriptionRow) (int, []PointerConflict, error) {
	plan := PlanPointerRebuild(rows, cs.GetPartitionIdForMessageSubscriptionPointer)
	rebuilt := 0
	for id := range cs.Partitions {
		leader, err := clients.PartitionLeader(id)
		if err != nil {
			return rebuilt, plan.Conflicts, fmt.Errorf("pointer rebuild: failed to get leader for partition %d: %w", id, err)
		}
		if _, err = leader.RebuildMessageSubscriptionPointers(ctx, &proto.RebuildMessageSubscriptionPointersRequest{
			PartitionId: ptr.To(id),
			Pointers:    plan.ByPartition[id],
		}); err != nil {
			return rebuilt, plan.Conflicts, fmt.Errorf("pointer rebuild on partition %d failed: %w", id, err)
		}
		rebuilt += len(plan.ByPartition[id])
	}
	return rebuilt, plan.Conflicts, nil
}
```

Rewrite `reconcile`'s pointer section to use them (definition sync stays first):

```go
	rows, err := CollectSubscriptionRows(ctx, deps.Clients, cs)
	if err != nil {
		return err
	}
	rebuilt, conflicts, err := ApplyPointerRebuild(ctx, deps.Clients, cs, rows)
	report.PointerConflicts = conflicts
	report.PointersRebuilt = rebuilt
	if err != nil {
		return err
	}
	return nil
```

Run: `go test ./internal/cluster/backup/... -v` — expected: PASS (pure refactor).

- [ ] **Step 2: Write the failing controller tests.** For unit-testability without real partition gRPC, the controller gets injectable function fields defaulting to the backup helpers:

```go
func TestPointerRebuildWaitsForFormation(t *testing.T) {
	tStore := newRebuildTestStore(1 /*routing*/, 3 /*desired*/, 2 /*formed*/)
	c := newRebuildTestController(t, tStore)
	collectCalled := false
	c.collectPointerRows = func(ctx context.Context, cs state.Cluster) ([]*zenproto.MessageSubscriptionRow, error) {
		collectCalled = true
		return nil, nil
	}
	c.performLeaderOperations(t.Context())
	assert.False(t, collectCalled, "rebuild must wait until len(Partitions) == DesiredPartitions")
}

func TestFreshFormationSkipsRebuildGate(t *testing.T) {
	tStore := newRebuildTestStore(0, 1, 1)
	c := newRebuildTestController(t, tStore)
	c.collectPointerRows = func(ctx context.Context, cs state.Cluster) ([]*zenproto.MessageSubscriptionRow, error) {
		return nil, nil // fresh cluster: no pointers
	}
	applyCalled := false
	c.applyPointerRebuild = func(ctx context.Context, cs state.Cluster, rows []*zenproto.MessageSubscriptionRow) error {
		applyCalled = true
		return nil
	}
	c.performLeaderOperations(t.Context())

	assert.False(t, applyCalled, "zero pointers: no rebuild call")
	assert.Empty(t, tStore.maintenanceWrites, "zero pointers: no maintenance gate")
	assert.Equal(t, uint32(1), tStore.clusterState.Config.RoutingPartitions,
		"RoutingPartitions marker must still be advanced")
}

func TestPointerRebuildTriggeredAfterScaleUp(t *testing.T) {
	tStore := newRebuildTestStore(1, 3, 3)
	c := newRebuildTestController(t, tStore)
	c.collectPointerRows = func(ctx context.Context, cs state.Cluster) ([]*zenproto.MessageSubscriptionRow, error) {
		return []*zenproto.MessageSubscriptionRow{{}}, nil // one live pointer
	}
	applyCalled := false
	c.applyPointerRebuild = func(ctx context.Context, cs state.Cluster, rows []*zenproto.MessageSubscriptionRow) error {
		applyCalled = true
		return nil
	}
	c.performLeaderOperations(t.Context())

	assert.True(t, applyCalled)
	// gate set true then false around the rebuild
	assert.Equal(t, []bool{true, false}, tStore.maintenanceWrites)
	assert.Equal(t, uint32(3), tStore.clusterState.Config.RoutingPartitions)
}

func TestPointerRebuildPendingSurvivesLeaderChange(t *testing.T) {
	// A brand-new controller (fresh memory) must detect the pending rebuild
	// purely from replicated state: routing(1) != formed(3) == desired(3).
	tStore := newRebuildTestStore(1, 3, 3)
	c := newRebuildTestController(t, tStore) // fresh instance, no prior knowledge
	rebuilt := false
	c.collectPointerRows = func(ctx context.Context, cs state.Cluster) ([]*zenproto.MessageSubscriptionRow, error) {
		return []*zenproto.MessageSubscriptionRow{{}}, nil
	}
	c.applyPointerRebuild = func(ctx context.Context, cs state.Cluster, rows []*zenproto.MessageSubscriptionRow) error {
		rebuilt = true
		return nil
	}
	c.performLeaderOperations(t.Context())
	assert.True(t, rebuilt, "pending rebuild must be re-detected from replicated state")
}
```

Shared helpers for these tests (same file):

```go
// newRebuildTestStore builds a leader fake with `formed` partitions (each with
// a leader), desired/routing config values, and one started node per partition.
func newRebuildTestStore(routing, desired, formed uint32) *ControllerTestStore {
	partitions := map[uint32]state.Partition{}
	nodeParts := map[uint32]state.NodePartition{}
	for i := uint32(1); i <= formed; i++ {
		partitions[i] = state.Partition{Id: i, LeaderId: "n1"}
		nodeParts[i] = state.NodePartition{Id: i, State: state.NodePartitionStateInitialized, Role: state.RoleLeader}
	}
	return &ControllerTestStore{
		id: "n1", leader: true,
		clusterState: state.Cluster{
			Config:     state.ClusterConfig{DesiredPartitions: desired, RoutingPartitions: routing},
			Partitions: partitions,
			Nodes: map[string]state.Node{
				"n1": {Id: "n1", State: state.NodeStateStarted, Partitions: nodeParts},
			},
		},
	}
}

func newRebuildTestController(t *testing.T, tStore *ControllerTestStore) *Controller {
	c, err := NewController(nil, config.Cluster{NodeId: "n1",
		Raft: config.ClusterRaft{BootstrapExpect: 1}})
	assert.NoError(t, err)
	c.store = tStore
	return c
}
```

The fake also needs maintenance recording:

```go
func (c *ControllerTestStore) WriteMaintenanceChange(change *proto.ClusterMaintenanceChange) error {
	c.maintenanceWrites = append(c.maintenanceWrites, change.GetRestoring())
	c.clusterState.Restoring = change.GetRestoring()
	return nil
}
```

(field `maintenanceWrites []bool`). Note `zenproto` is the existing test-file alias for `internal/cluster/proto`.

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test ./internal/cluster/controller/... -run TestPointerRebuild -v`
Expected: FAIL — `collectPointerRows` field undefined.

- [ ] **Step 4: Implement.** In `controller.go`:

(a) `ControlledStore` gains `WriteMaintenanceChange(change *proto.ClusterMaintenanceChange) error`.

(b) Controller struct fields + defaults in `Start()` (after `c.client = clientMgr`):

```go
	// injectable for unit tests; default to the shared backup helpers
	collectPointerRows  func(ctx context.Context, cs state.Cluster) ([]*zenproto.MessageSubscriptionRow, error)
	applyPointerRebuild func(ctx context.Context, cs state.Cluster, rows []*zenproto.MessageSubscriptionRow) error
```

```go
	if c.collectPointerRows == nil {
		c.collectPointerRows = func(ctx context.Context, cs state.Cluster) ([]*zenproto.MessageSubscriptionRow, error) {
			return backup.CollectSubscriptionRows(ctx, c.client, cs)
		}
	}
	if c.applyPointerRebuild == nil {
		c.applyPointerRebuild = func(ctx context.Context, cs state.Cluster, rows []*zenproto.MessageSubscriptionRow) error {
			rebuilt, conflicts, err := backup.ApplyPointerRebuild(ctx, c.client, cs, rows)
			if len(conflicts) > 0 {
				c.logger.Warn(fmt.Sprintf("pointer rebuild resolved %d correlation conflicts", len(conflicts)))
			}
			if err == nil {
				c.logger.Info(fmt.Sprintf("pointer rebuild re-placed %d pointers across %d partitions", rebuilt, len(cs.Partitions)))
			}
			return err
		}
	}
```

(check that `internal/cluster/backup` importing does not cycle with `controller` — backup imports `client`, `state`, `proto`, not `controller`; if `backup` were to import `controller` transitively, move the two helpers into a leaf package instead — verify with `go build ./...`).

(c) trigger at the **end** of `performLeaderOperations`:

```go
	// Pointer-routing rebuild (spec §6): fires only after formation completes,
	// pending state derived from replicated config so it survives failover.
	if len(cs.Partitions) > 0 &&
		len(cs.Partitions) == int(cs.Config.DesiredPartitions) &&
		cs.Config.RoutingPartitions != uint32(len(cs.Partitions)) {
		c.rebuildPointerRouting(ctx, cs)
	}
```

(d) the method:

```go
// rebuildPointerRouting re-places message-subscription pointers after the
// partition count changed. Zero live pointers (fresh bootstrap, upgrade) skips
// the maintenance gate entirely and just advances the marker.
func (c *Controller) rebuildPointerRouting(ctx context.Context, cs state.Cluster) {
	n := uint32(len(cs.Partitions))
	rows, err := c.collectPointerRows(ctx, cs)
	if err != nil {
		c.logger.Error(fmt.Sprintf("pointer rebuild: collect failed, will retry: %s", err))
		return
	}
	if len(rows) > 0 {
		// Gate engine traffic exactly like restore does: quiesces subscription
		// creation so the wipe+reinsert cannot lose concurrent pointer writes.
		if err := c.store.WriteMaintenanceChange(&proto.ClusterMaintenanceChange{Restoring: ptr.To(true)}); err != nil {
			c.logger.Error(fmt.Sprintf("pointer rebuild: failed to set maintenance gate: %s", err))
			return
		}
		if err := c.applyPointerRebuild(ctx, cs, rows); err != nil {
			// leave the gate up: rule still pending, next reconcile pass retries;
			// matches restore semantics (operator-visible, idempotent retry)
			c.logger.Error(fmt.Sprintf("pointer rebuild failed, gate stays up for retry: %s", err))
			return
		}
	}
	if err := c.store.WriteConfigurationChange(&proto.ConfigurationChange{RoutingPartitions: ptr.To(n)}); err != nil {
		c.logger.Error(fmt.Sprintf("pointer rebuild: failed to advance RoutingPartitions: %s", err))
		return
	}
	if len(rows) > 0 {
		if err := c.store.WriteMaintenanceChange(&proto.ClusterMaintenanceChange{Restoring: ptr.To(false)}); err != nil {
			c.logger.Error(fmt.Sprintf("pointer rebuild: failed to clear maintenance gate: %s", err))
		}
	}
}
```

- [ ] **Step 5: Run tests**

Run: `go test ./internal/cluster/... -v`
Expected: PASS (all packages: controller, store, server, backup).

- [ ] **Step 6: MILESTONE — fast e2e** (fresh formation must not flake on the new marker write)

Run: `make test-e2e-cluster`
Expected: PASS.

- [ ] **Step 7: Doc-sync + commit.** Plan file: describe the rebuild under Phase 2; note 3.3's premise is stale (pointer indirection already gives exact addressing; scale-up case now closed) — do **not** mark 3.3 done yet (needs e2e verification in Task 12); bump Last updated.

```bash
git add internal/cluster/ docs/cluster-implementation-plan.md
git commit -m "feat(cluster): rebuild message pointers after partition scale-up via RoutingPartitions marker"
```

---

### Task 10: `LeastStressedPartition` off-by-one fix

**Files:**
- Modify: `internal/cluster/state/state.go:103`
- Test: `internal/cluster/state/state_test.go`
- Modify: `docs/cluster-implementation-plan.md` (same commit)

- [ ] **Step 1: Write the failing test:**

```go
func TestLeastStressedPartitionSinglePartition(t *testing.T) {
	c := Cluster{Partitions: map[uint32]Partition{1: {Id: 1, LeaderId: "n1"}}}
	// must not panic (rand.Intn(0)) and must return the only partition
	p, err := c.LeastStressedPartition()
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), p.Id)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/cluster/state/... -run TestLeastStressedPartitionSinglePartition -v`
Expected: FAIL — panic `invalid argument to Intn`.

- [ ] **Step 3: Fix** `state.go:103`:

```go
	pick := rand.Intn(len(c.Partitions))
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/cluster/state/... -v`
Expected: PASS.

- [ ] **Step 5: Doc-sync + commit.** Plan file: check off 3.2, update Phase 3 progress count, bump Last updated.

```bash
git add internal/cluster/state/ docs/cluster-implementation-plan.md
git commit -m "fix(cluster): LeastStressedPartition off-by-one panic on single partition"
```

---

### Task 11: E2E harness wiring + fast-tier multi-partition tests (MILESTONE: fast e2e)

**Files:**
- Modify: `test/e2e/cluster/harness.go` (~line 137 and the `AddNode` config at ~line 287)
- Modify: `test/e2e/cluster/partition_test.go` (un-skip lines 20, 34, 142)
- Modify: `test/e2e/cluster/data_test.go:250`, `test/e2e/cluster/stream_resilience_test.go:214`, `test/e2e/cluster/backup_restore_test.go:191`
- Modify: `docs/cluster-implementation-plan.md` (same commit)

- [ ] **Step 1: Wire `WithPartitions`.** In `harness.go`, both `config.Config` struct literals (NewTestCluster ~line 134 and AddNode ~line 286) gain, inside `Cluster:`:

```go
				DesiredPartitions: uint32(o.partitions),
```

(For `AddNode`, use the cluster's stored `tc.opts.partitions`.) Struct-literal configs skip env-defaults (existing comment at line ~298), and `o.partitions` defaults to 1 in `NewTestCluster`, so single-partition tests are unaffected.

- [ ] **Step 2: Un-skip the fast-tier multi-partition tests.** Delete the `t.Skip("multi-partition formation requires Phase 2: ...")` lines at `partition_test.go:20` (`TestPartitionCreation`), `:34` (`TestPartitionAssignment`), `:142` (`TestMultiplePartitionsPerNode`), and the equivalents at `data_test.go:250`, `stream_resilience_test.go:214`, `backup_restore_test.go:191`.

**Note on `TestMultiplePartitionsPerNode`:** it runs 2 nodes with `WithPartitions(4)` — with full replication each node hosts all 4 partitions, satisfying its `>= 2` assertion.

- [ ] **Step 3: Run the fast tier**

Run: `make test-e2e-cluster`
Expected: PASS including the newly un-skipped tests. If a formation timeout appears in multi-partition tests, first check sequencing (partitions form one at a time — 3 partitions ≈ 3× single-partition formation time) and bump only that test's `WaitForPartitions` timeout, not harness globals.

- [ ] **Step 4: Doc-sync + commit.** Plan file: move the un-skipped tests out of "Unblocks E2E" into done, bump Last updated.

```bash
git add test/e2e/cluster/ docs/cluster-implementation-plan.md
git commit -m "test(cluster): enable multi-partition e2e via WithPartitions harness wiring"
```

---

### Task 12: Runtime scale-up e2e + correlation-after-scale-up + 3.3 verification

**Files:**
- Modify: `test/e2e/cluster/partition_test.go` (`TestIncreasePartitionCount` at ~line 111, new test, `TestMaxPartitions` un-skip at line 160)
- Modify: `test/e2e/cluster/data_test.go` (verify `TestMessageCorrelationAcrossNodes`)
- Modify: `docs/cluster-implementation-plan.md` (same commit)

- [ ] **Step 1: Implement `TestIncreasePartitionCount`** — replace its body's TODO + `t.Skip` (line ~111-123):

```go
func TestIncreasePartitionCount(t *testing.T) {
	// Start with 1 partition, increase to 3 via the REST admin endpoint.
	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)

	WaitForHealthy(t, tc, 150*time.Second)
	WaitForPartitions(t, tc, 1, 30*time.Second)

	// scale up through a FOLLOWER node to exercise forward-to-leader
	followers := tc.Followers()
	require.NotEmpty(t, followers)
	body := bytes.NewBufferString(`{"desiredPartitions": 3}`)
	req, err := http.NewRequest(http.MethodPut,
		"http://"+followers[0].RestAddr+"/system/v1/cluster/config", body)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	WaitForPartitions(t, tc, 3, 120*time.Second)
	for pid := uint32(1); pid <= 3; pid++ {
		AssertPartitionHasLeader(t, tc, pid)
	}

	// scale-down must be rejected
	body = bytes.NewBufferString(`{"desiredPartitions": 1}`)
	req, err = http.NewRequest(http.MethodPut,
		"http://"+followers[0].RestAddr+"/system/v1/cluster/config", body)
	require.NoError(t, err)
	resp2, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp2.Body.Close()
	require.Equal(t, http.StatusConflict, resp2.StatusCode)
}
```

(add `bytes`, `net/http` imports).

- [ ] **Step 2: Add `TestMessageCorrelationAfterScaleUp`** in `partition_test.go`. **First inspect the fixture** `test/e2e/cluster/` uses for message catch (`simple-intermediate-message-catch-event.bpmn`, referenced by `TestMessageCorrelationAcrossNodes` in `data_test.go`): find its message name and the FEEL correlation-key expression, and set the instance variables so the subscription's correlation key equals the published one. Skeleton (adjust `"test-message"`/variable name to the fixture):

```go
func TestMessageCorrelationAfterScaleUp(t *testing.T) {
	// Pointer written at N=1 must still be found after scaling to N=3
	// (the rebuild re-places it; a miss here is the stale-pointer regression).
	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)
	WaitForHealthy(t, tc, 150*time.Second)
	WaitForPartitions(t, tc, 1, 30*time.Second)

	nodeA := tc.Nodes[0]
	DeployDefinitionOnNode(t, nodeA, "simple-intermediate-message-catch-event.bpmn")
	defKey := GetFirstDefinitionKey(t, nodeA)
	instanceKey := CreateInstanceOnNode(t, nodeA, defKey, nil) // set vars per fixture's correlation expression
	require.NotZero(t, instanceKey)

	// scale 1 → 3 and wait for formation + pointer rebuild to settle
	body := bytes.NewBufferString(`{"desiredPartitions": 3}`)
	req, _ := http.NewRequest(http.MethodPut, "http://"+nodeA.RestAddr+"/system/v1/cluster/config", body)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	WaitForPartitions(t, tc, 3, 120*time.Second)

	// publish with the pre-scale-up correlation key — poll: the rebuild window
	// (maintenance gate) may briefly reject requests
	correlationKey := "test-key" // must match the fixture's correlation expression result
	assert.Eventually(t, func() bool {
		msgResp, err := tc.Nodes[1].RestClient.PublishMessageWithResponse(context.Background(),
			zenclient.PublishMessageJSONRequestBody{
				MessageName:    "test-message",
				CorrelationKey: &correlationKey,
			})
		return err == nil && msgResp.StatusCode() < 300
	}, 60*time.Second, 1*time.Second,
		"pre-scale-up subscription must be correlatable after pointer rebuild")
}
```

- [ ] **Step 3: Un-skip `TestMaxPartitions`** (line 160) — it keeps `skipIfShort`, so it runs only in the slow tier.

- [ ] **Step 4: Verify 3.3.** Run `TestMessageCorrelationAcrossNodes` (it is not skipped today; confirm it still passes and actually correlates — if its assertions are the current "routing only" soft checks, leave them but note it). Then run the new tests:

Run: `go test -tags cluster_e2e ./test/e2e/cluster/ -run 'TestIncreasePartitionCount|TestMessageCorrelationAfterScaleUp|TestMessageCorrelationAcrossNodes' -v -timeout 20m`
Expected: PASS.

- [ ] **Step 5: Full fast tier + slow-tier spot-check**

Run: `make test-e2e-cluster`
Expected: PASS. Optionally (manual, long): `make test-e2e-cluster-slow` for `TestMaxPartitions`.

- [ ] **Step 6: Doc-sync + commit.** Plan file: mark 3.3 resolved-by-Phase-2 (stale premise documented in spec §6; scale-up case closed; correlation e2e green), update Phase 3 progress to reflect 3.2+3.3 done, update Risks table row 3 (message correlation routing — resolved: exact addressing + rebuild), bump Last updated.

```bash
git add test/e2e/cluster/ docs/cluster-implementation-plan.md
git commit -m "test(cluster): cover runtime partition scale-up and pointer rebuild end to end"
```

---

### Task 13: Final documentation reconciliation

**Files:**
- Modify: `docs/cluster-implementation-plan.md`

- [ ] **Step 1: Full sweep.** Re-read the whole plan file against reality:
  - Progress Summary: Phase 2 → **Done** (5/5 + REST endpoint + rebuild), Phase 3 → 4/4 (3.1 won't-fix, 3.2 done, 3.3 resolved, 3.4 won't-fix) → **Done**.
  - Dependency graph note: Phase 4/5 now unblocked by Phase 2+3 completion.
  - Panic Tracker: re-verify every `⏳` row's line numbers (`server.go` shifted); `ConfigurationUpdate` row Done.
  - Phase 2 section: replace the original 2.1-2.5 text with what was actually built (reference the spec file), keep decision rationale links.
  - Files Reference table: add `node.go` (config update), `openapi/system.yaml`, `internal/rest/`.
  - Bump `> Last updated:`.

- [ ] **Step 2: Verify everything one last time**

Run: `go build ./... && go test ./internal/cluster/... ./internal/config/... ./internal/rest/... && make test-e2e-cluster`
Expected: all PASS.

- [ ] **Step 3: Commit**

```bash
git add docs/cluster-implementation-plan.md
git commit -m "docs(cluster): mark phase 2 configurable partitions done"
```

---

## Self-Review Notes (already applied)

- **Spec coverage:** D1→T4, D2→T5+T6, D3→T7, D4→T7, D5→T6, D6→T9, D7→T3, D8→T8; R1→T7, R2→T6, R3→T5, R4→T9, R5→T9 (self-healing rule, no code), R6→spec-documented, R7→T7/T12, R8→T3. `TestConfigurationUpdateIdempotentSameValue` folded into `TestConfigurationUpdateOnLeader`; spec's `TestApplyConfigurationChange` folded into `TestWriteConfigurationChange` (the codebase tests FSM behavior through store writes, not FSM methods directly — see `TestWriteMaintenanceChange`). Spec's store-Config plumb dropped (YAGNI, noted in Global Constraints).
- **Known risk points for the implementer:** (1) import cycle check in T9 step 4b; (2) the fixture-dependent correlation key in T12 step 2 requires reading the BPMN file first; (3) T5's ticker test must register the node **after** `Start` to be honestly red; (4) any pre-existing fake implementing `StoreService`/`ControlledStore` must gain the new methods — let the compiler list them.
