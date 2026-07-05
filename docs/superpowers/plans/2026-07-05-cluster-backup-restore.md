# Cluster Backup & Restore Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Whole-cluster backup and restore of all partition rqlite databases via a streaming tar bundle (`GET /v1/cluster/backup`, `POST /v1/cluster/restore?force=true`), with restore-time reconciliation of message-subscription pointers and definitions.

**Architecture:** Each partition leader streams its rqlite `store.Backup()` (vacuumed, gzipped) over a new server-streaming gRPC RPC to a coordinator node, which spools, verifies leader-anchored sha256 digests, and multiplexes files into a tar with a trailing `manifest.json`. Restore reverses this: spool + validate the bundle, flip a raft-replicated `Restoring` flag that stops engines and rejects client ops, `store.Load()` each partition sequentially, re-run migrations, sync definitions, rebuild pointer tables from authoritative `message_subscription` rows, un-gate, return a report.

**Tech Stack:** Go, rqlite v10.2.0 embedded (`store.Backup`/`store.Load`), gRPC streaming (proto edition 2023), chi router (REST), HashiCorp Raft (cluster-state flag), `archive/tar`, `crypto/sha256`, `compress/gzip`, testify.

**Spec:** `docs/superpowers/specs/2026-07-05-cluster-backup-restore-design.md` — read it before starting any task.

## Global Constraints

- Scope: `internal/cluster/**`, `test/e2e/`, `docs/`, `conf/` — plus ONE approved exception: `internal/rest` (REST endpoint shims) and the `server.New` call-site line in `internal/cluster/node.go`. `pkg/bpmn/**`, `pkg/dmn/**`, `pkg/storage/**`, `internal/sql/**` are FORBIDDEN — never edit them (importing/reading `internal/sql` is fine).
- Commits: single-line Conventional Commits with `(cluster)` scope. NEVER add a `Co-Authored-By` trailer or any AI/Claude mention.
- Protobuf: edit `.proto` files, then `make generate`. NEVER hand-edit `*.pb.go` or `zz_generated.deepcopy.go`.
- TDD: write the failing test first for every unit with logic. Use `testify/assert`, co-located `*_test.go`, same package, table-driven where sensible.
- rqlite APIs (verified against v10.2.0): `store.Backup(ctx, *proto.BackupRequest, io.Writer)`, `store.Load(ctx, *proto.LoadRequest)`, `db.IsValidSQLiteData([]byte)`. `LoadChunkRequest` is legacy — do not use.
- The restore bundle is only valid for a cluster with the SAME partition count (hash routing is `% len(partitions)`).
- Build check after every task: `go build ./... && go vet ./internal/cluster/...`.
- **Generated proto field style:** this repo's proto codegen (edition 2023) emits POINTER fields for scalars (existing code writes `&proto.ResolveIncidentRequest{IncidentKey: &key}` and uses `ptr.To(...)`). ALL proto struct literals in this plan's code samples are written with plain values for readability — adapt them to the generated style (`ptr.To(true)`, `&id`, etc., or builder API if generated) exactly as neighboring code in `internal/cluster` does. Getters (`GetX()`) are always safe as written.

## File Structure (locked-in decomposition)

```
internal/cluster/backup/            NEW package — no imports of partition/ or server/ (avoids cycles)
├── manifest.go / manifest_test.go      Manifest schema, JSON round-trip, Validate
├── stream.go / stream_test.go          Partition-side: StreamPartitionBackup, ReceivePartitionRestore
├── bundle.go / bundle_test.go          Coordinator-side: WriteBundle (tar out), OpenBundle (tar in)
├── coordinator.go                      RunClusterBackup / RunClusterRestore orchestration
├── reconcile.go / reconcile_test.go    PlanPointerRebuild, MissingDefinitions (pure logic)
└── report.go                           RestoreReport types
internal/cluster/partition/partition_backup.go / _test.go   DB helpers: SchemaVersion, DataStats,
                                                            ListActiveMessageSubscriptions,
                                                            RebuildMessageSubscriptionPointers,
                                                            ListDefinitionRefs, GetDefinitionResource
internal/cluster/server/backup.go       gRPC handlers (replaces stubs in server.go)
internal/cluster/node_backup.go         ZenNode.ClusterBackup / ClusterRestore (REST entry points)
internal/rest/cluster_backup.go         REST shims (approved exception)
test/e2e/cluster/backup_restore_test.go E2E coverage (build tag cluster_e2e)
```

Modified: `internal/cluster/proto/zen_cluster.proto`, `internal/cluster/command/proto/zencommand.proto`, `internal/cluster/store/fsm.go`, `internal/cluster/store/store.go`, `internal/cluster/state/state.go`, `internal/cluster/controller/controller.go`, `internal/cluster/server/server.go`, `internal/cluster/node.go`, `internal/rest/server.go`, `docs/reference/cluster.md`.

---

## Phase 1 — Backup path

### Task 1: Streaming RPC surface (proto)

**Files:**
- Modify: `internal/cluster/proto/zen_cluster.proto` (rpc lines 38-47, messages at lines 776-790)
- Modify: `internal/cluster/server/server.go:177-200` (stub signatures)

**Interfaces:**
- Produces: gRPC types used by all later tasks — `proto.BackupChunk{Data, Eof, Sha256, SchemaVersion}`, `proto.RestoreChunk{Meta|Data payload, Eof}`, `proto.RestoreMeta{PartitionId, Sha256, SizeBytes, Force}`, `proto.PartitionBackupRequest{PartitionId}`, `proto.PartitionDataStatsRequest/Response`, `proto.MessageSubscriptionRow`, `proto.ListActiveMessageSubscriptionsRequest/Response`, `proto.RebuildMessageSubscriptionPointersRequest/Response`, `proto.DefinitionRef`, `proto.DefinitionType`, `proto.ListDefinitionsRequest/Response`, `proto.GetDefinitionResourceRequest/Response`, `proto.ClusterRestoreResponse{Error, ReportJson}`.

- [ ] **Step 1: Edit the service RPCs.** In `zen_cluster.proto` replace the four backup/restore rpc lines (keep `ConfigurationUpdate`, `AssignPartition`, `UnassignPartition`, pprof lines as they are):

```proto
  rpc ClusterBackup(ClusterBackupRequest) returns (stream BackupChunk);
  rpc ClusterRestore(stream RestoreChunk) returns (ClusterRestoreResponse);
  rpc PartitionBackup(PartitionBackupRequest) returns (stream BackupChunk);
  rpc PartitionRestore(stream RestoreChunk) returns (PartitionRestoreResponse);
  rpc PartitionDataStats(PartitionDataStatsRequest) returns (PartitionDataStatsResponse);
  rpc ListActiveMessageSubscriptions(ListActiveMessageSubscriptionsRequest) returns (ListActiveMessageSubscriptionsResponse);
  rpc RebuildMessageSubscriptionPointers(RebuildMessageSubscriptionPointersRequest) returns (RebuildMessageSubscriptionPointersResponse);
  rpc ListDefinitions(ListDefinitionsRequest) returns (ListDefinitionsResponse);
  rpc GetDefinitionResource(GetDefinitionResourceRequest) returns (GetDefinitionResourceResponse);
```

- [ ] **Step 2: Replace the empty messages** (lines ~776-790) with:

```proto
message PartitionBackupRequest { uint32 partition_id = 1; }

// BackupChunk streams backup bytes. The final chunk has eof=true, empty data,
// and carries the source-side digest + schema version.
message BackupChunk {
  bytes data = 1;
  bool eof = 2;
  string sha256 = 3;         // hex digest of ALL data bytes sent, set when eof
  string schema_version = 4; // latest applied migration filename, set when eof
}

message RestoreMeta {
  uint32 partition_id = 1;
  string sha256 = 2;     // hex digest of the gzipped stream that follows
  int64 size_bytes = 3;
  bool force = 4;        // only meaningful on ClusterRestore
}

// RestoreChunk: first chunk carries meta, subsequent chunks carry data, last has eof=true.
message RestoreChunk {
  oneof payload {
    RestoreMeta meta = 1;
    bytes data = 2;
  }
  bool eof = 3;
}

message PartitionRestoreResponse { ErrorResult error = 1; }
message ClusterBackupRequest {}
message ClusterRestoreResponse {
  ErrorResult error = 1;
  bytes report_json = 2; // JSON-encoded backup.RestoreReport
}

message PartitionDataStatsRequest { uint32 partition_id = 1; }
message PartitionDataStatsResponse {
  ErrorResult error = 1;
  int64 process_definitions = 2;
  int64 process_instances = 3;
}

message MessageSubscriptionRow {
  int64 key = 1;
  string name = 2;
  string correlation_key = 3;
  int64 created_at = 4;
  int64 state = 5;
}
message ListActiveMessageSubscriptionsRequest { uint32 partition_id = 1; }
message ListActiveMessageSubscriptionsResponse {
  ErrorResult error = 1;
  repeated MessageSubscriptionRow rows = 2;
}
message RebuildMessageSubscriptionPointersRequest {
  uint32 partition_id = 1;
  repeated MessageSubscriptionRow pointers = 2;
}
message RebuildMessageSubscriptionPointersResponse { ErrorResult error = 1; }

enum DefinitionType {
  DEFINITION_TYPE_UNKNOWN = 0;
  DEFINITION_TYPE_PROCESS = 1;
  DEFINITION_TYPE_DMN_RESOURCE = 2;
}
message DefinitionRef {
  int64 key = 1;
  DefinitionType type = 2;
}
message ListDefinitionsRequest { uint32 partition_id = 1; }
message ListDefinitionsResponse {
  ErrorResult error = 1;
  repeated DefinitionRef definitions = 2;
}
message GetDefinitionResourceRequest {
  uint32 partition_id = 1;
  int64 key = 2;
  DefinitionType type = 3;
}
message GetDefinitionResourceResponse {
  ErrorResult error = 1;
  bytes data = 2;
  string resource_name = 3;
}
```

Keep `UnassignPartitionRequest/Response`, `AssignPartitionRequest/Response`, `ConfigurationUpdateRequest/Response` as-is.

- [ ] **Step 3: Regenerate.** Run: `make generate`. Expected: `zen_cluster.pb.go` / `zen_cluster_grpc.pb.go` regenerate without errors.

- [ ] **Step 4: Discover the generated streaming signatures.** Run: `grep -n "PartitionBackup\|ClusterBackup\|PartitionRestore\|ClusterRestore\|PartitionDataStats" internal/cluster/proto/zen_cluster_grpc.pb.go | head -30`. Note the exact server-side signatures (either `proto.ZenService_PartitionBackupServer`-style named interfaces or generic `grpc.ServerStreamingServer[proto.BackupChunk]` / `grpc.ClientStreamingServer[proto.RestoreChunk, proto.PartitionRestoreResponse]`, depending on the repo's protoc-gen-go-grpc version). Use those exact signatures in the next step and all later handler tasks.

- [ ] **Step 5: Fix the stubs** in `internal/cluster/server/server.go:177-200` to the new signatures (returning Unimplemented). Example shape (adapt stream types to Step 4's findings):

```go
func (s *Server) ClusterBackup(req *proto.ClusterBackupRequest, stream proto.ZenService_ClusterBackupServer) error {
	return status.Errorf(codes.Unimplemented, "ClusterBackup is not implemented")
}

func (s *Server) ClusterRestore(stream proto.ZenService_ClusterRestoreServer) error {
	return status.Errorf(codes.Unimplemented, "ClusterRestore is not implemented")
}

func (s *Server) PartitionBackup(req *proto.PartitionBackupRequest, stream proto.ZenService_PartitionBackupServer) error {
	return status.Errorf(codes.Unimplemented, "PartitionBackup is not implemented")
}

func (s *Server) PartitionRestore(stream proto.ZenService_PartitionRestoreServer) error {
	return status.Errorf(codes.Unimplemented, "PartitionRestore is not implemented")
}

func (s *Server) PartitionDataStats(ctx context.Context, req *proto.PartitionDataStatsRequest) (*proto.PartitionDataStatsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "PartitionDataStats is not implemented")
}

func (s *Server) ListActiveMessageSubscriptions(ctx context.Context, req *proto.ListActiveMessageSubscriptionsRequest) (*proto.ListActiveMessageSubscriptionsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "ListActiveMessageSubscriptions is not implemented")
}

func (s *Server) RebuildMessageSubscriptionPointers(ctx context.Context, req *proto.RebuildMessageSubscriptionPointersRequest) (*proto.RebuildMessageSubscriptionPointersResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "RebuildMessageSubscriptionPointers is not implemented")
}

func (s *Server) ListDefinitions(ctx context.Context, req *proto.ListDefinitionsRequest) (*proto.ListDefinitionsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "ListDefinitions is not implemented")
}

func (s *Server) GetDefinitionResource(ctx context.Context, req *proto.GetDefinitionResourceRequest) (*proto.GetDefinitionResourceResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "GetDefinitionResource is not implemented")
}
```

- [ ] **Step 6: Verify build.** Run: `go build ./... && go test ./internal/cluster/server/ -count=1`. Expected: PASS (existing tests unaffected).

- [ ] **Step 7: Commit.**

```bash
git add internal/cluster/proto/ internal/cluster/server/server.go
git commit -m "feat(cluster): define streaming backup/restore gRPC surface"
```

---

### Task 2: Manifest

**Files:**
- Create: `internal/cluster/backup/manifest.go`
- Test: `internal/cluster/backup/manifest_test.go`

**Interfaces:**
- Produces: `backup.Manifest{FormatVersion int, ZenBPMVersion string, CreatedAtMillis int64, PartitionCount uint32, Partitions map[uint32]PartitionMeta}`, `backup.PartitionMeta{SnapshotAtMillis int64, SizeBytes int64, SHA256 string, SchemaVersion string}`, `backup.ManifestFileName = "manifest.json"`, `backup.PartitionFileName(id uint32) string` → `"partition-%d.db.gz"`, `(m *Manifest) Validate(clusterPartitionCount uint32, binarySchemaVersion string) error`, `backup.ZenBPMVersion() string`.

- [ ] **Step 1: Write the failing test** `internal/cluster/backup/manifest_test.go`:

```go
package backup

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func validManifest() Manifest {
	return Manifest{
		FormatVersion:   ManifestFormatVersion,
		ZenBPMVersion:   "test",
		CreatedAtMillis: 1000,
		PartitionCount:  2,
		Partitions: map[uint32]PartitionMeta{
			1: {SnapshotAtMillis: 1000, SizeBytes: 10, SHA256: "aa", SchemaVersion: "0007_process_instance_start_element_id.up.sql"},
			2: {SnapshotAtMillis: 1001, SizeBytes: 20, SHA256: "bb", SchemaVersion: "0007_process_instance_start_element_id.up.sql"},
		},
	}
}

func TestManifestJSONRoundTrip(t *testing.T) {
	m := validManifest()
	b, err := json.Marshal(m)
	assert.NoError(t, err)
	var got Manifest
	assert.NoError(t, json.Unmarshal(b, &got))
	assert.Equal(t, m, got)
}

func TestPartitionFileName(t *testing.T) {
	assert.Equal(t, "partition-3.db.gz", PartitionFileName(3))
}

func TestManifestValidate(t *testing.T) {
	binSchema := "0007_process_instance_start_element_id.up.sql"
	tests := []struct {
		name    string
		mutate  func(m *Manifest)
		count   uint32
		wantErr string
	}{
		{name: "valid", mutate: func(m *Manifest) {}, count: 2, wantErr: ""},
		{name: "partition count mismatch", mutate: func(m *Manifest) {}, count: 3, wantErr: "partition count"},
		{name: "wrong format version", mutate: func(m *Manifest) { m.FormatVersion = 99 }, count: 2, wantErr: "format version"},
		{name: "missing partition entry", mutate: func(m *Manifest) { delete(m.Partitions, 2) }, count: 2, wantErr: "missing partition"},
		{name: "empty checksum", mutate: func(m *Manifest) { p := m.Partitions[1]; p.SHA256 = ""; m.Partitions[1] = p }, count: 2, wantErr: "checksum"},
		{name: "schema newer than binary", mutate: func(m *Manifest) { p := m.Partitions[1]; p.SchemaVersion = "9999_future.up.sql"; m.Partitions[1] = p }, count: 2, wantErr: "schema"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := validManifest()
			tt.mutate(&m)
			err := m.Validate(tt.count, binSchema)
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}
```

- [ ] **Step 2: Run to verify failure.** Run: `go test ./internal/cluster/backup/ -v`. Expected: FAIL (package does not exist / undefined symbols).

- [ ] **Step 3: Implement** `internal/cluster/backup/manifest.go`:

```go
// Package backup implements cluster-wide backup and restore of partition
// databases as a streamed tar bundle with a trailing manifest.
package backup

import (
	"fmt"
	"runtime/debug"
	"strings"
)

const (
	ManifestFormatVersion = 1
	ManifestFileName      = "manifest.json"
)

type PartitionMeta struct {
	SnapshotAtMillis int64  `json:"snapshotAtMillis"`
	SizeBytes        int64  `json:"sizeBytes"`
	// SHA256 is the hex digest of the stored (gzipped) partition file bytes,
	// computed on the partition leader while streaming.
	SHA256        string `json:"sha256"`
	SchemaVersion string `json:"schemaVersion"`
}

type Manifest struct {
	FormatVersion   int                      `json:"formatVersion"`
	ZenBPMVersion   string                   `json:"zenbpmVersion"`
	CreatedAtMillis int64                    `json:"createdAtMillis"`
	PartitionCount  uint32                   `json:"partitionCount"`
	Partitions      map[uint32]PartitionMeta `json:"partitions"`
}

func PartitionFileName(id uint32) string {
	return fmt.Sprintf("partition-%d.db.gz", id)
}

// Validate checks that the manifest can be restored into a cluster with
// clusterPartitionCount partitions running a binary whose newest migration
// is binarySchemaVersion. Migration filenames sort lexically (0001_, 0002_, ...).
func (m *Manifest) Validate(clusterPartitionCount uint32, binarySchemaVersion string) error {
	if m.FormatVersion != ManifestFormatVersion {
		return fmt.Errorf("unsupported manifest format version %d (supported: %d)", m.FormatVersion, ManifestFormatVersion)
	}
	if m.PartitionCount != clusterPartitionCount {
		return fmt.Errorf("backup partition count %d does not match cluster partition count %d", m.PartitionCount, clusterPartitionCount)
	}
	for id := uint32(1); id <= m.PartitionCount; id++ {
		meta, ok := m.Partitions[id]
		if !ok {
			return fmt.Errorf("manifest is missing partition %d", id)
		}
		if meta.SHA256 == "" {
			return fmt.Errorf("manifest checksum for partition %d is empty", id)
		}
		if strings.Compare(meta.SchemaVersion, binarySchemaVersion) > 0 {
			return fmt.Errorf("partition %d schema version %q is newer than this binary's %q", id, meta.SchemaVersion, binarySchemaVersion)
		}
	}
	return nil
}

// ZenBPMVersion returns the running module version for the manifest.
func ZenBPMVersion() string {
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" {
		return bi.Main.Version
	}
	return "unknown"
}
```

- [ ] **Step 4: Run tests.** Run: `go test ./internal/cluster/backup/ -v`. Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add internal/cluster/backup/
git commit -m "feat(cluster): add backup bundle manifest with restore validation"
```

---

### Task 3: Partition-side backup streaming + PartitionBackup handler

**Files:**
- Create: `internal/cluster/backup/stream.go`
- Test: `internal/cluster/backup/stream_test.go`
- Create: `internal/cluster/server/backup.go` (move/replace the PartitionBackup stub from server.go into here)
- Create: `internal/cluster/partition/partition_backup.go` (SchemaVersion helper)
- Test: `internal/cluster/partition/partition_backup_test.go`

**Interfaces:**
- Consumes: `proto.BackupChunk` (Task 1).
- Produces: `backup.BackupSource` interface `{ Backup(ctx context.Context, br *rqcmd.BackupRequest, dst io.Writer) error }` (satisfied by `*rqlite/store.Store`); `backup.StreamPartitionBackup(ctx context.Context, src BackupSource, schemaVersion string, send func(*proto.BackupChunk) error) error`; `(rq *partition.DB) SchemaVersion(ctx context.Context) (string, error)`.

- [ ] **Step 1: Write the failing test** `internal/cluster/backup/stream_test.go`:

```go
package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/stretchr/testify/assert"
	rqcmd "github.com/rqlite/rqlite/v10/command/proto"
)

type fakeBackupSource struct {
	payload []byte
	err     error
	gotReq  *rqcmd.BackupRequest
}

func (f *fakeBackupSource) Backup(ctx context.Context, br *rqcmd.BackupRequest, dst io.Writer) error {
	f.gotReq = br
	if f.err != nil {
		return f.err
	}
	_, err := dst.Write(f.payload)
	return err
}

func TestStreamPartitionBackup(t *testing.T) {
	payload := bytes.Repeat([]byte("zen"), 700_000) // > 1 chunk (1 MiB)
	src := &fakeBackupSource{payload: payload}
	var chunks []*proto.BackupChunk
	err := StreamPartitionBackup(context.Background(), src, "0007_x.up.sql", func(c *proto.BackupChunk) error {
		chunks = append(chunks, c)
		return nil
	})
	assert.NoError(t, err)

	// request must ask the leader for a vacuumed, compressed binary copy
	assert.True(t, src.gotReq.Leader)
	assert.True(t, src.gotReq.Vacuum)
	assert.True(t, src.gotReq.Compress)
	assert.Equal(t, rqcmd.BackupRequest_BACKUP_REQUEST_FORMAT_BINARY, src.gotReq.Format)

	// last chunk is the digest trailer
	last := chunks[len(chunks)-1]
	assert.True(t, last.GetEof())
	assert.Empty(t, last.GetData())
	assert.Equal(t, "0007_x.up.sql", last.GetSchemaVersion())
	sum := sha256.Sum256(payload)
	assert.Equal(t, hex.EncodeToString(sum[:]), last.GetSha256())

	// data chunks reassemble the payload and respect the chunk size
	var got []byte
	for _, c := range chunks[:len(chunks)-1] {
		assert.False(t, c.GetEof())
		assert.LessOrEqual(t, len(c.GetData()), backupChunkSize)
		got = append(got, c.GetData()...)
	}
	assert.Equal(t, payload, got)
	assert.Greater(t, len(chunks), 2)
}

func TestStreamPartitionBackupSourceError(t *testing.T) {
	src := &fakeBackupSource{err: errors.New("boom")}
	err := StreamPartitionBackup(context.Background(), src, "s", func(c *proto.BackupChunk) error { return nil })
	assert.ErrorContains(t, err, "boom")
}
```

- [ ] **Step 2: Run to verify failure.** Run: `go test ./internal/cluster/backup/ -run TestStreamPartitionBackup -v`. Expected: FAIL, undefined `StreamPartitionBackup`.

- [ ] **Step 3: Implement** `internal/cluster/backup/stream.go`:

```go
package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"

	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	rqcmd "github.com/rqlite/rqlite/v10/command/proto"
)

// backupChunkSize bounds each gRPC message well below default frame limits.
const backupChunkSize = 1 << 20 // 1 MiB

// BackupSource is the subset of *rqlite/store.Store used for backups.
type BackupSource interface {
	Backup(ctx context.Context, br *rqcmd.BackupRequest, dst io.Writer) error
}

// chunkSendWriter adapts a chunk-send callback into an io.Writer,
// hashing everything written so the digest is anchored at the source.
type chunkSendWriter struct {
	send func(*proto.BackupChunk) error
	hash hash.Hash
}

func (w *chunkSendWriter) Write(p []byte) (int, error) {
	w.hash.Write(p)
	for off := 0; off < len(p); off += backupChunkSize {
		end := min(off+backupChunkSize, len(p))
		// copy: gRPC may retain the buffer past Send
		data := append([]byte(nil), p[off:end]...)
		if err := w.send(&proto.BackupChunk{Data: data}); err != nil {
			return off, err
		}
	}
	return len(p), nil
}

// StreamPartitionBackup produces a vacuumed, gzipped, leader-consistent copy of
// the partition database as a chunk stream, terminated by an eof chunk that
// carries the sha256 of all sent bytes and the partition's schema version.
func StreamPartitionBackup(ctx context.Context, src BackupSource, schemaVersion string, send func(*proto.BackupChunk) error) error {
	w := &chunkSendWriter{send: send, hash: sha256.New()}
	br := &rqcmd.BackupRequest{
		Format:   rqcmd.BackupRequest_BACKUP_REQUEST_FORMAT_BINARY,
		Leader:   true,
		Vacuum:   true,
		Compress: true,
	}
	if err := src.Backup(ctx, br, w); err != nil {
		return fmt.Errorf("partition backup failed: %w", err)
	}
	return send(&proto.BackupChunk{
		Eof:           true,
		Sha256:        hex.EncodeToString(w.hash.Sum(nil)),
		SchemaVersion: schemaVersion,
	})
}
```

Note: if `make generate` produced pointer-style setters (edition 2023 with explicit presence), adapt struct literals accordingly (e.g. `proto.BackupChunk_builder{...}.Build()` or `ptr.To(...)` fields) — mirror how existing messages like `proto.NotifyRequest` are constructed elsewhere in `internal/cluster`.

- [ ] **Step 4: Run tests.** Run: `go test ./internal/cluster/backup/ -v`. Expected: PASS.

- [ ] **Step 5: Write the failing test for `DB.SchemaVersion`** in `internal/cluster/partition/partition_backup_test.go`. Reuse the existing partition DB test setup — open `internal/cluster/partition/partition_persistence_test.go`, find how it constructs a test `*DB` (store + migrations helper used by `TestSchemaReady`), and use the identical setup:

```go
func TestSchemaVersion(t *testing.T) {
	// construct db exactly like partition_persistence_test.go does for its DB tests
	// (same helper/fixture), then:
	version, err := db.SchemaVersion(t.Context())
	assert.NoError(t, err)
	migs, err := sql.GetUpMigrations(db.migrationDir)
	assert.NoError(t, err)
	var latest string
	for _, m := range migs {
		if m.Filename > latest {
			latest = m.Filename
		}
	}
	assert.Equal(t, latest, version)
}
```

- [ ] **Step 6: Run to verify failure**, then implement in `internal/cluster/partition/partition_backup.go`:

```go
package partition

import (
	"context"
	"fmt"
)

// SchemaVersion returns the filename of the newest migration applied to this
// partition's local store. Used to stamp backups and validate restores.
func (rq *DB) SchemaVersion(ctx context.Context) (string, error) {
	migs, err := rq.Queries.GetMigrations(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to read applied migrations: %w", err)
	}
	var latest string
	for _, m := range migs {
		if m.Name > latest {
			latest = m.Name
		}
	}
	return latest, nil
}
```

Run: `go test ./internal/cluster/partition/ -run TestSchemaVersion -v`. Expected: PASS.

- [ ] **Step 7: Implement the gRPC handler.** Delete the `PartitionBackup` stub from `server.go`; create `internal/cluster/server/backup.go` (stream type per Task 1 Step 4):

```go
package server

import (
	"github.com/pbinitiative/zenbpm/internal/cluster/backup"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PartitionBackup streams a point-in-time copy of a locally-led partition to
// the backup coordinator. The final chunk carries the source-side sha256.
func (s *Server) PartitionBackup(req *proto.PartitionBackupRequest, stream proto.ZenService_PartitionBackupServer) error {
	ctx := stream.Context()
	partitionNode := s.controller.GetPartition(ctx, req.GetPartitionId())
	if partitionNode == nil {
		return status.Errorf(codes.NotFound, "partition %d is not hosted on this node", req.GetPartitionId())
	}
	schemaVersion, err := partitionNode.DB.SchemaVersion(ctx)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to read schema version: %s", err)
	}
	err = backup.StreamPartitionBackup(ctx, partitionNode.DB.Store, schemaVersion, stream.Send)
	if err != nil {
		return status.Errorf(codes.Internal, "partition backup failed: %s", err)
	}
	return nil
}
```

- [ ] **Step 8: Verify build + full package tests.** Run: `go build ./... && go test ./internal/cluster/... -count=1`. Expected: PASS.

- [ ] **Step 9: Commit.**

```bash
git add internal/cluster/backup/ internal/cluster/server/ internal/cluster/partition/
git commit -m "feat(cluster): stream partition backups with source-anchored sha256"
```

---

### Task 4: Bundle writer (coordinator tar assembly)

**Files:**
- Create: `internal/cluster/backup/bundle.go`
- Test: `internal/cluster/backup/bundle_test.go`

**Interfaces:**
- Consumes: `Manifest`, `PartitionMeta`, `PartitionFileName`, `ManifestFileName` (Task 2).
- Produces: `backup.FetchResult{SHA256, SchemaVersion string}`; `backup.FetchFunc func(ctx context.Context, partitionID uint32, dst io.Writer) (FetchResult, error)`; `backup.WriteBundle(ctx context.Context, w io.Writer, spoolDir string, partitionIDs []uint32, fetch FetchFunc) (*Manifest, error)`.

Behavior contract: all fetches START concurrently (skew stays seconds); each fetch writes to its own spool file while the coordinator hashes it; coordinator digest must equal the leader-declared `FetchResult.SHA256` or the whole bundle fails; tar entries are written in ascending partition order, each spool file deleted after its entry; `manifest.json` is the LAST tar entry; the archive is a plain (uncompressed) tar.

- [ ] **Step 1: Write the failing test** `internal/cluster/backup/bundle_test.go`:

```go
package backup

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func shaHex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func testFetch(payloads map[uint32][]byte) FetchFunc {
	return func(ctx context.Context, id uint32, dst io.Writer) (FetchResult, error) {
		p, ok := payloads[id]
		if !ok {
			return FetchResult{}, fmt.Errorf("no payload for %d", id)
		}
		if _, err := dst.Write(p); err != nil {
			return FetchResult{}, err
		}
		return FetchResult{SHA256: shaHex(p), SchemaVersion: "0007_x.up.sql"}, nil
	}
}

func TestWriteBundleRoundTrip(t *testing.T) {
	payloads := map[uint32][]byte{1: []byte("partition-one-data"), 2: []byte("partition-two-data")}
	var buf bytes.Buffer
	m, err := WriteBundle(context.Background(), &buf, t.TempDir(), []uint32{1, 2}, testFetch(payloads))
	assert.NoError(t, err)
	assert.Equal(t, uint32(2), m.PartitionCount)

	tr := tar.NewReader(&buf)
	var names []string
	files := map[string][]byte{}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)
		b, _ := io.ReadAll(tr)
		names = append(names, hdr.Name)
		files[hdr.Name] = b
	}
	// partition files in ascending order, manifest last
	assert.Equal(t, []string{"partition-1.db.gz", "partition-2.db.gz", "manifest.json"}, names)
	assert.Equal(t, payloads[1], files["partition-1.db.gz"])
	assert.Equal(t, payloads[2], files["partition-2.db.gz"])

	var parsed Manifest
	assert.NoError(t, json.Unmarshal(files["manifest.json"], &parsed))
	assert.Equal(t, shaHex(payloads[1]), parsed.Partitions[1].SHA256)
	assert.Equal(t, int64(len(payloads[2])), parsed.Partitions[2].SizeBytes)
	assert.Equal(t, "0007_x.up.sql", parsed.Partitions[1].SchemaVersion)
}

func TestWriteBundleDigestMismatchAborts(t *testing.T) {
	lying := func(ctx context.Context, id uint32, dst io.Writer) (FetchResult, error) {
		dst.Write([]byte("actual bytes"))
		return FetchResult{SHA256: "deadbeef", SchemaVersion: "s"}, nil
	}
	var buf bytes.Buffer
	_, err := WriteBundle(context.Background(), &buf, t.TempDir(), []uint32{1}, lying)
	assert.ErrorContains(t, err, "digest mismatch")
}

func TestWriteBundleFetchErrorAborts(t *testing.T) {
	failing := func(ctx context.Context, id uint32, dst io.Writer) (FetchResult, error) {
		return FetchResult{}, errors.New("leader unreachable")
	}
	var buf bytes.Buffer
	_, err := WriteBundle(context.Background(), &buf, t.TempDir(), []uint32{1, 2}, failing)
	assert.ErrorContains(t, err, "leader unreachable")
}
```

- [ ] **Step 2: Run to verify failure.** Run: `go test ./internal/cluster/backup/ -run TestWriteBundle -v`. Expected: FAIL, undefined `WriteBundle`.

- [ ] **Step 3: Implement** in `internal/cluster/backup/bundle.go`:

```go
package backup

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

type FetchResult struct {
	SHA256        string
	SchemaVersion string
}

// FetchFunc streams one partition's backup into dst and returns the
// source-declared digest and schema version.
type FetchFunc func(ctx context.Context, partitionID uint32, dst io.Writer) (FetchResult, error)

type spoolResult struct {
	path   string
	size   int64
	meta   PartitionMeta
	err    error
}

// WriteBundle fans out to all partitions concurrently (bounding snapshot skew
// to seconds), spools each stream to disk while hashing it, verifies the
// coordinator-side digest against the source-declared one, then writes a plain
// tar: partition files in ascending id order, manifest.json last.
func WriteBundle(ctx context.Context, w io.Writer, spoolDir string, partitionIDs []uint32, fetch FetchFunc) (*Manifest, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(map[uint32]chan spoolResult, len(partitionIDs))
	for _, id := range partitionIDs {
		ch := make(chan spoolResult, 1)
		results[id] = ch
		go func(id uint32, ch chan spoolResult) {
			ch <- spoolPartition(ctx, spoolDir, id, fetch)
		}(id, ch)
	}
	// always drain spools so goroutines finish and files get removed on error
	defer func() {
		for _, ch := range results {
			if r, ok := <-ch; ok && r.path != "" {
				os.Remove(r.path)
			}
			// second receive never happens; channel is buffered size 1
		}
	}()

	manifest := &Manifest{
		FormatVersion:   ManifestFormatVersion,
		ZenBPMVersion:   ZenBPMVersion(),
		CreatedAtMillis: time.Now().UnixMilli(),
		PartitionCount:  uint32(len(partitionIDs)),
		Partitions:      make(map[uint32]PartitionMeta, len(partitionIDs)),
	}

	tw := tar.NewWriter(w)
	for _, id := range partitionIDs {
		r := <-results[id]
		results[id] = closedResult(r) // keep deferred cleanup happy after consume
		if r.err != nil {
			return nil, fmt.Errorf("backup of partition %d failed: %w", id, r.err)
		}
		if err := writeSpoolEntry(tw, PartitionFileName(id), r.path, r.size); err != nil {
			return nil, err
		}
		os.Remove(r.path)
		r.path = ""
		manifest.Partitions[id] = r.meta
	}

	mb, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal manifest: %w", err)
	}
	if err := tw.WriteHeader(&tar.Header{Name: ManifestFileName, Mode: 0o600, Size: int64(len(mb))}); err != nil {
		return nil, fmt.Errorf("failed to write manifest header: %w", err)
	}
	if _, err := tw.Write(mb); err != nil {
		return nil, fmt.Errorf("failed to write manifest: %w", err)
	}
	if err := tw.Close(); err != nil {
		return nil, fmt.Errorf("failed to finalize bundle: %w", err)
	}
	return manifest, nil
}

func closedResult(r spoolResult) chan spoolResult {
	ch := make(chan spoolResult, 1)
	r.path = "" // already handled by caller
	ch <- r
	return ch
}

func spoolPartition(ctx context.Context, spoolDir string, id uint32, fetch FetchFunc) spoolResult {
	f, err := os.CreateTemp(spoolDir, fmt.Sprintf("zenbpm-backup-p%d-*", id))
	if err != nil {
		return spoolResult{err: fmt.Errorf("failed to create spool file: %w", err)}
	}
	defer f.Close()
	snapshotAt := time.Now().UnixMilli()
	h := sha256.New()
	res, err := fetch(ctx, id, io.MultiWriter(f, h))
	if err != nil {
		os.Remove(f.Name())
		return spoolResult{err: err}
	}
	got := hex.EncodeToString(h.Sum(nil))
	if got != res.SHA256 {
		os.Remove(f.Name())
		return spoolResult{err: fmt.Errorf("digest mismatch for partition %d: source declared %s, coordinator computed %s", id, res.SHA256, got)}
	}
	info, err := f.Stat()
	if err != nil {
		os.Remove(f.Name())
		return spoolResult{err: err}
	}
	return spoolResult{
		path: f.Name(),
		size: info.Size(),
		meta: PartitionMeta{
			SnapshotAtMillis: snapshotAt,
			SizeBytes:        info.Size(),
			SHA256:           res.SHA256,
			SchemaVersion:    res.SchemaVersion,
		},
	}
}

func writeSpoolEntry(tw *tar.Writer, name, path string, size int64) error {
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: size}); err != nil {
		return fmt.Errorf("failed to write tar header for %s: %w", name, err)
	}
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to reopen spool %s: %w", filepath.Base(path), err)
	}
	defer f.Close()
	if _, err := io.Copy(tw, f); err != nil {
		return fmt.Errorf("failed to copy %s into bundle: %w", name, err)
	}
	return nil
}
```

If the double-receive-guard bookkeeping (`closedResult`) feels awkward during implementation, an equivalent structure with a `sync.WaitGroup` plus a results map guarded by the goroutines completing is fine — the contract in the tests is what matters.

- [ ] **Step 4: Run tests.** Run: `go test ./internal/cluster/backup/ -v -count=1`. Expected: PASS (all bundle + manifest + stream tests).

- [ ] **Step 5: Commit.**

```bash
git add internal/cluster/backup/
git commit -m "feat(cluster): assemble backup bundle tar with digest verification"
```

---

### Task 5: Backup coordinator wiring — ZenNode, gRPC ClusterBackup, REST endpoint

**Files:**
- Create: `internal/cluster/backup/coordinator.go`
- Modify: `internal/cluster/server/server.go` (struct + `New` signature: add client manager; delete ClusterBackup stub)
- Modify: `internal/cluster/server/backup.go` (add ClusterBackup handler)
- Modify: `internal/cluster/node.go:128` (pass `node.client` to `server.New`)
- Create: `internal/cluster/node_backup.go`
- Create: `internal/rest/cluster_backup.go`
- Modify: `internal/rest/server.go` (register routes inside the existing `r.Route("/v1", ...)` block, BEFORE `r.Mount("/", h)`)

**Interfaces:**
- Consumes: `WriteBundle`, `FetchFunc`, `StreamPartitionBackup` chunk protocol (final chunk carries sha256+schema), `client.ClientManager.PartitionLeader(partition uint32) (proto.ZenServiceClient, error)`.
- Produces: `backup.ClientProvider` interface `{ PartitionLeader(partition uint32) (proto.ZenServiceClient, error) }`; `backup.RunClusterBackup(ctx context.Context, cs state.Cluster, clients ClientProvider, spoolDir string, w io.Writer) (*Manifest, error)`; `(node *cluster.ZenNode) ClusterBackup(ctx context.Context, w io.Writer) (*backup.Manifest, error)`; REST `GET /v1/cluster/backup`.

- [ ] **Step 1: Implement `RunClusterBackup`** in `internal/cluster/backup/coordinator.go`:

```go
package backup

import (
	"context"
	"fmt"
	"io"
	"sort"

	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
)

// ClientProvider is the subset of client.ClientManager the coordinator needs.
type ClientProvider interface {
	PartitionLeader(partition uint32) (proto.ZenServiceClient, error)
}

// RunClusterBackup checks that every partition has a leader, then streams a
// bundle of all partition backups into w.
func RunClusterBackup(ctx context.Context, cs state.Cluster, clients ClientProvider, spoolDir string, w io.Writer) (*Manifest, error) {
	if len(cs.Partitions) == 0 {
		return nil, fmt.Errorf("cluster has no partitions")
	}
	ids := make([]uint32, 0, len(cs.Partitions))
	for id, p := range cs.Partitions {
		if p.LeaderId == "" {
			return nil, fmt.Errorf("partition %d has no leader; refusing to start backup", id)
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	return WriteBundle(ctx, w, spoolDir, ids, fetchFromLeader(clients))
}

func fetchFromLeader(clients ClientProvider) FetchFunc {
	return func(ctx context.Context, id uint32, dst io.Writer) (FetchResult, error) {
		leader, err := clients.PartitionLeader(id)
		if err != nil {
			return FetchResult{}, fmt.Errorf("failed to get leader client for partition %d: %w", id, err)
		}
		stream, err := leader.PartitionBackup(ctx, &proto.PartitionBackupRequest{PartitionId: id})
		if err != nil {
			return FetchResult{}, fmt.Errorf("failed to open backup stream for partition %d: %w", id, err)
		}
		for {
			chunk, err := stream.Recv()
			if err != nil {
				return FetchResult{}, fmt.Errorf("backup stream for partition %d failed: %w", id, err)
			}
			if chunk.GetEof() {
				return FetchResult{SHA256: chunk.GetSha256(), SchemaVersion: chunk.GetSchemaVersion()}, nil
			}
			if _, err := dst.Write(chunk.GetData()); err != nil {
				return FetchResult{}, err
			}
		}
	}
}
```

(Adapt proto struct literals to the generated style, as in Task 3.)

- [ ] **Step 2: Give the gRPC server a client manager.** In `internal/cluster/server/server.go`: `New` already stores a `client *client.ClientManager` field — extend the constructor and its call site:

```go
func New(ln net.Listener, store StoreService, controller ControllerService, jobManager *jobmanager.JobManager, clientMgr *client.ClientManager) *Server {
	return &Server{
		ln:         ln,
		addr:       ln.Addr(),
		store:      store,
		controller: controller,
		jobManager: jobManager,
		client:     clientMgr,
	}
}
```

In `internal/cluster/node.go:128` change the call to `server.New(clusterSrvLn, node.store, node.controller, node.JobManager, node.client)` (the client manager is created at node.go:118, before the server — verify the ordering still holds when editing). Fix any other `server.New` call sites: `grep -rn "server.New(" internal/ test/`.

- [ ] **Step 3: Implement the gRPC ClusterBackup handler** (delete its stub from server.go; add to `internal/cluster/server/backup.go`):

```go
// clusterBackupChunkWriter adapts the ClusterBackup stream into an io.Writer
// carrying raw tar bytes.
type clusterBackupChunkWriter struct {
	send func(*proto.BackupChunk) error
}

func (w *clusterBackupChunkWriter) Write(p []byte) (int, error) {
	data := append([]byte(nil), p...)
	if err := w.send(&proto.BackupChunk{Data: data}); err != nil {
		return 0, err
	}
	return len(p), nil
}

// ClusterBackup streams the whole-cluster backup bundle (tar) to a gRPC client.
func (s *Server) ClusterBackup(req *proto.ClusterBackupRequest, stream proto.ZenService_ClusterBackupServer) error {
	spoolDir, err := os.MkdirTemp("", "zenbpm-backup-*")
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create spool dir: %s", err)
	}
	defer os.RemoveAll(spoolDir)
	w := &clusterBackupChunkWriter{send: stream.Send}
	if _, err := backup.RunClusterBackup(stream.Context(), s.store.ClusterState(), s.client, spoolDir, w); err != nil {
		return status.Errorf(codes.Internal, "cluster backup failed: %s", err)
	}
	return stream.Send(&proto.BackupChunk{Eof: true})
}
```

- [ ] **Step 4: ZenNode entry point** — create `internal/cluster/node_backup.go`:

```go
package cluster

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/pbinitiative/zenbpm/internal/cluster/backup"
)

// ClusterBackup streams a whole-cluster backup bundle (plain tar) into w.
// Callable on any node; partition data is pulled from each partition leader.
func (node *ZenNode) ClusterBackup(ctx context.Context, w io.Writer) (*backup.Manifest, error) {
	spoolDir, err := os.MkdirTemp("", "zenbpm-backup-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create backup spool dir: %w", err)
	}
	defer os.RemoveAll(spoolDir)
	return backup.RunClusterBackup(ctx, node.store.ClusterState(), node.client, spoolDir, w)
}
```

- [ ] **Step 5: REST endpoint** — create `internal/rest/cluster_backup.go`:

```go
package rest

import (
	"fmt"
	"net/http"
	"time"

	"github.com/pbinitiative/zenbpm/internal/log"
)

// handleClusterBackup streams the backup bundle. Errors after the first byte
// surface as a truncated tar (no manifest), which restore rejects.
func (s *Server) handleClusterBackup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/x-tar")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="zenbpm-backup-%d.tar"`, time.Now().Unix()))
	if _, err := s.node.ClusterBackup(r.Context(), w); err != nil {
		log.Error("cluster backup failed: %s", err)
	}
}
```

Register in `internal/rest/server.go` inside `r.Route("/v1", func(r chi.Router) { ... })`, ABOVE the `r.Mount("/", h)` line (chi prefers static routes over the mount wildcard):

```go
		r.Get("/cluster/backup", s.handleClusterBackup)
```

- [ ] **Step 6: Verify build + vet + existing tests.** Run: `go build ./... && go vet ./internal/cluster/... ./internal/rest/ && go test ./internal/cluster/... ./internal/rest/... -count=1`. Expected: PASS. (End-to-end behavior is covered in Task 14.)

- [ ] **Step 7: Commit.**

```bash
git add internal/cluster/ internal/rest/
git commit -m "feat(cluster): wire cluster backup coordinator with REST and gRPC endpoints"
```

---

## Phase 2 — Restore path

### Task 6: Bundle reader + validation

**Files:**
- Modify: `internal/cluster/backup/bundle.go`
- Test: `internal/cluster/backup/bundle_test.go`

**Interfaces:**
- Produces: `backup.Bundle{Manifest Manifest}` with `(b *Bundle) PartitionFile(id uint32) (io.ReadCloser, error)` (gzipped bytes as stored) and `(b *Bundle) Close() error`; `backup.OpenBundle(r io.Reader, spoolDir string) (*Bundle, error)`.

Behavior contract: reads the tar stream sequentially, spooling each `partition-N.db.gz` to `spoolDir` while hashing; requires `manifest.json` as an entry (conventionally last); after reading, verifies per-partition: file present ⇔ manifest entry present, sha256 matches, size matches, and the gunzipped content starts with the SQLite magic (`SQLite format 3\x00`). A truncated tar (no manifest) must fail with a clear error.

- [ ] **Step 1: Write the failing tests** (append to `bundle_test.go`):

```go
func gzipBytes(t *testing.T, raw []byte) []byte {
	var b bytes.Buffer
	zw := gzip.NewWriter(&b)
	_, err := zw.Write(raw)
	assert.NoError(t, err)
	assert.NoError(t, zw.Close())
	return b.Bytes()
}

func sqliteish(t *testing.T, tail string) []byte {
	return append([]byte("SQLite format 3\x00"), []byte(tail)...)
}

func TestOpenBundleRoundTrip(t *testing.T) {
	payloads := map[uint32][]byte{
		1: gzipBytes(t, sqliteish(t, "one")),
		2: gzipBytes(t, sqliteish(t, "two")),
	}
	var buf bytes.Buffer
	_, err := WriteBundle(context.Background(), &buf, t.TempDir(), []uint32{1, 2}, testFetch(payloads))
	assert.NoError(t, err)

	b, err := OpenBundle(&buf, t.TempDir())
	assert.NoError(t, err)
	defer b.Close()
	assert.Equal(t, uint32(2), b.Manifest.PartitionCount)

	rc, err := b.PartitionFile(2)
	assert.NoError(t, err)
	got, _ := io.ReadAll(rc)
	rc.Close()
	assert.Equal(t, payloads[2], got)
}

func TestOpenBundleTruncated(t *testing.T) {
	payloads := map[uint32][]byte{1: gzipBytes(t, sqliteish(t, "one"))}
	var buf bytes.Buffer
	_, err := WriteBundle(context.Background(), &buf, t.TempDir(), []uint32{1}, testFetch(payloads))
	assert.NoError(t, err)
	trunc := buf.Bytes()[:buf.Len()-600] // cut into/before the manifest entry
	_, err = OpenBundle(bytes.NewReader(trunc), t.TempDir())
	assert.Error(t, err)
}

func TestOpenBundleCorruptedPartitionFile(t *testing.T) {
	payloads := map[uint32][]byte{1: gzipBytes(t, sqliteish(t, "one"))}
	var buf bytes.Buffer
	_, err := WriteBundle(context.Background(), &buf, t.TempDir(), []uint32{1}, testFetch(payloads))
	assert.NoError(t, err)
	raw := buf.Bytes()
	// flip a byte inside the partition file body (first entry data starts at 512)
	raw[600] ^= 0xFF
	_, err = OpenBundle(bytes.NewReader(raw), t.TempDir())
	assert.ErrorContains(t, err, "checksum")
}

func TestOpenBundleNotSQLite(t *testing.T) {
	payloads := map[uint32][]byte{1: gzipBytes(t, []byte("definitely not a database"))}
	var buf bytes.Buffer
	_, err := WriteBundle(context.Background(), &buf, t.TempDir(), []uint32{1}, testFetch(payloads))
	assert.NoError(t, err)
	_, err = OpenBundle(bytes.NewReader(buf.Bytes()), t.TempDir())
	assert.ErrorContains(t, err, "not a valid SQLite")
}
```

Add imports `compress/gzip` to the test file.

- [ ] **Step 2: Run to verify failure.** Run: `go test ./internal/cluster/backup/ -run TestOpenBundle -v`. Expected: FAIL, undefined `OpenBundle`.

- [ ] **Step 3: Implement** (append to `bundle.go`):

```go
type Bundle struct {
	Manifest Manifest
	files    map[uint32]string // partition id -> spooled gz file path
}

// OpenBundle spools a bundle stream to disk and fully validates it BEFORE any
// destructive restore step: manifest present, every partition file present
// with matching sha256 and size, and gunzipped content that looks like SQLite.
func OpenBundle(r io.Reader, spoolDir string) (*Bundle, error) {
	b := &Bundle{files: map[uint32]string{}}
	tr := tar.NewReader(r)
	shas := map[uint32]string{}
	sizes := map[uint32]int64{}
	manifestSeen := false
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			b.Close()
			return nil, fmt.Errorf("failed to read bundle (truncated or corrupt tar): %w", err)
		}
		var id uint32
		if hdr.Name == ManifestFileName {
			if err := json.NewDecoder(tr).Decode(&b.Manifest); err != nil {
				b.Close()
				return nil, fmt.Errorf("failed to parse manifest: %w", err)
			}
			manifestSeen = true
			continue
		}
		if _, err := fmt.Sscanf(hdr.Name, "partition-%d.db.gz", &id); err != nil {
			b.Close()
			return nil, fmt.Errorf("unexpected bundle entry %q", hdr.Name)
		}
		f, err := os.CreateTemp(spoolDir, fmt.Sprintf("zenbpm-restore-p%d-*", id))
		if err != nil {
			b.Close()
			return nil, fmt.Errorf("failed to create restore spool: %w", err)
		}
		h := sha256.New()
		n, err := io.Copy(io.MultiWriter(f, h), tr)
		f.Close()
		if err != nil {
			b.Close()
			return nil, fmt.Errorf("failed to spool %s: %w", hdr.Name, err)
		}
		b.files[id] = f.Name()
		shas[id] = hex.EncodeToString(h.Sum(nil))
		sizes[id] = n
	}
	if !manifestSeen {
		b.Close()
		return nil, fmt.Errorf("bundle has no %s (incomplete backup?)", ManifestFileName)
	}
	for id, meta := range b.Manifest.Partitions {
		if _, ok := b.files[id]; !ok {
			b.Close()
			return nil, fmt.Errorf("bundle is missing file for partition %d", id)
		}
		if shas[id] != meta.SHA256 {
			b.Close()
			return nil, fmt.Errorf("checksum mismatch for partition %d: manifest %s, bundle %s", id, meta.SHA256, shas[id])
		}
		if sizes[id] != meta.SizeBytes {
			b.Close()
			return nil, fmt.Errorf("size mismatch for partition %d", id)
		}
		if err := verifySQLiteGzip(b.files[id]); err != nil {
			b.Close()
			return nil, fmt.Errorf("partition %d: %w", id, err)
		}
	}
	for id := range b.files {
		if _, ok := b.Manifest.Partitions[id]; !ok {
			b.Close()
			return nil, fmt.Errorf("bundle contains partition %d not listed in manifest", id)
		}
	}
	return b, nil
}

func verifySQLiteGzip(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("not gzip data: %w", err)
	}
	defer zr.Close()
	head := make([]byte, 16)
	if _, err := io.ReadFull(zr, head); err != nil {
		return fmt.Errorf("failed to read database header: %w", err)
	}
	if string(head) != "SQLite format 3\x00" {
		return fmt.Errorf("content is not a valid SQLite database")
	}
	// drain to let gzip verify its CRC over the whole stream
	if _, err := io.Copy(io.Discard, zr); err != nil {
		return fmt.Errorf("gzip stream corrupt: %w", err)
	}
	return nil
}

func (b *Bundle) PartitionFile(id uint32) (io.ReadCloser, error) {
	path, ok := b.files[id]
	if !ok {
		return nil, fmt.Errorf("no file for partition %d", id)
	}
	return os.Open(path)
}

func (b *Bundle) Close() error {
	for _, p := range b.files {
		os.Remove(p)
	}
	return nil
}
```

Add imports `compress/gzip` to bundle.go.

- [ ] **Step 4: Run tests.** Run: `go test ./internal/cluster/backup/ -v -count=1`. Expected: PASS. (If the truncation offset in `TestOpenBundleTruncated` happens to still parse, cut more bytes — the assertion is just "must error".)

- [ ] **Step 5: Commit.**

```bash
git add internal/cluster/backup/
git commit -m "feat(cluster): validate and spool restore bundles before any destructive step"
```

---

### Task 7: Raft-replicated Restoring flag

**Files:**
- Modify: `internal/cluster/command/proto/zencommand.proto`
- Modify: `internal/cluster/state/state.go` (Cluster struct)
- Modify: `internal/cluster/store/fsm.go` (Apply dispatch)
- Modify: `internal/cluster/store/store.go` (WriteMaintenanceChange)
- Test: `internal/cluster/store/store_test.go` (append)

**Interfaces:**
- Produces: `protoc.ClusterMaintenanceChange{Restoring bool}`, `protoc.Command_TYPE_CLUSTER_MAINTENANCE_CHANGE`; `state.Cluster.Restoring bool`; `(s *store.Store) WriteMaintenanceChange(change *protoc.ClusterMaintenanceChange) error`.

- [ ] **Step 1: Write the failing test** (append to `internal/cluster/store/store_test.go`, using the existing `newMustTestStore` helper):

```go
func TestWriteMaintenanceChange(t *testing.T) {
	s := newMustTestStore(t) // adapt to the helper's exact signature in this file
	assert.False(t, s.ClusterState().Restoring)

	err := s.WriteMaintenanceChange(&proto.ClusterMaintenanceChange{Restoring: true})
	assert.NoError(t, err)
	assert.True(t, s.ClusterState().Restoring)

	err = s.WriteMaintenanceChange(&proto.ClusterMaintenanceChange{Restoring: false})
	assert.NoError(t, err)
	assert.False(t, s.ClusterState().Restoring)
}
```

(Match the import alias for command proto used in that test file, and how the helper waits for leadership — copy the pattern from an adjacent `WriteNodeChange`/`WritePartitionChange` test if one exists.)

- [ ] **Step 2: Edit `zencommand.proto`:**

```proto
message Command {
  enum Type {
    TYPE_UNKNOWN = 0;
    TYPE_NOOP = 1;
    TYPE_NODE_CHANGE = 2;
    TYPE_NODE_PARTITION_CHANGE = 3;
    TYPE_CLUSTER_MAINTENANCE_CHANGE = 4;
  }
  Type type = 1;
  oneof request {
    NodeChange node_change = 2;
    NodePartitionChange node_partition_change = 3;
    ClusterMaintenanceChange cluster_maintenance_change = 4;
  }
}

// ClusterMaintenanceChange flips cluster-wide maintenance state.
// restoring=true gates all engine traffic while a restore is in progress.
message ClusterMaintenanceChange {
  bool restoring = 1;
}
```

- [ ] **Step 3: Add the state field** in `internal/cluster/state/state.go` (Cluster struct):

```go
	// Restoring is true while a cluster restore is in progress. Engines are
	// stopped and client-facing operations are rejected until it clears.
	Restoring bool `json:"restoring"`
```

- [ ] **Step 4: Regenerate.** Run: `make generate`. Expected: `zencommand.pb.go` and `zz_generated.deepcopy.go` regenerate cleanly.

- [ ] **Step 5: FSM dispatch** — in `internal/cluster/store/fsm.go` `Apply` switch add:

```go
	case proto.Command_TYPE_CLUSTER_MAINTENANCE_CHANGE:
		res = f.applyMaintenanceChange(command.GetClusterMaintenanceChange())
```

and below the other apply methods:

```go
func (f *FSM) applyMaintenanceChange(cmd *proto.ClusterMaintenanceChange) interface{} {
	f.store.stateMu.Lock()
	defer f.store.stateMu.Unlock()
	newState := *f.store.state.DeepCopy()
	newState.Restoring = cmd.GetRestoring()
	f.store.state = newState
	return nil
}
```

- [ ] **Step 6: Store writer** — in `internal/cluster/store/store.go`, mirror `WriteNodeChange` (store.go:159):

```go
// WriteMaintenanceChange replicates a cluster maintenance flag change
// (e.g. restore-in-progress) through the raft log.
func (s *Store) WriteMaintenanceChange(change *proto.ClusterMaintenanceChange) error {
	command := &proto.Command{
		Type: proto.Command_TYPE_CLUSTER_MAINTENANCE_CHANGE.Enum(),
		Request: &proto.Command_ClusterMaintenanceChange{
			ClusterMaintenanceChange: change,
		},
	}
	b, err := pb.Marshal(command)
	if err != nil {
		return fmt.Errorf("failed to marshal ClusterMaintenanceChange message before applying to log: %w", err)
	}
	f := s.raft.Apply(b, s.cfg.RaftTimeout)
	if f.Error() != nil && f.Response() != nil {
		return fmt.Errorf("failed to apply ClusterMaintenanceChange message to raft log: %w", f.Error())
	}
	return nil
}
```

- [ ] **Step 7: Run the test.** Run: `go test ./internal/cluster/store/ -run TestWriteMaintenanceChange -v`. Expected: PASS. Then full: `go test ./internal/cluster/... -count=1`. Expected: PASS.

- [ ] **Step 8: Commit.**

```bash
git add internal/cluster/command/ internal/cluster/state/ internal/cluster/store/
git commit -m "feat(cluster): add raft-replicated cluster restoring flag"
```

---

### Task 8: Gate enforcement — stop engines, reject client ops

**Files:**
- Modify: `internal/cluster/controller/controller.go` (`performMemberOperations`, controller.go:188)
- Modify: `internal/cluster/node.go` (guard on mutating ZenNode methods)
- Test: `internal/cluster/controller/controller_test.go` (append, following existing controller test patterns)

**Interfaces:**
- Consumes: `state.Cluster.Restoring` (Task 7), `stopEngineIfRunning(partitionNode *partition.ZenPartitionNode, partitionId uint32, reason string)` (controller.go:440).
- Produces: `(node *ZenNode) rejectIfRestoring() error` used by all mutating node methods.

- [ ] **Step 1: Controller reaction.** At the very top of `performMemberOperations` (controller.go:188) add:

```go
	if c.store.ClusterState().Restoring {
		c.partitionsMu.RLock()
		local := make(map[uint32]*partition.ZenPartitionNode, len(c.partitions))
		for id, pn := range c.partitions {
			local[id] = pn
		}
		c.partitionsMu.RUnlock()
		for id, pn := range local {
			c.stopEngineIfRunning(pn, id, "cluster restore in progress")
		}
		return
	}
```

When the flag clears, the flag-clear raft apply triggers `ClusterStateChangeNotification` → `performMemberOperations` runs its normal flow → `handlePartitionStateInitialized` → `startEngineIfLeader` restarts engines. No extra code needed for restart — verify this reasoning against `handlePartitionStateInitialized` (controller.go:393) while implementing; if the initialized-handler short-circuits when state hasn't changed, also call `startEngineIfLeader` from the flag-clear path.

- [ ] **Step 2: Add a controller test.** Follow the construction pattern of existing tests in `controller_test.go` (they build a controller against a test store). The test flips `Restoring` in the store state, calls `ClusterStateChangeNotification`, and asserts the engine was stopped. If existing test fixtures make engine assertions impractical, assert indirectly: `PartitionEngine(ctx, id)` returns nil / stopped marker after the notification. If the fixture cannot express this at all, document why in the test file and rely on Task 14's E2E — do not silently skip.

- [ ] **Step 3: ZenNode guard.** In `internal/cluster/node.go` add:

```go
// rejectIfRestoring blocks client-facing mutations while a cluster restore is
// in progress.
func (node *ZenNode) rejectIfRestoring() error {
	if node.store.ClusterState().Restoring {
		return zenerr.ClusterError(fmt.Errorf("cluster restore in progress; try again later"))
	}
	return nil
}
```

Enumerate mutating methods: `grep -n "^func (node \*ZenNode)" internal/cluster/node.go`. Add the guard as the FIRST statement to every method that deploys, creates, publishes, completes, fails, resolves, cancels, modifies, deletes, or assigns (i.e. everything that routes a write to a partition leader — deploy definition, create instance, publish message, job complete/fail/assign/reassign, incident resolve, instance modify/cancel, variable delete). Read-only Get* methods stay unguarded. Canonical example (node.go:738):

```go
func (node *ZenNode) PublishMessage(ctx context.Context, name string, correlationKey *string, variables map[string]any) error {
	if err := node.rejectIfRestoring(); err != nil {
		return err
	}
	if correlationKey == nil {
		return node.publishDefinitionMessageByName(ctx, name, variables)
	}
	return node.publishCorrelatedMessageByName(ctx, name, *correlationKey, variables)
}
```

- [ ] **Step 4: Verify.** Run: `go build ./... && go test ./internal/cluster/... -count=1`. Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add internal/cluster/controller/ internal/cluster/node.go
git commit -m "feat(cluster): gate engine traffic while cluster restore is in progress"
```

---

### Task 9: Partition-side restore — receive, verify, Load, re-migrate

**Files:**
- Modify: `internal/cluster/backup/stream.go`
- Test: `internal/cluster/backup/stream_test.go`
- Modify: `internal/cluster/server/backup.go` (PartitionRestore handler; delete stub from server.go)

**Interfaces:**
- Consumes: `proto.RestoreChunk`/`proto.RestoreMeta` (Task 1), `(rq *DB) RunMigrations(ctx)` (exists, migrations.go:15).
- Produces: `backup.LoadTarget` interface `{ Load(ctx context.Context, lr *rqcmd.LoadRequest) error }` (satisfied by `*rqlite/store.Store`); `backup.ReceivePartitionRestore(ctx context.Context, spoolDir string, meta *proto.RestoreMeta, recv func() (*proto.RestoreChunk, error), dst LoadTarget) error`.

Behavior contract: spool incoming data chunks to disk while hashing; on eof verify sha256 against `meta.Sha256`; gunzip into memory; check `SQLite format 3\x00` magic; call `dst.Load(&rqcmd.LoadRequest{Data: raw})`.

- [ ] **Step 1: Write the failing test** (append to `stream_test.go`):

```go
type fakeLoadTarget struct {
	loaded []byte
	err    error
}

func (f *fakeLoadTarget) Load(ctx context.Context, lr *rqcmd.LoadRequest) error {
	f.loaded = lr.Data
	return f.err
}

func chunkFeed(meta *proto.RestoreMeta, data []byte, chunk int) func() (*proto.RestoreChunk, error) {
	sent, metaSent, eofSent := 0, false, false
	return func() (*proto.RestoreChunk, error) {
		if !metaSent {
			metaSent = true
			return &proto.RestoreChunk{Payload: &proto.RestoreChunk_Meta{Meta: meta}}, nil
		}
		if sent < len(data) {
			end := min(sent+chunk, len(data))
			c := &proto.RestoreChunk{Payload: &proto.RestoreChunk_Data{Data: data[sent:end]}}
			sent = end
			return c, nil
		}
		if !eofSent {
			eofSent = true
			return &proto.RestoreChunk{Eof: true}, nil
		}
		return nil, io.EOF
	}
}

func TestReceivePartitionRestore(t *testing.T) {
	raw := append([]byte("SQLite format 3\x00"), bytes.Repeat([]byte("d"), 5000)...)
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	zw.Write(raw)
	zw.Close()
	sum := sha256.Sum256(gz.Bytes())
	meta := &proto.RestoreMeta{PartitionId: 1, Sha256: hex.EncodeToString(sum[:]), SizeBytes: int64(gz.Len())}

	dst := &fakeLoadTarget{}
	err := ReceivePartitionRestore(context.Background(), t.TempDir(), meta, chunkFeed(meta, gz.Bytes(), 1024), dst)
	assert.NoError(t, err)
	assert.Equal(t, raw, dst.loaded)
}

func TestReceivePartitionRestoreBadDigest(t *testing.T) {
	raw := append([]byte("SQLite format 3\x00"), []byte("data")...)
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	zw.Write(raw)
	zw.Close()
	meta := &proto.RestoreMeta{PartitionId: 1, Sha256: "deadbeef", SizeBytes: int64(gz.Len())}
	dst := &fakeLoadTarget{}
	err := ReceivePartitionRestore(context.Background(), t.TempDir(), meta, chunkFeed(meta, gz.Bytes(), 1024), dst)
	assert.ErrorContains(t, err, "digest mismatch")
	assert.Nil(t, dst.loaded)
}

func TestReceivePartitionRestoreNotSQLite(t *testing.T) {
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	zw.Write([]byte("not a database at all"))
	zw.Close()
	sum := sha256.Sum256(gz.Bytes())
	meta := &proto.RestoreMeta{PartitionId: 1, Sha256: hex.EncodeToString(sum[:]), SizeBytes: int64(gz.Len())}
	dst := &fakeLoadTarget{}
	err := ReceivePartitionRestore(context.Background(), t.TempDir(), meta, chunkFeed(meta, gz.Bytes(), 1024), dst)
	assert.ErrorContains(t, err, "not a valid SQLite")
	assert.Nil(t, dst.loaded)
}
```

(The first recv returning meta again is fine to tolerate or reject — the function receives `meta` separately because the gRPC handler reads the first chunk itself; the feed above starts with meta to test that tolerance. Add imports: `compress/gzip`.)

- [ ] **Step 2: Run to verify failure.** Run: `go test ./internal/cluster/backup/ -run TestReceivePartitionRestore -v`. Expected: FAIL.

- [ ] **Step 3: Implement** (append to `stream.go`):

```go
// LoadTarget is the subset of *rqlite/store.Store used for restores.
type LoadTarget interface {
	Load(ctx context.Context, lr *rqcmd.LoadRequest) error
}

// ReceivePartitionRestore spools the incoming gzipped stream, verifies its
// digest against meta, gunzips and validates the SQLite image, then loads it
// through the partition's raft log.
func ReceivePartitionRestore(ctx context.Context, spoolDir string, meta *proto.RestoreMeta, recv func() (*proto.RestoreChunk, error), dst LoadTarget) error {
	spool, err := os.CreateTemp(spoolDir, fmt.Sprintf("zenbpm-restore-recv-p%d-*", meta.GetPartitionId()))
	if err != nil {
		return fmt.Errorf("failed to create restore spool: %w", err)
	}
	defer os.Remove(spool.Name())
	defer spool.Close()

	h := sha256.New()
	w := io.MultiWriter(spool, h)
recvLoop:
	for {
		chunk, err := recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("restore stream failed: %w", err)
		}
		switch p := chunk.GetPayload().(type) {
		case *proto.RestoreChunk_Meta:
			// tolerated duplicate of the header chunk; ignore
		case *proto.RestoreChunk_Data:
			if _, err := w.Write(p.Data); err != nil {
				return fmt.Errorf("failed to spool restore data: %w", err)
			}
		}
		if chunk.GetEof() {
			break recvLoop
		}
	}
	got := hex.EncodeToString(h.Sum(nil))
	if got != meta.GetSha256() {
		return fmt.Errorf("digest mismatch for partition %d: expected %s, received %s", meta.GetPartitionId(), meta.GetSha256(), got)
	}

	if _, err := spool.Seek(0, io.SeekStart); err != nil {
		return err
	}
	zr, err := gzip.NewReader(spool)
	if err != nil {
		return fmt.Errorf("restore payload is not gzip: %w", err)
	}
	raw, err := io.ReadAll(zr)
	if err != nil {
		return fmt.Errorf("failed to decompress restore payload: %w", err)
	}
	if len(raw) < 16 || string(raw[:16]) != "SQLite format 3\x00" {
		return fmt.Errorf("restore payload is not a valid SQLite database")
	}
	if err := dst.Load(ctx, &rqcmd.LoadRequest{Data: raw}); err != nil {
		return fmt.Errorf("failed to load database into partition %d: %w", meta.GetPartitionId(), err)
	}
	return nil
}
```

Add imports: `compress/gzip`, `os`.

- [ ] **Step 4: Run tests.** Run: `go test ./internal/cluster/backup/ -v -count=1`. Expected: PASS.

- [ ] **Step 5: gRPC handler** (delete `PartitionRestore` stub; append to `server/backup.go`):

```go
// PartitionRestore loads a partition database image shipped by the restore
// coordinator, then re-runs schema migrations (the image may be older than
// this binary).
func (s *Server) PartitionRestore(stream proto.ZenService_PartitionRestoreServer) error {
	ctx := stream.Context()
	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "failed to read restore header: %s", err)
	}
	meta := first.GetMeta()
	if meta == nil {
		return status.Errorf(codes.InvalidArgument, "first restore chunk must carry meta")
	}
	partitionNode := s.controller.GetPartition(ctx, meta.GetPartitionId())
	if partitionNode == nil {
		return status.Errorf(codes.NotFound, "partition %d is not hosted on this node", meta.GetPartitionId())
	}
	if !s.store.ClusterState().Restoring {
		return status.Errorf(codes.FailedPrecondition, "cluster is not in restoring state")
	}
	spoolDir, err := os.MkdirTemp("", "zenbpm-restore-*")
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create spool dir: %s", err)
	}
	defer os.RemoveAll(spoolDir)

	err = backup.ReceivePartitionRestore(ctx, spoolDir, meta, stream.Recv, partitionNode.DB.Store)
	if err != nil {
		return status.Errorf(codes.Internal, "partition restore failed: %s", err)
	}
	if err := partitionNode.DB.RunMigrations(ctx); err != nil {
		return status.Errorf(codes.Internal, "post-restore migrations failed: %s", err)
	}
	return stream.SendAndClose(&proto.PartitionRestoreResponse{})
}
```

(Client-streaming handlers use `SendAndClose`; adapt the exact method name to the generated interface from Task 1 Step 4.)

- [ ] **Step 6: Verify build + tests.** Run: `go build ./... && go test ./internal/cluster/... -count=1`. Expected: PASS.

- [ ] **Step 7: Commit.**

```bash
git add internal/cluster/backup/ internal/cluster/server/
git commit -m "feat(cluster): load partition restores through raft with digest verification"
```

---

### Task 10: Restore coordinator + empty-check + REST/gRPC endpoints

**Files:**
- Create: `internal/cluster/backup/report.go`
- Modify: `internal/cluster/backup/coordinator.go` (RunClusterRestore)
- Modify: `internal/cluster/partition/partition_backup.go` (+DataStats)
- Modify: `internal/cluster/server/backup.go` (PartitionDataStats + ClusterRestore handlers; delete stubs)
- Modify: `internal/cluster/server/server.go` (StoreService interface: add WriteMaintenanceChange)
- Create: `internal/rest/cluster_backup.go` handler additions
- Modify: `internal/rest/server.go` (register POST route)
- Modify: `internal/cluster/node_backup.go` (ZenNode.ClusterRestore)

**Interfaces:**
- Consumes: `OpenBundle`, `Bundle`, `Manifest.Validate`, `ReceivePartitionRestore` chunk protocol, `WriteMaintenanceChange` (Task 7), `proto.PartitionDataStats*`.
- Produces: `backup.RestoreReport{StartedAtMillis, FinishedAtMillis int64, Partitions []PartitionRestoreResult, PointersRebuilt int, PointerConflicts []PointerConflict, DefinitionsSynced []DefinitionSyncEntry}` with `PartitionRestoreResult{PartitionID uint32, LoadMillis int64}`, `PointerConflict{Name, CorrelationKey string, WinnerKey int64, LoserKeys []int64}`, `DefinitionSyncEntry{Key int64, Type string, ToPartitions []uint32}`; `backup.RestoreDeps{Clients ClientProvider, ClusterState func() state.Cluster, SetRestoring func(bool) error, BinarySchemaVersion string, SpoolDir string}`; `backup.RunClusterRestore(ctx context.Context, deps RestoreDeps, r io.Reader, force bool) (*RestoreReport, error)`; `backup.BinarySchemaVersion(migrationDir string) (string, error)`; `(rq *partition.DB) DataStats(ctx) (definitions, instances int64, err error)`; `(node *ZenNode) ClusterRestore(ctx, r io.Reader, force bool) (*backup.RestoreReport, error)`; REST `POST /v1/cluster/restore?force=true`.

- [ ] **Step 1: report.go:**

```go
package backup

type PartitionRestoreResult struct {
	PartitionID uint32 `json:"partitionId"`
	LoadMillis  int64  `json:"loadMillis"`
}

type PointerConflict struct {
	Name           string  `json:"name"`
	CorrelationKey string  `json:"correlationKey"`
	WinnerKey      int64   `json:"winnerKey"`
	LoserKeys      []int64 `json:"loserKeys"`
}

type DefinitionSyncEntry struct {
	Key          int64    `json:"key"`
	Type         string   `json:"type"` // "process" | "dmn"
	ToPartitions []uint32 `json:"toPartitions"`
}

// RestoreReport is returned to the operator after a cluster restore.
type RestoreReport struct {
	StartedAtMillis   int64                    `json:"startedAtMillis"`
	FinishedAtMillis  int64                    `json:"finishedAtMillis"`
	Partitions        []PartitionRestoreResult `json:"partitions"`
	PointersRebuilt   int                      `json:"pointersRebuilt"`
	PointerConflicts  []PointerConflict        `json:"pointerConflicts"`
	DefinitionsSynced []DefinitionSyncEntry    `json:"definitionsSynced"`
}
```

- [ ] **Step 2: DataStats on partition DB** (append to `partition_backup.go`; `QueryRowContext` exists at partition_persistence.go:390):

```go
// DataStats returns coarse row counts used by the restore empty-cluster check.
func (rq *DB) DataStats(ctx context.Context) (definitions int64, instances int64, err error) {
	row := rq.QueryRowContext(ctx,
		"SELECT (SELECT COUNT(*) FROM process_definition), (SELECT COUNT(*) FROM process_instance)")
	err = row.Scan(&definitions, &instances)
	return definitions, instances, err
}
```

Add a test in `partition_backup_test.go` using the same DB fixture as Task 3 Step 5: fresh DB → `(0, 0, nil)`.

- [ ] **Step 3: PartitionDataStats handler** (delete stub; append to `server/backup.go`):

```go
func (s *Server) PartitionDataStats(ctx context.Context, req *proto.PartitionDataStatsRequest) (*proto.PartitionDataStatsResponse, error) {
	partitionNode := s.controller.GetPartition(ctx, req.GetPartitionId())
	if partitionNode == nil {
		return nil, status.Errorf(codes.NotFound, "partition %d is not hosted on this node", req.GetPartitionId())
	}
	defs, insts, err := partitionNode.DB.DataStats(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to read data stats: %s", err)
	}
	return &proto.PartitionDataStatsResponse{ProcessDefinitions: defs, ProcessInstances: insts}, nil
}
```

- [ ] **Step 4: RunClusterRestore** (append to `coordinator.go`):

```go
// RestoreDeps carries the coordinator's dependencies so both ZenNode (REST)
// and the gRPC server can drive a restore.
type RestoreDeps struct {
	Clients             ClientProvider
	ClusterState        func() state.Cluster
	SetRestoring        func(restoring bool) error
	BinarySchemaVersion string
	SpoolDir            string
}

// RunClusterRestore validates the bundle, gates the cluster, loads every
// partition sequentially, reconciles derived state, and un-gates.
// On failure after gating, the cluster is deliberately LEFT in Restoring
// state — the operator retries the restore (the operation is idempotent).
func RunClusterRestore(ctx context.Context, deps RestoreDeps, r io.Reader, force bool) (*RestoreReport, error) {
	report := &RestoreReport{StartedAtMillis: time.Now().UnixMilli()}
	cs := deps.ClusterState()

	bundle, err := OpenBundle(r, deps.SpoolDir)
	if err != nil {
		return nil, fmt.Errorf("invalid backup bundle: %w", err)
	}
	defer bundle.Close()
	if err := bundle.Manifest.Validate(uint32(len(cs.Partitions)), deps.BinarySchemaVersion); err != nil {
		return nil, fmt.Errorf("bundle cannot be restored into this cluster: %w", err)
	}

	if !force {
		empty, err := clusterIsEmpty(ctx, cs, deps.Clients)
		if err != nil {
			return nil, fmt.Errorf("failed to check whether cluster is empty: %w", err)
		}
		if !empty {
			return nil, fmt.Errorf("cluster contains data; pass force=true to overwrite it")
		}
	}

	if err := deps.SetRestoring(true); err != nil {
		return nil, fmt.Errorf("failed to enter restore mode: %w", err)
	}

	ids := make([]uint32, 0, len(bundle.Manifest.Partitions))
	for id := range bundle.Manifest.Partitions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// sequential loads bound coordinator memory: each store.Load holds one
	// full partition image as a single raft entry
	for _, id := range ids {
		start := time.Now()
		if err := restoreOnePartition(ctx, deps, bundle, id); err != nil {
			return nil, fmt.Errorf("restore of partition %d failed (cluster left in restoring state; retry the restore): %w", id, err)
		}
		report.Partitions = append(report.Partitions, PartitionRestoreResult{
			PartitionID: id,
			LoadMillis:  time.Since(start).Milliseconds(),
		})
	}

	if err := reconcile(ctx, deps, report); err != nil {
		return nil, fmt.Errorf("post-restore reconciliation failed (cluster left in restoring state; retry the restore): %w", err)
	}

	if err := deps.SetRestoring(false); err != nil {
		return nil, fmt.Errorf("restore finished but failed to leave restore mode: %w", err)
	}
	report.FinishedAtMillis = time.Now().UnixMilli()
	return report, nil
}

func restoreOnePartition(ctx context.Context, deps RestoreDeps, bundle *Bundle, id uint32) error {
	leader, err := deps.Clients.PartitionLeader(id)
	if err != nil {
		return fmt.Errorf("failed to get leader client: %w", err)
	}
	stream, err := leader.PartitionRestore(ctx)
	if err != nil {
		return fmt.Errorf("failed to open restore stream: %w", err)
	}
	meta := bundle.Manifest.Partitions[id]
	err = stream.Send(&proto.RestoreChunk{Payload: &proto.RestoreChunk_Meta{Meta: &proto.RestoreMeta{
		PartitionId: id,
		Sha256:      meta.SHA256,
		SizeBytes:   meta.SizeBytes,
	}}})
	if err != nil {
		return fmt.Errorf("failed to send restore meta: %w", err)
	}
	f, err := bundle.PartitionFile(id)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := make([]byte, backupChunkSize)
	for {
		n, rerr := f.Read(buf)
		if n > 0 {
			data := append([]byte(nil), buf[:n]...)
			if err := stream.Send(&proto.RestoreChunk{Payload: &proto.RestoreChunk_Data{Data: data}}); err != nil {
				return fmt.Errorf("failed to send restore data: %w", err)
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return rerr
		}
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("partition leader rejected restore: %w", err)
	}
	return nil
}

func clusterIsEmpty(ctx context.Context, cs state.Cluster, clients ClientProvider) (bool, error) {
	for id := range cs.Partitions {
		leader, err := clients.PartitionLeader(id)
		if err != nil {
			return false, err
		}
		resp, err := leader.PartitionDataStats(ctx, &proto.PartitionDataStatsRequest{PartitionId: id})
		if err != nil {
			return false, err
		}
		if resp.GetProcessDefinitions() > 0 || resp.GetProcessInstances() > 0 {
			return false, nil
		}
	}
	return true, nil
}

// reconcile is completed in Tasks 12-13; at this stage it is a no-op shell.
func reconcile(ctx context.Context, deps RestoreDeps, report *RestoreReport) error {
	return nil
}
```

Add `BinarySchemaVersion` helper (append to `manifest.go`):

```go
// BinarySchemaVersion returns the newest migration filename shipped with this
// binary, used to reject bundles created by a newer schema.
func BinarySchemaVersion(migrationDir string) (string, error) {
	migs, err := zensql.GetUpMigrations(migrationDir)
	if err != nil {
		return "", fmt.Errorf("failed to read migrations: %w", err)
	}
	var latest string
	for _, m := range migs {
		if m.Filename > latest {
			latest = m.Filename
		}
	}
	return latest, nil
}
```

with import `zensql "github.com/pbinitiative/zenbpm/internal/sql"` (read-only use of the package is allowed; check `sql.DefaultMigrationsDir` and `MigrationData.Filename` field names against `internal/cluster/partition/migrations.go` usage).

- [ ] **Step 5: ZenNode.ClusterRestore** (append to `node_backup.go`):

```go
// ClusterRestore restores the whole cluster from a backup bundle.
// Without force it refuses when the cluster holds any definitions/instances.
func (node *ZenNode) ClusterRestore(ctx context.Context, r io.Reader, force bool) (*backup.RestoreReport, error) {
	spoolDir, err := os.MkdirTemp("", "zenbpm-restore-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create restore spool dir: %w", err)
	}
	defer os.RemoveAll(spoolDir)
	binSchema, err := backup.BinarySchemaVersion(node.controller.Config.Persistence.Migration.Dir)
	if err != nil {
		return nil, err
	}
	deps := backup.RestoreDeps{
		Clients:             node.client,
		ClusterState:        node.store.ClusterState,
		SetRestoring: func(restoring bool) error {
			return node.store.WriteMaintenanceChange(&protoc.ClusterMaintenanceChange{Restoring: restoring})
		},
		BinarySchemaVersion: binSchema,
		SpoolDir:            spoolDir,
	}
	return backup.RunClusterRestore(ctx, deps, r, force)
}
```

(Import alias `protoc` for command proto, matching node.go conventions. If `Config.Persistence.Migration.Dir` is empty, fall back to `sql.DefaultMigrationsDir` — same fallback as `NewDB` in partition_persistence.go. NOTE: `WriteMaintenanceChange` goes through raft — it only succeeds on the cluster raft leader; if this node is a follower, the coordinator returns the raft not-leader error and the operator retries against the leader. Acceptable for v1; document in Task 15.)

- [ ] **Step 6: gRPC ClusterRestore handler** (delete stub; append to `server/backup.go`). First add `WriteMaintenanceChange(change *protoc.ClusterMaintenanceChange) error` to the `StoreService` interface in server.go (the concrete `*store.Store` already satisfies it after Task 7). Then:

```go
// ClusterRestore accepts a backup bundle over gRPC and drives the same
// coordinator as the REST endpoint.
func (s *Server) ClusterRestore(stream proto.ZenService_ClusterRestoreServer) error {
	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "failed to read restore header: %s", err)
	}
	meta := first.GetMeta()
	if meta == nil {
		return status.Errorf(codes.InvalidArgument, "first restore chunk must carry meta")
	}

	pr, pw := io.Pipe()
	go func() {
		for {
			chunk, err := stream.Recv()
			if err == io.EOF {
				pw.Close()
				return
			}
			if err != nil {
				pw.CloseWithError(err)
				return
			}
			if d := chunk.GetData(); len(d) > 0 {
				if _, err := pw.Write(d); err != nil {
					return
				}
			}
			if chunk.GetEof() {
				pw.Close()
				return
			}
		}
	}()

	spoolDir, err := os.MkdirTemp("", "zenbpm-restore-*")
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create spool dir: %s", err)
	}
	defer os.RemoveAll(spoolDir)
	binSchema, err := backup.BinarySchemaVersion(sql.DefaultMigrationsDir)
	if err != nil {
		return status.Errorf(codes.Internal, "%s", err)
	}
	deps := backup.RestoreDeps{
		Clients:      s.client,
		ClusterState: s.store.ClusterState,
		SetRestoring: func(restoring bool) error {
			return s.store.WriteMaintenanceChange(&protoc.ClusterMaintenanceChange{Restoring: restoring})
		},
		BinarySchemaVersion: binSchema,
		SpoolDir:            spoolDir,
	}
	report, err := backup.RunClusterRestore(stream.Context(), deps, pr, meta.GetForce())
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "cluster restore failed: %s", err)
	}
	reportJSON, err := json.Marshal(report)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to encode restore report: %s", err)
	}
	return stream.SendAndClose(&proto.ClusterRestoreResponse{ReportJson: reportJSON})
}
```

(`sql` here is `github.com/pbinitiative/zenbpm/internal/sql`, already imported by server.go; `protoc` is the command-proto alias used there.)

- [ ] **Step 7: REST handler** (append to `internal/rest/cluster_backup.go`):

```go
func (s *Server) handleClusterRestore(w http.ResponseWriter, r *http.Request) {
	force := r.URL.Query().Get("force") == "true"
	report, err := s.node.ClusterRestore(r.Context(), r.Body, force)
	if err != nil {
		writeError(w, r, http.StatusConflict, public.Error{Message: err.Error(), Code: "RESTORE_FAILED"})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(report)
}
```

Register in `internal/rest/server.go` next to the backup route:

```go
		r.Post("/cluster/restore", s.handleClusterRestore)
```

- [ ] **Step 8: Verify build + tests.** Run: `go build ./... && go test ./internal/cluster/... ./internal/rest/... -count=1`. Expected: PASS.

- [ ] **Step 9: Commit.**

```bash
git add internal/cluster/ internal/rest/
git commit -m "feat(cluster): add cluster restore coordinator with force gate and empty-cluster check"
```

---

## Phase 3 — Reconciliation

### Task 11: Pointer rebuild planner (pure logic)

**Files:**
- Create: `internal/cluster/backup/reconcile.go`
- Test: `internal/cluster/backup/reconcile_test.go`

**Interfaces:**
- Consumes: `proto.MessageSubscriptionRow` (Task 1), `PointerConflict` (Task 10).
- Produces: `backup.PointerPlan{ByPartition map[uint32][]*proto.MessageSubscriptionRow, Conflicts []PointerConflict}`; `backup.PlanPointerRebuild(subs []*proto.MessageSubscriptionRow, homePartition func(name, correlationKey string) uint32) PointerPlan`.

Duplicate policy (from the spec): group by `(name, correlationKey)`; winner = newest `CreatedAt`, ties broken by higher `Key` (deterministic); losers recorded as conflicts.

- [ ] **Step 1: Write the failing test** `internal/cluster/backup/reconcile_test.go`:

```go
package backup

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/stretchr/testify/assert"
)

func row(key int64, name, ck string, createdAt int64) *proto.MessageSubscriptionRow {
	return &proto.MessageSubscriptionRow{Key: key, Name: name, CorrelationKey: ck, CreatedAt: createdAt, State: 2}
}

func TestPlanPointerRebuild(t *testing.T) {
	// deterministic fake hash: route by first byte of correlationKey (or name)
	home := func(name, ck string) uint32 {
		s := ck
		if s == "" {
			s = name
		}
		return uint32(s[0]%2) + 1
	}
	tests := []struct {
		name          string
		subs          []*proto.MessageSubscriptionRow
		wantPerPart   map[uint32][]int64 // partition -> expected pointer subscription keys
		wantConflicts int
	}{
		{
			name: "routes by correlation key hash",
			subs: []*proto.MessageSubscriptionRow{row(10, "msg", "a", 1), row(11, "msg", "b", 1)},
			wantPerPart: map[uint32][]int64{
				home("msg", "a"): {10},
				home("msg", "b"): {11},
			},
		},
		{
			name: "definition-level subscription routes by name",
			subs: []*proto.MessageSubscriptionRow{row(20, "start-msg", "", 5)},
			wantPerPart: map[uint32][]int64{
				home("start-msg", ""): {20},
			},
		},
		{
			name:          "duplicate actives: newest created_at wins",
			subs:          []*proto.MessageSubscriptionRow{row(30, "msg", "same", 100), row(31, "msg", "same", 200)},
			wantPerPart:   map[uint32][]int64{home("msg", "same"): {31}},
			wantConflicts: 1,
		},
		{
			name:          "created_at tie: higher key wins deterministically",
			subs:          []*proto.MessageSubscriptionRow{row(41, "msg", "same", 100), row(40, "msg", "same", 100)},
			wantPerPart:   map[uint32][]int64{home("msg", "same"): {41}},
			wantConflicts: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := PlanPointerRebuild(tt.subs, home)
			got := map[uint32][]int64{}
			for part, rows := range plan.ByPartition {
				for _, r := range rows {
					got[part] = append(got[part], r.GetKey())
				}
			}
			assert.Equal(t, tt.wantPerPart, got)
			assert.Len(t, plan.Conflicts, tt.wantConflicts)
			if tt.wantConflicts == 1 {
				c := plan.Conflicts[0]
				assert.Equal(t, "msg", c.Name)
				assert.Len(t, c.LoserKeys, 1)
			}
		})
	}
}
```

- [ ] **Step 2: Run to verify failure**, then **Step 3: implement** `reconcile.go`:

```go
package backup

import (
	"sort"

	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
)

type PointerPlan struct {
	ByPartition map[uint32][]*proto.MessageSubscriptionRow
	Conflicts   []PointerConflict
}

// PlanPointerRebuild recomputes the message_subscription_pointer placement for
// every ACTIVE subscription in the restored cluster. Duplicates on
// (name, correlationKey) — possible from snapshot skew — resolve to the newest
// CreatedAt (ties: highest Key); losers are reported, not silently dropped.
func PlanPointerRebuild(subs []*proto.MessageSubscriptionRow, homePartition func(name, correlationKey string) uint32) PointerPlan {
	type slot struct{ name, ck string }
	groups := map[slot][]*proto.MessageSubscriptionRow{}
	for _, s := range subs {
		k := slot{s.GetName(), s.GetCorrelationKey()}
		groups[k] = append(groups[k], s)
	}

	plan := PointerPlan{ByPartition: map[uint32][]*proto.MessageSubscriptionRow{}}
	// deterministic iteration for stable output
	keys := make([]slot, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].name != keys[j].name {
			return keys[i].name < keys[j].name
		}
		return keys[i].ck < keys[j].ck
	})

	for _, k := range keys {
		rows := groups[k]
		sort.Slice(rows, func(i, j int) bool {
			if rows[i].GetCreatedAt() != rows[j].GetCreatedAt() {
				return rows[i].GetCreatedAt() > rows[j].GetCreatedAt()
			}
			return rows[i].GetKey() > rows[j].GetKey()
		})
		winner := rows[0]
		part := homePartition(k.name, k.ck)
		plan.ByPartition[part] = append(plan.ByPartition[part], winner)
		if len(rows) > 1 {
			c := PointerConflict{Name: k.name, CorrelationKey: k.ck, WinnerKey: winner.GetKey()}
			for _, loser := range rows[1:] {
				c.LoserKeys = append(c.LoserKeys, loser.GetKey())
			}
			plan.Conflicts = append(plan.Conflicts, c)
		}
	}
	return plan
}
```

- [ ] **Step 4: Run tests.** Run: `go test ./internal/cluster/backup/ -run TestPlanPointerRebuild -v`. Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add internal/cluster/backup/
git commit -m "feat(cluster): plan pointer rebuild with newest-wins duplicate policy"
```

---

### Task 12: Pointer rebuild wiring — DB methods, RPCs, coordinator integration

**Files:**
- Modify: `internal/cluster/partition/partition_backup.go` (+ListActiveMessageSubscriptions, +RebuildMessageSubscriptionPointers)
- Test: `internal/cluster/partition/partition_backup_test.go`
- Modify: `internal/cluster/server/backup.go` (two handlers; delete stubs)
- Modify: `internal/cluster/backup/coordinator.go` (fill `reconcile` pointer phase)

**Interfaces:**
- Consumes: `PlanPointerRebuild` (Task 11), `DB.QueryContext` (partition_persistence.go:366), `DB.ExecuteStatements` (partition_persistence.go:168), `generateStatement` (partition_persistence.go:197, package-private — our new methods live in the same package), `bpmnruntime.ActivityStateActive`, `state.Cluster.GetPartitionIdForMessageSubscriptionPointer(name, correlationKey string) uint32` (state.go:144).
- Produces: `(rq *DB) ListActiveMessageSubscriptions(ctx) ([]*zenproto.MessageSubscriptionRow, error)`; `(rq *DB) RebuildMessageSubscriptionPointers(ctx, rows []*zenproto.MessageSubscriptionRow) error` (wipe + batch insert, single ExecuteStatements call).

- [ ] **Step 1: Write the failing DB test** (append to `partition_backup_test.go`, same fixture as before). Seed via existing sqlc queries — `db.Queries.SaveMessageSubscription` and `db.Queries.SaveMessageSubscriptionPointer` exist:

```go
func TestListActiveMessageSubscriptionsAndRebuildPointers(t *testing.T) {
	// fixture as in TestSchemaVersion
	ctx := t.Context()

	// two ACTIVE subscriptions, one COMPLETED (must not be listed)
	saveSub := func(key int64, name, ck string, state int64, createdAt int64) {
		err := db.Queries.SaveMessageSubscription(ctx, sql.SaveMessageSubscriptionParams{
			Key: key, ElementId: "el", ProcessDefinitionKey: 1, ProcessInstanceKey: 1,
			Name: name, State: state, CreatedAt: createdAt, CorrelationKey: ck, ExecutionToken: 1,
			Type: 1,
		})
		assert.NoError(t, err)
	}
	active := int64(bpmnruntime.ActivityStateActive)
	saveSub(101, "m1", "ck-a", active, 1000)
	saveSub(102, "m2", "ck-b", active, 2000)
	saveSub(103, "m3", "ck-c", int64(bpmnruntime.ActivityStateCompleted), 3000)

	rows, err := db.ListActiveMessageSubscriptions(ctx)
	assert.NoError(t, err)
	assert.Len(t, rows, 2)

	// pre-existing stale pointer must be wiped by rebuild
	err = db.Queries.SaveMessageSubscriptionPointer(ctx, sql.SaveMessageSubscriptionPointerParams{
		State: active, CreatedAt: 1, Name: "stale", CorrelationKey: "gone", MessageSubscriptionKey: 999,
	})
	assert.NoError(t, err)

	err = db.RebuildMessageSubscriptionPointers(ctx, rows)
	assert.NoError(t, err)

	// stale pointer gone
	_, err = db.Queries.FindMessageSubscriptionPointer(ctx, sql.FindMessageSubscriptionPointerParams{
		FilterState: active, CorrelationKey: "gone", Name: "stale",
	})
	assert.Error(t, err) // no rows

	// rebuilt pointers resolve
	ptr, err := db.Queries.FindMessageSubscriptionPointer(ctx, sql.FindMessageSubscriptionPointerParams{
		FilterState: active, CorrelationKey: "ck-a", Name: "m1",
	})
	assert.NoError(t, err)
	assert.Equal(t, int64(101), ptr.MessageSubscriptionKey)
}
```

(Adjust `SaveMessageSubscriptionParams` field set to the generated struct — check `internal/sql` generated code for exact fields; `Type` discriminator: 1 = instance-level per `pkg/bpmn/runtime/types.go`; verify constant when writing.)

- [ ] **Step 2: Run to verify failure**, then **Step 3: implement** (append to `partition_backup.go`):

```go
// ListActiveMessageSubscriptions returns every ACTIVE subscription row on this
// partition — the authoritative source for rebuilding pointer tables. There is
// deliberately no sqlc query for this (internal/sql is out of scope), so it
// uses the raw read path.
func (rq *DB) ListActiveMessageSubscriptions(ctx context.Context) ([]*zenproto.MessageSubscriptionRow, error) {
	rows, err := rq.QueryContext(ctx,
		"SELECT key, name, correlation_key, created_at, state FROM message_subscription WHERE state = ?",
		int64(bpmnruntime.ActivityStateActive))
	if err != nil {
		return nil, fmt.Errorf("failed to list active message subscriptions: %w", err)
	}
	defer rows.Close()
	var out []*zenproto.MessageSubscriptionRow
	for rows.Next() {
		r := &zenproto.MessageSubscriptionRow{}
		var key, createdAt, state int64
		var name, ck string
		if err := rows.Scan(&key, &name, &ck, &createdAt, &state); err != nil {
			return nil, err
		}
		r.Key, r.Name, r.CorrelationKey, r.CreatedAt, r.State = key, name, ck, createdAt, state
		out = append(out, r)
	}
	return out, rows.Err()
}

// RebuildMessageSubscriptionPointers wipes this partition's pointer table and
// re-inserts the given rows in one raft-replicated batch.
func (rq *DB) RebuildMessageSubscriptionPointers(ctx context.Context, rows []*zenproto.MessageSubscriptionRow) error {
	statements := make([]*rqproto.Statement, 0, len(rows)+1)
	statements = append(statements, &rqproto.Statement{Sql: "DELETE FROM message_subscription_pointer"})
	for _, r := range rows {
		st, err := rq.generateStatement(
			"INSERT INTO message_subscription_pointer(state, created_at, name, correlation_key, message_subscription_key) VALUES (?, ?, ?, ?, ?)",
			r.GetState(), r.GetCreatedAt(), r.GetName(), r.GetCorrelationKey(), r.GetKey())
		if err != nil {
			return fmt.Errorf("failed to build pointer insert: %w", err)
		}
		statements = append(statements, st)
	}
	results, err := rq.ExecuteStatements(ctx, statements)
	if err != nil {
		return fmt.Errorf("failed to rebuild message subscription pointers: %w", err)
	}
	for _, res := range results {
		if res != nil && res.GetError() != "" {
			return fmt.Errorf("pointer rebuild statement failed: %s", res.GetError())
		}
	}
	return nil
}
```

(Imports: `zenproto "github.com/pbinitiative/zenbpm/internal/cluster/proto"`, `rqproto "github.com/rqlite/rqlite/v10/command/proto"`, `bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"` — match aliases already used in partition_persistence.go.)

Run: `go test ./internal/cluster/partition/ -run TestListActive -v`. Expected: PASS.

- [ ] **Step 4: gRPC handlers** (delete both stubs; append to `server/backup.go`):

```go
func (s *Server) ListActiveMessageSubscriptions(ctx context.Context, req *proto.ListActiveMessageSubscriptionsRequest) (*proto.ListActiveMessageSubscriptionsResponse, error) {
	partitionNode := s.controller.GetPartition(ctx, req.GetPartitionId())
	if partitionNode == nil {
		return nil, status.Errorf(codes.NotFound, "partition %d is not hosted on this node", req.GetPartitionId())
	}
	rows, err := partitionNode.DB.ListActiveMessageSubscriptions(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s", err)
	}
	return &proto.ListActiveMessageSubscriptionsResponse{Rows: rows}, nil
}

func (s *Server) RebuildMessageSubscriptionPointers(ctx context.Context, req *proto.RebuildMessageSubscriptionPointersRequest) (*proto.RebuildMessageSubscriptionPointersResponse, error) {
	if !s.store.ClusterState().Restoring {
		return nil, status.Errorf(codes.FailedPrecondition, "cluster is not in restoring state")
	}
	partitionNode := s.controller.GetPartition(ctx, req.GetPartitionId())
	if partitionNode == nil {
		return nil, status.Errorf(codes.NotFound, "partition %d is not hosted on this node", req.GetPartitionId())
	}
	if err := partitionNode.DB.RebuildMessageSubscriptionPointers(ctx, req.GetPointers()); err != nil {
		return nil, status.Errorf(codes.Internal, "%s", err)
	}
	return &proto.RebuildMessageSubscriptionPointersResponse{}, nil
}
```

- [ ] **Step 5: Coordinator integration** — replace the `reconcile` no-op body in `coordinator.go`:

```go
func reconcile(ctx context.Context, deps RestoreDeps, report *RestoreReport) error {
	if err := syncDefinitions(ctx, deps, report); err != nil { // Task 13; stub returns nil until then
		return err
	}
	cs := deps.ClusterState()

	var all []*proto.MessageSubscriptionRow
	for id := range cs.Partitions {
		leader, err := deps.Clients.PartitionLeader(id)
		if err != nil {
			return fmt.Errorf("pointer scan: failed to get leader for partition %d: %w", id, err)
		}
		resp, err := leader.ListActiveMessageSubscriptions(ctx, &proto.ListActiveMessageSubscriptionsRequest{PartitionId: id})
		if err != nil {
			return fmt.Errorf("pointer scan on partition %d failed: %w", id, err)
		}
		all = append(all, resp.GetRows()...)
	}

	plan := PlanPointerRebuild(all, cs.GetPartitionIdForMessageSubscriptionPointer)
	report.PointerConflicts = plan.Conflicts

	// every partition gets a rebuild call — even with zero rows — to wipe stale pointers
	for id := range cs.Partitions {
		rows := plan.ByPartition[id]
		leader, err := deps.Clients.PartitionLeader(id)
		if err != nil {
			return fmt.Errorf("pointer rebuild: failed to get leader for partition %d: %w", id, err)
		}
		_, err = leader.RebuildMessageSubscriptionPointers(ctx, &proto.RebuildMessageSubscriptionPointersRequest{
			PartitionId: id,
			Pointers:    rows,
		})
		if err != nil {
			return fmt.Errorf("pointer rebuild on partition %d failed: %w", id, err)
		}
		report.PointersRebuilt += len(rows)
	}
	return nil
}

// syncDefinitions is completed in Task 13.
func syncDefinitions(ctx context.Context, deps RestoreDeps, report *RestoreReport) error {
	return nil
}
```

- [ ] **Step 6: Verify.** Run: `go build ./... && go test ./internal/cluster/... -count=1`. Expected: PASS.

- [ ] **Step 7: Commit.**

```bash
git add internal/cluster/
git commit -m "feat(cluster): rebuild message subscription pointers during restore"
```

---

### Task 13: Definition sync

**Files:**
- Modify: `internal/cluster/partition/partition_backup.go` (+ListDefinitionRefs, +GetDefinitionResource)
- Test: `internal/cluster/partition/partition_backup_test.go`
- Modify: `internal/cluster/server/backup.go` (two handlers; delete stubs)
- Modify: `internal/cluster/backup/reconcile.go` (+MissingDefinitions pure helper)
- Test: `internal/cluster/backup/reconcile_test.go`
- Modify: `internal/cluster/backup/coordinator.go` (fill syncDefinitions)

**Interfaces:**
- Consumes: existing internal deploy RPCs `DeployProcessDefinition{Key, Data, ResourceName, RegisterProcessDefinitionSubscriptions}` and `DeployDmnResourceDefinition{Key, Data}` (idempotent on caller-assigned key).
- Produces: `(rq *DB) ListDefinitionRefs(ctx) ([]*zenproto.DefinitionRef, error)`; `(rq *DB) GetDefinitionResource(ctx, key int64, defType zenproto.DefinitionType) (data []byte, resourceName string, err error)`; `backup.MissingDefinitions(perPartition map[uint32][]*proto.DefinitionRef) map[uint32][]*proto.DefinitionRef` (partition → refs it lacks).

- [ ] **Step 1: Verify column names first.** Run: `grep -A8 "CREATE TABLE.*dmn_resource_definition" internal/sql/migrations/*.up.sql`. Note the data column (expected `dmn_data`; adjust the SQL below if different). `process_definition` columns are `key, version, bpmn_process_id, bpmn_data, bpmn_checksum, bpmn_process_name` (migrations/0001).

- [ ] **Step 2: TDD `MissingDefinitions`** in `reconcile_test.go` + `reconcile.go`:

```go
func TestMissingDefinitions(t *testing.T) {
	ref := func(key int64) *proto.DefinitionRef {
		return &proto.DefinitionRef{Key: key, Type: proto.DefinitionType_DEFINITION_TYPE_PROCESS}
	}
	perPartition := map[uint32][]*proto.DefinitionRef{
		1: {ref(100), ref(200)},
		2: {ref(100)},
		3: {},
	}
	missing := MissingDefinitions(perPartition)
	assert.Len(t, missing[2], 1)
	assert.Equal(t, int64(200), missing[2][0].GetKey())
	assert.Len(t, missing[3], 2)
	assert.NotContains(t, missing, uint32(1))
}
```

```go
// MissingDefinitions computes, per partition, the definitions present
// somewhere in the cluster but absent locally (mid-backup deploy skew).
func MissingDefinitions(perPartition map[uint32][]*proto.DefinitionRef) map[uint32][]*proto.DefinitionRef {
	type refKey struct {
		key int64
		typ proto.DefinitionType
	}
	union := map[refKey]*proto.DefinitionRef{}
	for _, refs := range perPartition {
		for _, r := range refs {
			union[refKey{r.GetKey(), r.GetType()}] = r
		}
	}
	// deterministic union ordering
	ordered := make([]*proto.DefinitionRef, 0, len(union))
	for _, r := range union {
		ordered = append(ordered, r)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].GetKey() < ordered[j].GetKey() })

	missing := map[uint32][]*proto.DefinitionRef{}
	for part, refs := range perPartition {
		have := map[refKey]bool{}
		for _, r := range refs {
			have[refKey{r.GetKey(), r.GetType()}] = true
		}
		for _, r := range ordered {
			if !have[refKey{r.GetKey(), r.GetType()}] {
				missing[part] = append(missing[part], r)
			}
		}
	}
	return missing
}
```

- [ ] **Step 3: DB methods + tests** (fixture as before; seed a process definition row via `db.Queries.SaveProcessDefinition`):

```go
// ListDefinitionRefs lists all process and DMN definition keys on this partition.
func (rq *DB) ListDefinitionRefs(ctx context.Context) ([]*zenproto.DefinitionRef, error) {
	var out []*zenproto.DefinitionRef
	collect := func(query string, typ zenproto.DefinitionType) error {
		rows, err := rq.QueryContext(ctx, query)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var key int64
			if err := rows.Scan(&key); err != nil {
				return err
			}
			out = append(out, &zenproto.DefinitionRef{Key: key, Type: typ})
		}
		return rows.Err()
	}
	if err := collect("SELECT key FROM process_definition", zenproto.DefinitionType_DEFINITION_TYPE_PROCESS); err != nil {
		return nil, fmt.Errorf("failed to list process definitions: %w", err)
	}
	if err := collect("SELECT key FROM dmn_resource_definition", zenproto.DefinitionType_DEFINITION_TYPE_DMN_RESOURCE); err != nil {
		return nil, fmt.Errorf("failed to list dmn resource definitions: %w", err)
	}
	return out, nil
}

// GetDefinitionResource returns the raw resource for re-deploying a definition
// to a partition that misses it.
func (rq *DB) GetDefinitionResource(ctx context.Context, key int64, defType zenproto.DefinitionType) ([]byte, string, error) {
	switch defType {
	case zenproto.DefinitionType_DEFINITION_TYPE_PROCESS:
		row := rq.QueryRowContext(ctx, "SELECT bpmn_data, bpmn_process_id FROM process_definition WHERE key = ?", key)
		var data, processID string
		if err := row.Scan(&data, &processID); err != nil {
			return nil, "", fmt.Errorf("failed to load process definition %d: %w", key, err)
		}
		return []byte(data), processID + ".bpmn", nil
	case zenproto.DefinitionType_DEFINITION_TYPE_DMN_RESOURCE:
		row := rq.QueryRowContext(ctx, "SELECT dmn_data FROM dmn_resource_definition WHERE key = ?", key) // column per Step 1
		var data string
		if err := row.Scan(&data); err != nil {
			return nil, "", fmt.Errorf("failed to load dmn resource definition %d: %w", key, err)
		}
		return []byte(data), "", nil
	}
	return nil, "", fmt.Errorf("unknown definition type %v", defType)
}
```

- [ ] **Step 4: gRPC handlers** (delete stubs; append to `server/backup.go`):

```go
func (s *Server) ListDefinitions(ctx context.Context, req *proto.ListDefinitionsRequest) (*proto.ListDefinitionsResponse, error) {
	partitionNode := s.controller.GetPartition(ctx, req.GetPartitionId())
	if partitionNode == nil {
		return nil, status.Errorf(codes.NotFound, "partition %d is not hosted on this node", req.GetPartitionId())
	}
	refs, err := partitionNode.DB.ListDefinitionRefs(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s", err)
	}
	return &proto.ListDefinitionsResponse{Definitions: refs}, nil
}

func (s *Server) GetDefinitionResource(ctx context.Context, req *proto.GetDefinitionResourceRequest) (*proto.GetDefinitionResourceResponse, error) {
	partitionNode := s.controller.GetPartition(ctx, req.GetPartitionId())
	if partitionNode == nil {
		return nil, status.Errorf(codes.NotFound, "partition %d is not hosted on this node", req.GetPartitionId())
	}
	data, resourceName, err := partitionNode.DB.GetDefinitionResource(ctx, req.GetKey(), req.GetType())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%s", err)
	}
	return &proto.GetDefinitionResourceResponse{Data: data, ResourceName: resourceName}, nil
}
```

- [ ] **Step 5: Coordinator `syncDefinitions`** — replace the stub in `coordinator.go`:

```go
func syncDefinitions(ctx context.Context, deps RestoreDeps, report *RestoreReport) error {
	cs := deps.ClusterState()
	perPartition := map[uint32][]*proto.DefinitionRef{}
	for id := range cs.Partitions {
		leader, err := deps.Clients.PartitionLeader(id)
		if err != nil {
			return fmt.Errorf("definition scan: failed to get leader for partition %d: %w", id, err)
		}
		resp, err := leader.ListDefinitions(ctx, &proto.ListDefinitionsRequest{PartitionId: id})
		if err != nil {
			return fmt.Errorf("definition scan on partition %d failed: %w", id, err)
		}
		perPartition[id] = resp.GetDefinitions()
	}

	missing := MissingDefinitions(perPartition)
	synced := map[int64]*DefinitionSyncEntry{}
	for part, refs := range missing {
		for _, ref := range refs {
			data, resourceName, err := fetchDefinition(ctx, deps, perPartition, ref)
			if err != nil {
				return err
			}
			target, err := deps.Clients.PartitionLeader(part)
			if err != nil {
				return err
			}
			switch ref.GetType() {
			case proto.DefinitionType_DEFINITION_TYPE_PROCESS:
				resp, err := target.DeployProcessDefinition(ctx, &proto.DeployProcessDefinitionRequest{
					Key:          ref.GetKey(),
					Data:         data,
					ResourceName: resourceName,
					RegisterProcessDefinitionSubscriptions: true,
				})
				if err != nil || resp.GetError() != nil {
					return fmt.Errorf("failed to sync process definition %d to partition %d: %v %v", ref.GetKey(), part, err, resp.GetError())
				}
			case proto.DefinitionType_DEFINITION_TYPE_DMN_RESOURCE:
				resp, err := target.DeployDmnResourceDefinition(ctx, &proto.DeployDmnResourceDefinitionRequest{
					Key:  ref.GetKey(),
					Data: data,
				})
				if err != nil || resp.GetError() != nil {
					return fmt.Errorf("failed to sync dmn definition %d to partition %d: %v %v", ref.GetKey(), part, err, resp.GetError())
				}
			}
			entry, ok := synced[ref.GetKey()]
			if !ok {
				typ := "process"
				if ref.GetType() == proto.DefinitionType_DEFINITION_TYPE_DMN_RESOURCE {
					typ = "dmn"
				}
				entry = &DefinitionSyncEntry{Key: ref.GetKey(), Type: typ}
				synced[ref.GetKey()] = entry
			}
			entry.ToPartitions = append(entry.ToPartitions, part)
		}
	}
	for _, e := range synced {
		report.DefinitionsSynced = append(report.DefinitionsSynced, *e)
	}
	sort.Slice(report.DefinitionsSynced, func(i, j int) bool {
		return report.DefinitionsSynced[i].Key < report.DefinitionsSynced[j].Key
	})
	return nil
}

func fetchDefinition(ctx context.Context, deps RestoreDeps, perPartition map[uint32][]*proto.DefinitionRef, ref *proto.DefinitionRef) ([]byte, string, error) {
	for part, refs := range perPartition {
		for _, r := range refs {
			if r.GetKey() == ref.GetKey() && r.GetType() == ref.GetType() {
				leader, err := deps.Clients.PartitionLeader(part)
				if err != nil {
					return nil, "", err
				}
				resp, err := leader.GetDefinitionResource(ctx, &proto.GetDefinitionResourceRequest{
					PartitionId: part, Key: ref.GetKey(), Type: ref.GetType(),
				})
				if err != nil {
					return nil, "", fmt.Errorf("failed to fetch definition %d from partition %d: %w", ref.GetKey(), part, err)
				}
				return resp.GetData(), resp.GetResourceName(), nil
			}
		}
	}
	return nil, "", fmt.Errorf("definition %d not found on any partition", ref.GetKey())
}
```

Note the ordering already encoded in Task 12's `reconcile`: definition sync runs BEFORE pointer rebuild, so definition-level subscriptions created by the sync deploys are included in the pointer scan. Also verify the `DeployProcessDefinition` handler's semantics while wiring (server.go — it deploys to partitions the receiving node leads): send the request to the leader of the TARGET partition, which is what the code above does.

- [ ] **Step 6: Verify.** Run: `go build ./... && go test ./internal/cluster/... -count=1`. Expected: PASS.

- [ ] **Step 7: Commit.**

```bash
git add internal/cluster/
git commit -m "feat(cluster): sync definitions across partitions during restore reconciliation"
```

---

## Phase 4 — E2E + docs

### Task 14: E2E coverage

**Files:**
- Create: `test/e2e/cluster/backup_restore_test.go` (build tag `//go:build cluster_e2e`, package matching the other files there)

**Interfaces:**
- Consumes: e2e harness — `NewTestCluster(t, nodeCount, opts...)`, `WithPartitions(n)`, `WaitForHealthy`, `DeployDefinitionOnNode`, `CreateInstanceOnNode`, `GetFirstDefinitionKey`, `TestNode.RestAddr` (harness.go:25-40, helpers.go). REST endpoints from Tasks 5/10.

Test plan (each its own test function; follow the file conventions of `data_test.go`, including polling helpers instead of immediate reads — see the eventual-consistency note in `clustering_backlog`):

- [ ] **Step 1: Write `TestClusterBackupRestoreRoundtrip`** (single-node, 1 partition — solid on current Phase-1 infra):

```go
//go:build cluster_e2e

package cluster

// imports: bytes, fmt, io, net/http, testing, time, existing harness imports

func TestClusterBackupRestoreRoundtrip(t *testing.T) {
	tc := NewTestCluster(t, 1)
	defer tc.Teardown(t)
	WaitForHealthy(t, tc, 60*time.Second)
	node := tc.Leader()

	// seed state
	DeployDefinitionOnNode(t, node, "simple_task.bpmn")
	defKey := GetFirstDefinitionKey(t, node)
	instanceKey := CreateInstanceOnNode(t, node, defKey, nil)
	_ = instanceKey

	// take backup
	resp, err := http.Get("http://" + node.RestAddr + "/v1/cluster/backup")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("backup request failed: %v status=%v", err, resp)
	}
	bundle, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || len(bundle) == 0 {
		t.Fatalf("failed to read bundle: %v", err)
	}

	// mutate state after the backup: deploy a second definition
	DeployDefinitionOnNode(t, node, "simple_task.bpmn")

	// restore WITHOUT force must be refused (cluster has data)
	post, err := http.Post("http://"+node.RestAddr+"/v1/cluster/restore", "application/x-tar", bytes.NewReader(bundle))
	if err != nil {
		t.Fatal(err)
	}
	if post.StatusCode == http.StatusOK {
		t.Fatal("restore without force must be refused on a non-empty cluster")
	}
	post.Body.Close()

	// restore WITH force succeeds and returns a report
	post, err = http.Post("http://"+node.RestAddr+"/v1/cluster/restore?force=true", "application/x-tar", bytes.NewReader(bundle))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(post.Body)
	post.Body.Close()
	if post.StatusCode != http.StatusOK {
		t.Fatalf("forced restore failed: status=%d body=%s", post.StatusCode, body)
	}

	// after restore the cluster serves traffic again: poll until a new
	// instance can be created (engines restart on flag clear)
	deadline := time.Now().Add(30 * time.Second)
	for {
		k := GetFirstDefinitionKey(t, node)
		if k != 0 {
			CreateInstanceOnNode(t, node, k, nil)
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cluster did not resume serving after restore")
		}
		time.Sleep(200 * time.Millisecond)
	}
}
```

(Adapt helper signatures/assertions to compile against the harness — e.g. `GetFirstDefinitionKey` may `t.Fatal` internally rather than return 0; restructure the polling accordingly. BPMN fixture name: use whatever `data_test.go` deploys.)

- [ ] **Step 2: Write `TestClusterRestoreRejectsPartitionCountMismatch`** — take a backup on a 1-partition cluster, hand-edit the tar in memory is complex, so instead: `OpenBundle`-level coverage already exists (Task 6); at E2E level assert a corrupted/truncated upload is rejected: POST the first half of the bundle bytes with `force=true`, expect non-200 and cluster still healthy afterwards (deploy still works).

- [ ] **Step 3: Write `TestBackupRestoreMessagePointerReconciliation`** (multi-partition — the reconciliation payoff):

Shape: `NewTestCluster(t, 3, WithPartitions(2))`; deploy the message-catch BPMN used by `TestMessageCorrelationAcrossNodes` (data_test.go:147); create an instance with a correlation key so an instance-level subscription + pointer exist (possibly on different partitions); take a backup; publish the message to complete the catch event (consuming the subscription); force-restore the backup; then publish the message again and assert it correlates (the instance is back to waiting and the pointer was rebuilt). 

**Known risk:** Phase-2 multi-partition formation is not fully landed (`TestPartitionCreation`/`TestPartitionAssignment` fail — see backlog). If `WaitForPartitions(t, tc, 2, ...)` cannot form, mark this test `t.Skip("blocked on Phase 2 multi-partition formation — see clustering backlog")` with the skip reason, keep the single-partition variant of the same flow (pointer and subscription on partition 1) as the active assertion, and record the gap in the backlog memory. Do NOT delete the multi-partition test — it documents the target behavior.

- [ ] **Step 4: Run.** `go test -tags cluster_e2e ./test/e2e/cluster/ -run "TestClusterBackup|TestBackupRestore|TestClusterRestore" -v -timeout 300s`. Expected: PASS (or the explicitly-skipped multi-partition case). Also run the pre-existing suite to check for regressions: `go test -tags cluster_e2e ./test/e2e/cluster/ -run "TestReadAfterWrite|TestThreeNode" -v -timeout 300s`.

- [ ] **Step 5: Commit.**

```bash
git add test/e2e/cluster/
git commit -m "test(cluster): e2e backup/restore roundtrip, force gate, and pointer reconciliation"
```

---

### Task 15: Operator documentation

**Files:**
- Modify: `docs/reference/cluster.md`

- [ ] **Step 1: Add a "Backup & Restore" section** covering: what is backed up (all partition DBs; NOT cluster topology); `curl -o backup.tar http://<node>:<port>/v1/cluster/backup`; `curl -X POST --data-binary @backup.tar "http://<node>:<port>/v1/cluster/restore?force=true"`; the same-partition-count requirement; force semantics (required unless the cluster is empty); restore must target the cluster raft leader node (v1 limitation); behavior during restore (writes rejected, engines paused); the restore report fields (pointers rebuilt, conflicts = potentially-waiting instances to review, definitions synced); failure mid-restore leaves the cluster in restoring state — retry the restore; scheduling via cron+curl; the rqlite auto-backup (S3) option still exists per partition and restores from it get the same reconciliation only when driven through `POST /v1/cluster/restore`; workers holding pre-restore job keys will get NotFound on completion (expected after rollback).

- [ ] **Step 2: Commit.**

```bash
git add docs/reference/cluster.md
git commit -m "docs(cluster): document backup and restore operations"
```

---

## Plan self-review notes (already applied)

- Spec coverage: streaming bundle + manifest (Tasks 2-5), validation-before-destruction + force + empty-check (Tasks 6, 10), Restoring gate + engine stop (Tasks 7-8), sequential loads + migration re-run (Tasks 9-10), pointer rebuild with newest-wins policy (Tasks 11-12), definition sync ordered before pointer rebuild (Task 13), restore report (10, 12, 13), E2E + docs (14-15). Leader-anchored sha256 (spec amendment) is in Task 3/4; stored-bytes checksums in manifest (spec amendment) in Tasks 2/4/6.
- Known deviations from spec, both deliberate: (1) restore coordinator must run on the cluster raft leader (WriteMaintenanceChange goes through raft) — documented in Task 15; (2) definition sync runs BEFORE pointer rebuild (spec listed pointer rebuild first) so def-level subscriptions from synced deploys are scanned — noted in Tasks 12/13.
- Multi-partition E2E is at risk from unfinished Phase 2 (partition formation) — Task 14 Step 3 has an explicit contingency rather than a silent failure.
