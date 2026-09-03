# Cluster Backup & Restore — Design

Date: 2026-07-05
Status: Approved
Scope: `internal/cluster/**`, `internal/cluster/proto`, `test/e2e/`, plus two REST endpoints in `internal/rest` (explicitly approved boundary crossing).

## Context

ZenBPM runs one embedded rqlite (v10.2.0) raft group per partition. Facts this design
relies on, verified against the code:

- Each partition store exposes rqlite's native backup/restore primitives:
  `store.Backup(ctx, req, dst)` (vacuumed, point-in-time, optionally gzipped binary
  SQLite copy, leader-enforced) and `store.Load(ctx, lr)` (loads a full SQLite image
  through the raft log as a single gzip-compressed entry, replicated to followers).
  These are the same code paths behind rqlite's `GET /db/backup` and `POST /db/load`.
- Per-partition auto-backup/auto-restore to S3-compatible storage is already wired
  (`AutoBackupFile` / `AutoRestoreFile`, `internal/cluster/partition/partition.go`).
  It remains untouched as an optional extra.
- gRPC stubs `ClusterBackup`, `ClusterRestore`, `PartitionBackup`, `PartitionRestore`
  exist in `zen_cluster.proto` as unary RPCs with empty messages, returning
  `Unimplemented` (`internal/cluster/server/server.go:177-200`).
- Message subscriptions are authoritative on the process instance's partition
  (`message_subscription` table; subscription key is a zenflake key). The hash
  partition (`GetPartitionIdForMessageSubscriptionPointer`: `hash % partitionCount + 1`)
  holds only a derived routing row in `message_subscription_pointer(name,
  correlation_key [PK], state, created_at, message_subscription_key)`. Every pointer
  column is derivable from the authoritative subscription row.
- Process/decision definitions are replicated to all partitions and are immutable
  per key.
- Routing depends on `hash % len(partitions)`: **a backup set is only restorable
  into a cluster with the same partition count.**

## Goals

- Whole-cluster backup and restore of all partition databases.
- Scenarios: disaster recovery, point-in-time rollback, environment cloning.
- No external infrastructure required: backup streams to the operator's HTTP client;
  restore streams back. The backup is "done" only when the bytes are off-cluster.
- Reuse rqlite machinery maximally; no custom snapshot formats.

## Non-goals

- Single-partition repair (restore is always all partitions from one backup set).
- Incremental backups.
- Built-in scheduling (operators use cron + curl; rqlite auto-backup remains for
  S3/MinIO environments).
- Backing up cluster topology metadata (the HashiCorp raft cluster store). Topology
  is environment-specific; only the partition count matters and lives in the manifest.

## Decisions and rationale

1. **Cross-partition consistency: reconcile on restore.** Partition snapshots are
   taken concurrently (skew: seconds), with no pause and no checkpoint protocol.
   The subscription-pointer table is treated as a disposable derived index: it is
   included in the backup (rqlite snapshots whole files; excluding a table would
   add complexity for no benefit) but wiped and rebuilt during restore. Rejected
   alternatives: quiesce-during-backup (write unavailability on every backup),
   Zeebe-style checkpoint markers (touches every cross-partition code path).
2. **Trigger/download: synchronous streaming bundle** (rqlite/etcd/Consul shape).
   Rejected alternatives: push-to-object-store as primary (S3 must not be required),
   two-phase stage-and-download (backup registry + staged-file GC + the staged
   copy dies with the node; unneeded for SQLite-sized partition files).
3. **Restore targets a live cluster only with `force=true`**; without it, restore
   is refused unless the cluster is empty (no deployed definitions, no instances).
4. **Restore loads partitions sequentially** to bound coordinator memory
   (`store.Load` holds one full partition image as a single raft entry — same
   constraint rqlite accepts for `/db/load`). Escape hatch for very large
   partitions, documented but not built now: `SetRestorePath` (place file locally,
   restart partition), already wired for auto-restore.
5. The proto's `LoadChunkRequest` is legacy — v10 exposes no chunked-load store
   method. Do not design against it.

## Operator API

- `GET /system/v1/cluster/backup` → streams `zenbpm-backup-<timestamp>.tar`.
  Callable on any node; that node coordinates.
- `POST /system/v1/cluster/restore[?force=true]` ← streams the same bundle back.
- (Amended 2026-07-06: endpoints moved from `/v1/cluster/...` to
  `/system/v1/cluster/...` — the `/v1` prefix is reserved for the business API
  documented in `openapi/api.yaml`; the operational plane lives under
  `/system`, with versioning as a sub-path for contract-carrying APIs.)
- Both endpoints live in `internal/rest` and delegate to the cluster node
  (`ZenNode`) — thin HTTP shims only.
- gRPC equivalents on `ZenService` for gRPC-native operators:
  - `ClusterBackup(ClusterBackupRequest) returns (stream BackupChunk)`
  - `ClusterRestore(stream RestoreChunk) returns (ClusterRestoreResponse)` —
    request metadata (force flag, format version) in the first chunk;
    response carries the restore report.
- Internal fan-out RPCs:
  - `PartitionBackup(PartitionBackupRequest) returns (stream BackupChunk)` —
    coordinator → partition leader; request carries partition id, vacuum/compress.
  - `PartitionRestore(stream RestoreChunk) returns (PartitionRestoreResponse)` —
    coordinator → partition leader.
- Proto changes require `make generate`; generated files are never hand-edited.

## Backup flow

1. Coordinator verifies every partition has a leader; otherwise fail fast.
2. Fan out concurrently to all partition leaders: each runs
   `store.Backup(BINARY, vacuum=true, leader=true)` into the gRPC chunk stream.
   The leader computes sha256 over the bytes it sends and emits it in the final
   chunk, so the digest is anchored at the source, not at the coordinator.
   (Snapshot consistency on the leader is rqlite's: `VACUUM INTO` under a read
   transaction — writes continuing during backup cannot appear partially.)
3. Each partition's gzipped chunk stream is spooled to a coordinator temp file
   while being fed through a sha256 hasher (tar headers need the entry size up
   front, so pure pass-through streaming into a tar is not possible). When the
   stream ends, the coordinator verifies its computed digest against the
   leader-sent one — a mismatch aborts the backup loudly instead of poisoning
   the bundle. As each verified spool completes, its tar header + bytes are
   written to the HTTP response and the spool file is deleted. Snapshots still *start* concurrently, so
   point-in-time skew between partitions stays seconds even when transfers take
   minutes. Worst-case coordinator temp disk is the bundle size — the same
   requirement restore already imposes. The archive itself is not gzipped: each
   partition file already is (rqlite streams gzip when `compress=true`), so
   outer compression would be wasted work.
4. Bundle layout:

   ```
   zenbpm-backup-<ts>.tar
   ├── partition-1.db.gz
   ├── partition-2.db.gz
   ├── ...
   └── manifest.json        (last entry — carries checksums computed while streaming)
   ```

5. `manifest.json` fields: manifest format version, zenbpm version, partition
   count, and per partition: schema/migration version, snapshot timestamp (unix
   millis), stored size, sha256 of the stored (gzipped) partition file bytes —
   checksums exist to detect corruption of the bundle, so they cover the bytes
   as stored.
6. A backup whose stream fails mid-way is aborted; the client sees a truncated
   tar with no manifest, which restore rejects. Retry is cheap.

## Restore flow

1. Coordinator spools the uploaded tar to its temp directory (deleted afterwards),
   then validates **before any destructive action**: manifest present and parseable,
   partition count matches the cluster, sha256 checksums, backup schema version ≤
   binary's schema version, every partition file a valid SQLite image per
   `IsValidSQLiteData` (checked on the gunzipped stream from the spooled file).
2. Empty-cluster check: refused with a clear error if the cluster holds any
   deployed definitions or instances, unless `force=true`.
3. **Maintenance gate**: a raft-replicated cluster-state flag moves all partitions
   to a `Restoring` state. Client-facing operations return Unavailable; job
   manager, timers, and engine processing pause. Reuses the same gating mechanism
   as the schema-gated `INITIALIZED` state.
4. Per partition, sequentially: send the image to the partition leader, which
   calls `store.Load()`; raft replicates the load to followers.
5. Re-run schema migrations on every partition (the loaded file may carry an
   older schema than the running binary).
6. Run reconciliation (below).
7. Un-gate partitions; return the restore report in the response.

Failure after the first `Load` leaves the cluster in `Restoring` state with a
clear error. The operator retries the restore; the operation is idempotent
(every step re-loads/rebuilds from the bundle). There is no automatic rollback
of a failed restore.

Accepted, documented quirk: workers holding pre-restore job keys receive
NotFound on complete/fail — inherent to any rollback.

## Reconciliation (restore-time)

Runs after all partitions are loaded and migrated, before un-gating:

1. **Pointer rebuild.** Wipe every partition's `message_subscription_pointer`
   table. Scan every partition's `message_subscription` rows in ACTIVE state
   (covers definition-level subscriptions — empty correlation key — as well).
   For each row compute the pointer's home partition via
   `GetPartitionIdForMessageSubscriptionPointer(name, correlationKey)`, then
   per home partition apply one raft-replicated transaction:
   `DELETE FROM message_subscription_pointer` + batch insert of recomputed rows
   (pointer `created_at` takes the subscription's `created_at`).
   - Duplicate policy: if two ACTIVE subscriptions share `(name, correlationKey)`
     (possible from snapshot skew), the newest `created_at` wins the pointer; the
     loser is listed in the restore report as a potentially-waiting instance
     (visible and cancellable, not silently lost). This re-establishes the same
     one-active-per-key invariant the runtime enforces in
     `partition_persistence.go` `SaveMessageSubscriptionPointer`.
   - This repairs both skew anomalies: dangling pointers vanish with the wipe;
     missing pointers are recreated from the surviving subscription.
2. **Definition sync.** Diff process/decision definition keys across partitions;
   copy missing definitions to partitions lacking them (immutable per key, so
   copying is safe).
3. **Restore report** (returned to the operator and logged): per-partition load
   timings, pointers rebuilt count, duplicate conflicts with losing subscription
   keys, definitions synced.

Reads during reconciliation go through partition queries (never the engine);
writes go through each partition's raft log.

## Testing (TDD order)

- Unit (`internal/cluster/...`): manifest serialization round-trip; checksum
  verification; partition-count mismatch rejection; pointer-rebuild logic
  including duplicate policy (table-driven); empty-cluster detection.
- Store-level: backup → load round-trip on a test partition
  (`newMustTestStore()` pattern).
- E2E (`test/e2e/`): multi-node cluster → deploy definitions + start instances
  with cross-partition message subscriptions → take backup → mutate state →
  restore with `force` → assert: instances resume, publishing a message
  correlates correctly through rebuilt pointers, dangling/missing pointer
  scenarios are repaired, restore without `force` on a non-empty cluster is
  refused, restore with mismatched partition count is refused.

## Implementation phases

1. **Backup path**: proto streaming RPCs + `make generate`; `PartitionBackup`
   handler wrapping `store.Backup`; coordinator fan-out + tar/manifest;
   REST download endpoint.
2. **Restore path**: spool + validation; maintenance gate (`Restoring` state);
   sequential `store.Load` per partition; migration re-run; REST upload endpoint
   with `force`.
3. **Reconciliation**: pointer rebuild + definition sync + restore report.
4. **E2E coverage** and operator documentation (`docs/`), including the
   auto-backup (S3) interplay: independent per-partition uploads have interval-
   sized skew; the same reconciliation applies when restoring from them.

## Similar systems considered

- **rqlite**: sync `GET /db/backup` stream + auto-backup to S3; `/db/load` for
  running-cluster restore. Adopted as the per-partition primitive.
- **Zeebe** (closest analog): async backup ID pushed to shared object storage,
  checkpoint markers for causal consistency, same-partition-count restore
  requirement. Rejected S3 dependency and checkpoint complexity; kept the
  same-partition-count rule and the manifest/report ideas.
- **etcd / Consul**: sync snapshot streamed to client; scheduling delegated to
  an external agent. Adopted this trigger/download shape.
- **CockroachDB**: nodes push to object storage with MVCC `AS OF SYSTEM TIME`
  consistency — requires global timestamps we don't have.
