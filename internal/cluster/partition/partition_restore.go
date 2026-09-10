package partition

import (
	"context"
	"errors"
	"fmt"

	"github.com/pbinitiative/zenbpm/pkg/bpmn"
	"github.com/rqlite/rqlite/v10/command/proto"
)

// ErrPartitionFenced is returned for a write that reaches a partition fenced
// for a cluster restore without carrying the restore token of the current
// owner. Application writes can therefore never race with a restore, however
// late a node learns about the restore.
var ErrPartitionFenced = errors.New("partition is fenced for cluster restore")

// RestoreToken identifies the restore operation that owns the cluster. The
// restore coordinator stamps it on every request; a fenced partition only
// accepts writes whose token matches its own.
type RestoreToken struct {
	OperationID string
	Epoch       uint64
}

func (t RestoreToken) String() string {
	return fmt.Sprintf("%s/%d", t.OperationID, t.Epoch)
}

type restoreTokenContextKey struct{}

// WithRestoreToken returns a context whose writes are allowed through the
// restore fence of a partition fenced for the same token.
func WithRestoreToken(ctx context.Context, token RestoreToken) context.Context {
	return context.WithValue(ctx, restoreTokenContextKey{}, token)
}

// RestoreTokenFromContext returns the restore token carried by ctx, if any.
func RestoreTokenFromContext(ctx context.Context) (RestoreToken, bool) {
	token, ok := ctx.Value(restoreTokenContextKey{}).(RestoreToken)
	return token, ok
}

// EnterRestoreFence rejects every write that does not carry token until
// LeaveRestoreFence is called. Re-entering with a newer token replaces the
// previous one, which fences out a coordinator that lost the restore.
func (rq *DB) EnterRestoreFence(token RestoreToken) {
	rq.fenceMu.Lock()
	defer rq.fenceMu.Unlock()
	if rq.fence == nil || *rq.fence != token {
		rq.logger.Info("Partition write fence entered for cluster restore", "partition", rq.Partition, "restore", token.String())
	}
	fence := token
	rq.fence = &fence
}

// LeaveRestoreFence lets application writes through again.
func (rq *DB) LeaveRestoreFence() {
	rq.fenceMu.Lock()
	defer rq.fenceMu.Unlock()
	if rq.fence != nil {
		rq.logger.Info("Partition write fence lifted", "partition", rq.Partition, "restore", rq.fence.String())
	}
	rq.fence = nil
}

// RestoreFence returns the token the partition is currently fenced with.
func (rq *DB) RestoreFence() (RestoreToken, bool) {
	rq.fenceMu.RLock()
	defer rq.fenceMu.RUnlock()
	if rq.fence == nil {
		return RestoreToken{}, false
	}
	return *rq.fence, true
}

// checkWriteAllowedLocked enforces the restore fence for a write issued under
// ctx. The caller holds fenceMu (read) for the whole write, so a fence change
// waits for admitted writes to finish instead of racing them. A write that
// carries a restore token while no fence is installed is refused as well: a
// retired restore must never turn into an ordinary application write.
func (rq *DB) checkWriteAllowedLocked(ctx context.Context) error {
	token, tagged := RestoreTokenFromContext(ctx)
	if rq.fence == nil {
		if tagged {
			return fmt.Errorf("%w: write carries restore token %s but the partition is not fenced", ErrPartitionFenced, token.String())
		}
		return nil
	}
	if !tagged {
		return fmt.Errorf("%w (operation %s)", ErrPartitionFenced, rq.fence.String())
	}
	if token != *rq.fence {
		return fmt.Errorf("%w: write carries restore token %s but the partition is fenced for %s", ErrPartitionFenced, token.String(), rq.fence.String())
	}
	return nil
}

// LoadUnderFence runs a destructive load step while holding the fence: it is
// refused unless the partition is fenced for token at the moment the step
// starts, and a fence change (takeover, abort, release) waits until the step
// has finished. That orders the destructive step against ownership changes
// on this partition instead of trusting an admission check made earlier.
func (rq *DB) LoadUnderFence(token RestoreToken, load func() error) error {
	rq.fenceMu.RLock()
	defer rq.fenceMu.RUnlock()
	if rq.fence == nil || *rq.fence != token {
		current := "none"
		if rq.fence != nil {
			current = rq.fence.String()
		}
		return fmt.Errorf("%w: load carries restore token %s but the partition fence is %s", ErrPartitionFenced, token.String(), current)
	}
	return load()
}

// ExecuteUnderFence runs one batch of statements as a single transaction
// through the partition's raft log while holding the fence (see
// LoadUnderFence). A restore copies a database image as many such batches, so
// a fence change is ordered between two batches instead of after the whole
// copy: a superseded owner is refused from its next batch on. A statement
// that fails rolls the batch back and is reported as an error.
func (rq *DB) ExecuteUnderFence(ctx context.Context, token RestoreToken, statements []*proto.Statement) error {
	return rq.LoadUnderFence(token, func() error {
		results, err := rq.executeStatementsUnfenced(ctx, statements)
		if err != nil {
			return err
		}
		for i, result := range results {
			if msg := result.GetError(); msg != "" {
				return fmt.Errorf("statement %d failed: %s", i, msg)
			}
		}
		return nil
	})
}

// SchemaObjects lists the tables and views of the partition database in
// creation order, read from this node's copy. SQLite's internal tables are
// left out except sqlite_sequence, which a restore has to empty because it
// cannot be dropped.
func (rq *DB) SchemaObjects(ctx context.Context) (tables, views []string, err error) {
	rows, err := rq.queryDatabase(ctx, `SELECT type, name FROM sqlite_master WHERE type IN ('table', 'view') AND (name NOT LIKE 'sqlite\_%' ESCAPE '\' OR name = 'sqlite_sequence') ORDER BY rowid`)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list schema objects: %w", err)
	}
	if len(rows) != 1 {
		return nil, nil, fmt.Errorf("failed to list schema objects: expected one result set, got %d", len(rows))
	}
	for _, row := range rows[0].Values {
		if len(row.GetParameters()) != 2 {
			return nil, nil, fmt.Errorf("failed to list schema objects: malformed row %v", row)
		}
		name := row.GetParameters()[1].GetS()
		switch typ := row.GetParameters()[0].GetS(); typ {
		case "table":
			tables = append(tables, name)
		case "view":
			views = append(views, name)
		default:
			return nil, nil, fmt.Errorf("failed to list schema objects: unexpected type %q for %s", typ, name)
		}
	}
	return tables, views, nil
}

// MaintenanceStatus describes how far a locally hosted partition has entered
// (or left) restore maintenance mode. The restore coordinator uses it as a
// barrier before loading data and as a readiness check afterwards.
type MaintenanceStatus struct {
	Hosted        bool
	Leader        bool
	EngineRunning bool
	// Quiesced reports whether the engine was stopped completely for the
	// restore token the status was asked for.
	Quiesced bool
	// Fenced reports whether the partition database rejects writes that do not
	// carry the restore token the status was asked for.
	Fenced bool
	// Initialized reports whether this node's cluster state shows the partition
	// as INITIALIZED on this node.
	Initialized bool
}

// DefinitionImporter writes BPMN and DMN definitions straight into the
// partition database while the cluster is fenced for a restore. It drives an
// engine that is never started, so importing can never resume process
// execution on the partition. Close releases the engine.
type DefinitionImporter struct {
	engine *bpmn.Engine
}

// NewDefinitionImporter returns an importer bound to this partition's database.
// The partition's script runtimes are reused when present; otherwise the
// importer owns (and later releases) its own.
func (zpn *ZenPartitionNode) NewDefinitionImporter() *DefinitionImporter {
	opts := []bpmn.EngineOption{bpmn.EngineWithLogger(zpn.logger.Named("definition-importer"))}
	if zpn.FeelRuntime != nil {
		opts = append(opts, bpmn.EngineWithStorageAndFeel(zpn.DB, zpn.FeelRuntime))
	} else {
		opts = append(opts, bpmn.EngineWithStorage(zpn.DB))
	}
	if zpn.JsRuntime != nil {
		opts = append(opts, bpmn.EngineWithJs(zpn.JsRuntime))
	}
	engine := bpmn.NewEngine(opts...)
	return &DefinitionImporter{engine: &engine}
}

// ImportProcessDefinition stores a process definition under the key and
// version it has on the partition it was copied from. It is idempotent: a
// definition already stored under the key is left alone. With
// registerSubscriptions the definition-level subscriptions (timer/message
// start events, instantiating receive tasks) are created on this partition;
// the caller decides that based on the deployment ownership rule so that only
// one partition ever owns them, and the engine creates them only when the
// imported definition is the latest version of its process.
//
// A partition that already holds a different definition at the same version
// cannot take the copy without changing which definition is the latest; the
// import is refused with storage.ErrUniqueConstraint and nothing is written.
func (i *DefinitionImporter) ImportProcessDefinition(ctx context.Context, key int64, version int32, data []byte, registerSubscriptions bool) error {
	if _, err := i.engine.ImportProcessDefinition(ctx, data, key, version, registerSubscriptions); err != nil {
		return fmt.Errorf("failed to import process definition %d: %w", key, err)
	}
	return nil
}

// ImportDmnResourceDefinition stores a DMN resource definition and its
// decision definitions under the key and versions they have on the partition
// they were copied from. Like ImportProcessDefinition it is idempotent and
// refuses a version another definition already holds.
func (i *DefinitionImporter) ImportDmnResourceDefinition(ctx context.Context, key int64, version int32, data []byte, decisionVersions map[string]int32) error {
	dmnEngine := i.engine.GetDmnEngine()
	definition, err := dmnEngine.ParseDmnFromBytes("", data)
	if err != nil {
		return fmt.Errorf("failed to parse dmn resource definition %d: %w", key, err)
	}
	versions := make(map[string]int64, len(decisionVersions))
	for id, v := range decisionVersions {
		versions[id] = int64(v)
	}
	if _, _, err := dmnEngine.ImportDmnResourceDefinition(ctx, definition, data, key, int64(version), versions); err != nil {
		return fmt.Errorf("failed to import dmn resource definition %d: %w", key, err)
	}
	return nil
}

// Close releases the importer's engine. The partition database stays open.
func (i *DefinitionImporter) Close() {
	i.engine.Stop()
}
