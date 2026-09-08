package partition

import (
	"context"
	"errors"
	"fmt"

	"github.com/pbinitiative/zenbpm/pkg/bpmn"
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

// LoadUnderFence runs a destructive load while holding the fence: it is
// refused unless the partition is fenced for token at the moment the load
// starts, and a fence change (takeover, abort, release) waits until the load
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

// ImportProcessDefinition stores a process definition under the given key. It
// is idempotent: a definition with identical content that already exists is
// left alone. With registerSubscriptions the definition-level subscriptions
// (timer/message start events, instantiating receive tasks) are created on
// this partition; the caller decides that based on the deployment ownership
// rule so that only one partition ever owns them.
func (i *DefinitionImporter) ImportProcessDefinition(ctx context.Context, key int64, data []byte, registerSubscriptions bool) error {
	definition, err := i.engine.LoadFromBytes(ctx, data, key)
	if err != nil {
		return fmt.Errorf("failed to import process definition %d: %w", key, err)
	}
	if !registerSubscriptions {
		return nil
	}
	if err := i.engine.RegisterProcessDefinitionSubscriptions(ctx, definition.Key); err != nil {
		return fmt.Errorf("failed to register subscriptions of imported process definition %d: %w", definition.Key, err)
	}
	return nil
}

// ImportDmnResourceDefinition stores a DMN resource definition and its
// decision definitions under the given key.
func (i *DefinitionImporter) ImportDmnResourceDefinition(ctx context.Context, key int64, data []byte) error {
	dmnEngine := i.engine.GetDmnEngine()
	definition, err := dmnEngine.ParseDmnFromBytes("", data)
	if err != nil {
		return fmt.Errorf("failed to parse dmn resource definition %d: %w", key, err)
	}
	if _, _, err := dmnEngine.SaveDmnResourceDefinition(ctx, definition, data, key); err != nil {
		return fmt.Errorf("failed to import dmn resource definition %d: %w", key, err)
	}
	return nil
}

// Close releases the importer's engine. The partition database stays open.
func (i *DefinitionImporter) Close() {
	i.engine.Stop()
}
