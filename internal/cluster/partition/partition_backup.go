package partition

import (
	"context"
	"fmt"

	zenproto "github.com/pbinitiative/zenbpm/internal/cluster/proto"
	bpmnruntime "github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	rqproto "github.com/rqlite/rqlite/v10/command/proto"
)

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
			out = append(out, &zenproto.DefinitionRef{Key: ptr.To(key), Type: typ.Enum()})
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
		row := rq.QueryRowContext(ctx, "SELECT dmn_data FROM dmn_resource_definition WHERE key = ?", key)
		var data string
		if err := row.Scan(&data); err != nil {
			return nil, "", fmt.Errorf("failed to load dmn resource definition %d: %w", key, err)
		}
		return []byte(data), "", nil
	}
	return nil, "", fmt.Errorf("unknown definition type %v", defType)
}

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

// DataStats returns coarse row counts used by the restore empty-cluster check.
func (rq *DB) DataStats(ctx context.Context) (definitions int64, instances int64, err error) {
	row := rq.QueryRowContext(ctx,
		"SELECT (SELECT COUNT(*) FROM process_definition), (SELECT COUNT(*) FROM process_instance)")
	err = row.Scan(&definitions, &instances)
	return definitions, instances, err
}

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
		r.Key, r.Name, r.CorrelationKey, r.CreatedAt, r.State = ptr.To(key), ptr.To(name), ptr.To(ck), ptr.To(createdAt), ptr.To(state)
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
