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

// DataStats returns coarse row counts used by the restore empty-cluster check.
func (rq *DB) DataStats(ctx context.Context) (definitions int64, instances int64, err error) {
	row := rq.QueryRowContext(ctx,
		"SELECT (SELECT COUNT(*) FROM process_definition), (SELECT COUNT(*) FROM process_instance)")
	err = row.Scan(&definitions, &instances)
	return definitions, instances, err
}
