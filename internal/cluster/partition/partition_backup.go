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
