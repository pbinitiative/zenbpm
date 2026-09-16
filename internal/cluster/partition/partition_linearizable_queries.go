package partition

import (
	"context"

	"github.com/pbinitiative/zenbpm/internal/sql"
)

// linearizableDBTX runs the generated queries at LINEARIZABLE read
// consistency (see DB.QueryContextLinearizable); writes and prepared
// statements are the partition's own.
type linearizableDBTX struct {
	*DB
}

func (l linearizableDBTX) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return l.DB.QueryContextLinearizable(ctx, query, args...)
}

func (l linearizableDBTX) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	rows, err := l.QueryContext(ctx, query, args...)
	if err != nil {
		return sql.ConstructRow(ctx, []string{}, []string{}, nil, err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		return sql.ConstructRow(ctx, []string{}, []string{}, nil, sql.ErrNoRows)
	}
	return sql.ConstructRowFromRows(ctx, rows, nil)
}
