package sql

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
)

// RecoverableRunningTokensQuery exposes the sqlc-generated statement so
// query-plan tests can inspect the same SQL used by production.
const RecoverableRunningTokensQuery = getRecoverableRunningTokens

// JobHeadersFromJSON parses the JSON object stored in the job.headers column.
// An unset or empty object yields a nil map. A malformed object returns an
// error, so a corrupt value is not hidden.
func JobHeadersFromJSON(raw string) (map[string]string, error) {
	if raw == "" || raw == "{}" {
		return nil, nil
	}
	var headers map[string]string
	if err := json.Unmarshal([]byte(raw), &headers); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job headers: %w", err)
	}
	if len(headers) == 0 {
		return nil, nil
	}
	return headers, nil
}

func ToNullString[S ~string](p *S) sql.NullString {
	if p == nil {
		return sql.NullString{
			Valid: false,
		}
	}
	return sql.NullString{
		String: string(*p),
		Valid:  true,
	}
}

func ToNullInt64[I ~int64](p *I) sql.NullInt64 {
	if p == nil {
		return sql.NullInt64{
			Valid: false,
		}
	}
	return sql.NullInt64{
		Int64: int64(*p),
		Valid: true,
	}
}

func FromNullInt64(p sql.NullInt64) *int64 {
	if p.Valid {
		return &p.Int64
	}
	return nil
}

func FromNullString(p sql.NullString) *string {
	if p.Valid {
		return &p.String
	}
	return nil
}

type Sort string

func SortString[O ~string, B ~string](sortOrder *O, sortBy *B) *Sort {
	// default order is asc
	order := string(public.SortOrderAsc)
	if sortOrder != nil {
		order = string(*sortOrder)
	}
	if sortBy == nil {
		return nil
	}

	return new(Sort(string(*sortBy) + "_" + order))
}
