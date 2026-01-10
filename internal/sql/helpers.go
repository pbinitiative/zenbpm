package sql

import (
	"database/sql"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
)

func ToNullString[S ~string](p *S) sql.NullString {
	if p == nil {
		return sql.NullString{
			Valid: false,
		}
	}
	return sql.NullString{
		String: string(ptr.Deref(p, "")),
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
		Int64: int64(ptr.Deref(p, 0)),
		Valid: true,
	}
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

	return ptr.To(Sort(string(*sortBy) + "_" + order))
}
