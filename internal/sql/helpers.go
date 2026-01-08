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
