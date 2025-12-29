package partition

import "database/sql"

func NullInt64(p *int64) sql.NullInt64 {
	if p == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{
		Int64: *p,
		Valid: true,
	}
}

func NullString(p *string) sql.NullString {
	if p == nil || *p == "" {
		return sql.NullString{}
	}
	return sql.NullString{
		String: *p,
		Valid:  true,
	}
}
