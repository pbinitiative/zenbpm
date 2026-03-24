package partition

import (
	"testing"

	"github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/stretchr/testify/assert"
)

func TestFilterPendingMigrationsReturnsPendingMigrations(t *testing.T) {
	fileMigrations := sql.Migrations{
		{Filename: "0001_schema.up.sql"},
		{Filename: "0002_indexes.up.sql"},
		{Filename: "0003_data.up.sql"},
	}
	dbMigrations := []sql.Migration{
		{Name: "0001_schema.up.sql"},
		{Name: "0002_indexes.up.sql"},
	}

	pending := filterPendingMigrations(fileMigrations, dbMigrations)
	assert.Equal(t, sql.Migrations{{Filename: "0003_data.up.sql"}}, pending)
}

func TestFilterPendingMigrationsDoesNotValidateDBOrder(t *testing.T) {
	fileMigrations := sql.Migrations{
		{Filename: "0001_schema.up.sql"},
		{Filename: "0002_indexes.up.sql"},
		{Filename: "0003_data.up.sql"},
	}
	dbMigrations := []sql.Migration{
		{Name: "0001_schema.up.sql"},
		{Name: "0003_data.up.sql"},
	}

	pending := filterPendingMigrations(fileMigrations, dbMigrations)
	assert.Equal(t, sql.Migrations{{Filename: "0002_indexes.up.sql"}}, pending)
}

func TestFilterPendingMigrationsIgnoresDuplicateDBRecords(t *testing.T) {
	fileMigrations := sql.Migrations{
		{Filename: "0001_schema.up.sql"},
		{Filename: "0002_indexes.up.sql"},
	}
	dbMigrations := []sql.Migration{
		{Name: "0001_schema.up.sql"},
		{Name: "0001_schema.up.sql"},
	}

	pending := filterPendingMigrations(fileMigrations, dbMigrations)
	assert.Equal(t, sql.Migrations{{Filename: "0002_indexes.up.sql"}}, pending)
}

func TestFilterPendingMigrationsIgnoresUnknownDBMigrations(t *testing.T) {
	fileMigrations := sql.Migrations{
		{Filename: "0001_schema.up.sql"},
		{Filename: "0002_indexes.up.sql"},
	}
	dbMigrations := []sql.Migration{
		{Name: "0001_schema.up.sql"},
		{Name: "9999_custom.up.sql"},
	}

	pending := filterPendingMigrations(fileMigrations, dbMigrations)
	assert.Equal(t, sql.Migrations{{Filename: "0002_indexes.up.sql"}}, pending)
}
