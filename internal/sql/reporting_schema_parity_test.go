package sql

import (
	databaseSQL "database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

var (
	reportingTablePattern   = regexp.MustCompile(`(?ims)^CREATE TABLE IF NOT EXISTS\s+reporting\.([a-z_][a-z0-9_]*)\s*\((.*?)^\s*\);`)
	reportingDefaultPattern = regexp.MustCompile(`(?i)\bDEFAULT\s+(.+)$`)
)

type reportingSchemaColumn struct {
	Type               string
	Nullable           bool
	Default            string
	PrimaryKeyPosition int
}

type reportingSchemaTable map[string]reportingSchemaColumn

func TestReportingSchemaParity(t *testing.T) {
	t.Run("PostgreSQL reporting tables match the migrated SQLite schema", func(t *testing.T) {
		runtimeSchema, err := migratedSQLiteSchema()
		require.NoError(t, err)

		projectRoot, err := findProjectRoot()
		require.NoError(t, err)

		postgresInit, err := os.ReadFile(filepath.Join(projectRoot, ".dev", "postgres-init.sql"))
		require.NoError(t, err)

		reportingSchema, err := parseReportingSchema(string(postgresInit))
		require.NoError(t, err)
		delete(reportingSchema, "cdc_event")

		require.Equal(t, sortedTableNames(runtimeSchema), sortedTableNames(reportingSchema),
			"reporting projection tables must match runtime tables")
		for _, tableName := range sortedTableNames(runtimeSchema) {
			require.Equal(t, runtimeSchema[tableName], reportingSchema[tableName],
				"reporting projection for table %s must preserve column types, nullability, defaults, and primary keys", tableName)
		}
	})
}

func migratedSQLiteSchema() (_ map[string]reportingSchemaTable, resultErr error) {
	db, err := databaseSQL.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("open in-memory SQLite database: %w", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("close in-memory SQLite database: %w", err))
		}
	}()
	db.SetMaxOpenConns(1)

	migrations, err := GetUpMigrations(DefaultMigrationsDir)
	if err != nil {
		return nil, fmt.Errorf("load migrations: %w", err)
	}
	for _, migration := range migrations {
		if _, err := db.Exec(migration.SQL); err != nil {
			return nil, fmt.Errorf("apply migration %s: %w", migration.Filename, err)
		}
	}

	rows, err := db.Query(`
		SELECT name
		FROM sqlite_schema
		WHERE type = 'table'
		  AND name != 'migration'
		  AND name NOT LIKE 'sqlite_%'
		ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list runtime tables: %w", err)
	}

	var tableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			scanErr := fmt.Errorf("scan runtime table name: %w", err)
			if closeErr := rows.Close(); closeErr != nil {
				scanErr = errors.Join(scanErr, fmt.Errorf("close runtime table rows after scan failure: %w", closeErr))
			}
			return nil, scanErr
		}
		tableNames = append(tableNames, tableName)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close runtime table rows: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate runtime tables: %w", err)
	}

	schema := make(map[string]reportingSchemaTable, len(tableNames))
	for _, tableName := range tableNames {
		columns, err := sqliteTableColumns(db, tableName)
		if err != nil {
			return nil, err
		}
		schema[tableName] = columns
	}
	return schema, nil
}

func sqliteTableColumns(db *databaseSQL.DB, tableName string) (_ reportingSchemaTable, resultErr error) {
	quotedTableName := `"` + strings.ReplaceAll(tableName, `"`, `""`) + `"`
	rows, err := db.Query("PRAGMA table_info(" + quotedTableName + ")")
	if err != nil {
		return nil, fmt.Errorf("inspect SQLite table %s: %w", tableName, err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("close SQLite column rows for table %s: %w", tableName, err))
		}
	}()

	columns := reportingSchemaTable{}
	for rows.Next() {
		var (
			columnID           int
			columnName         string
			columnType         string
			notNull            int
			defaultValue       databaseSQL.NullString
			primaryKeyPosition int
		)
		if err := rows.Scan(&columnID, &columnName, &columnType, &notNull, &defaultValue, &primaryKeyPosition); err != nil {
			return nil, fmt.Errorf("scan SQLite column for table %s: %w", tableName, err)
		}

		columns[columnName] = reportingSchemaColumn{
			Type:               postgresTypeForSQLite(columnType),
			Nullable:           notNull == 0 && primaryKeyPosition == 0,
			Default:            normalizeSQLDefault(defaultValue.String),
			PrimaryKeyPosition: primaryKeyPosition,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate SQLite columns for table %s: %w", tableName, err)
	}
	return columns, nil
}

func parseReportingSchema(sql string) (map[string]reportingSchemaTable, error) {
	matches := reportingTablePattern.FindAllStringSubmatch(sql, -1)
	if len(matches) == 0 {
		return nil, fmt.Errorf("no reporting tables found")
	}

	schema := make(map[string]reportingSchemaTable, len(matches))
	for _, match := range matches {
		tableName := match[1]
		if _, exists := schema[tableName]; exists {
			return nil, fmt.Errorf("reporting table %s is declared more than once", tableName)
		}

		columns := reportingSchemaTable{}
		var compositePrimaryKey []string
		for _, rawLine := range strings.Split(match[2], "\n") {
			line := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(rawLine), ","))
			if line == "" || strings.HasPrefix(line, "--") {
				continue
			}

			upperLine := strings.ToUpper(line)
			if strings.HasPrefix(upperLine, "PRIMARY KEY") {
				openParen := strings.Index(line, "(")
				closeParen := strings.LastIndex(line, ")")
				if openParen == -1 || closeParen <= openParen {
					return nil, fmt.Errorf("parse primary key for reporting table %s: %q", tableName, line)
				}
				for _, columnName := range strings.Split(line[openParen+1:closeParen], ",") {
					compositePrimaryKey = append(compositePrimaryKey, strings.Trim(strings.TrimSpace(columnName), `"`))
				}
				continue
			}
			if strings.HasPrefix(upperLine, "CONSTRAINT ") || strings.HasPrefix(upperLine, "FOREIGN KEY") ||
				strings.HasPrefix(upperLine, "UNIQUE ") || strings.HasPrefix(upperLine, "CHECK ") {
				continue
			}

			fields := strings.Fields(line)
			if len(fields) < 2 {
				return nil, fmt.Errorf("parse column for reporting table %s: %q", tableName, line)
			}
			columnName := strings.Trim(fields[0], `"`)
			defaultValue := ""
			if defaultMatch := reportingDefaultPattern.FindStringSubmatch(line); defaultMatch != nil {
				defaultValue = normalizeSQLDefault(defaultMatch[1])
			}
			primaryKeyPosition := 0
			if strings.Contains(upperLine, "PRIMARY KEY") {
				primaryKeyPosition = 1
			}
			columns[columnName] = reportingSchemaColumn{
				Type:               strings.ToUpper(fields[1]),
				Nullable:           !strings.Contains(upperLine, "NOT NULL") && primaryKeyPosition == 0,
				Default:            defaultValue,
				PrimaryKeyPosition: primaryKeyPosition,
			}
		}

		for index, columnName := range compositePrimaryKey {
			column, ok := columns[columnName]
			if !ok {
				return nil, fmt.Errorf("primary key column %s is missing from reporting table %s", columnName, tableName)
			}
			column.Nullable = false
			column.PrimaryKeyPosition = index + 1
			columns[columnName] = column
		}
		schema[tableName] = columns
	}
	return schema, nil
}

func postgresTypeForSQLite(sqliteType string) string {
	switch strings.ToUpper(sqliteType) {
	case "INTEGER":
		return "BIGINT"
	case "TEXT":
		return "TEXT"
	case "BLOB":
		return "BYTEA"
	default:
		return strings.ToUpper(sqliteType)
	}
}

func normalizeSQLDefault(value string) string {
	return strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(value), ","))
}

func sortedTableNames(schema map[string]reportingSchemaTable) []string {
	names := make([]string, 0, len(schema))
	for name := range schema {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
