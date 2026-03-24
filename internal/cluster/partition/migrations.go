package partition

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/pbinitiative/zenbpm/internal/sql"
	rqproto "github.com/rqlite/rqlite/v8/command/proto"
)

func (db *DB) RunMigrations(ctx context.Context) error {
	if err := ensureMigrationTable(ctx, db); err != nil {
		return err
	}

	pendingMigrations, err := loadPendingMigrations(ctx, db)
	if err != nil {
		return err
	}

	for _, migration := range pendingMigrations {
		if migrationErr := executeSingleMigration(ctx, db, migration); migrationErr != nil {
			return migrationErr
		}
	}

	return nil
}

func loadPendingMigrations(ctx context.Context, db *DB) (sql.Migrations, error) {
	fileMigrations, err := sql.GetUpMigrations(db.migrationDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read migrations: %w", err)
	}

	appliedMigrations, err := db.Queries.GetMigrations(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read applied migrations: %w", err)
	}

	return filterPendingMigrations(fileMigrations, appliedMigrations), nil
}

func filterPendingMigrations(fileMigrations sql.Migrations, appliedMigrations []sql.Migration) sql.Migrations {
	appliedMigrationNames := make(map[string]struct{}, len(appliedMigrations))

	for _, appliedMigration := range appliedMigrations {
		appliedMigrationNames[appliedMigration.Name] = struct{}{}
	}

	pendingMigrations := make(sql.Migrations, 0, len(fileMigrations))
	for _, fileMigration := range fileMigrations {
		if _, ok := appliedMigrationNames[fileMigration.Filename]; ok {
			continue
		}

		pendingMigrations = append(pendingMigrations, fileMigration)
	}

	slices.SortFunc(pendingMigrations, func(a, b sql.MigrationData) int {
		return strings.Compare(a.Filename, b.Filename)
	})

	return pendingMigrations
}

func executeSingleMigration(ctx context.Context, db *DB, migration sql.MigrationData) error {
	if migrationErr := executeUpMigration(ctx, db, migration); migrationErr != nil {
		if err := executeRollbackMigration(ctx, db, migration); err != nil {
			return err
		}

		return migrationErr
	}

	err := db.Queries.SaveMigration(ctx, sql.SaveMigrationParams{
		Name:  migration.Filename,
		RanAt: time.Now().UnixMilli(),
	})
	if err != nil {
		return fmt.Errorf("failed to save migration info: %w", err)
	}

	db.logger.Info(fmt.Sprintf("Migration applied: %s", migration.Filename))

	return nil
}

func executeUpMigration(ctx context.Context, db *DB, migration sql.MigrationData) error {
	if err := executeSQLStatement(ctx, db, migration.SQL); err != nil {
		return fmt.Errorf("failed to execute migration %s: %w", migration.Filename, err)
	}

	return nil
}

func executeRollbackMigration(ctx context.Context, db *DB, migration sql.MigrationData) error {
	rollbackMigration, err := sql.GetRollbackMigration(db.migrationDir, migration.Filename)
	if err != nil {
		return fmt.Errorf("failed to read rollback migration: %w", err)
	}

	if rollbackMigration == nil {
		return nil
	}

	db.logger.Info(fmt.Sprintf("Rolling back migration: %s", rollbackMigration.Filename))
	if sqlStatementErr := executeSQLStatement(ctx, db, rollbackMigration.SQL); sqlStatementErr != nil {
		return fmt.Errorf("failed to execute rollback migration %s: %w", rollbackMigration.Filename, sqlStatementErr)
	}

	db.logger.Info(fmt.Sprintf("Rollback migration applied: %s", rollbackMigration.Filename))

	return nil
}

func ensureMigrationTable(ctx context.Context, db *DB) error {

	createMigrationTableSQL, err := sql.GetMigrationInitSql(db.migrationDir)

	if err != nil {
		return err
	}

	if createMigrationTableSQL == nil {
		return fmt.Errorf("failed to create migration table, failed to read migration init file")
	}

	createTableSQL := *createMigrationTableSQL

	if sqlStatementErr := executeSQLStatement(ctx, db, createTableSQL); sqlStatementErr != nil {
		return fmt.Errorf("failed to create migration table: %w", sqlStatementErr)
	}

	return nil
}

func executeSQLStatement(ctx context.Context, db *DB, statementSQL string) error {
	results, err := db.ExecuteStatements(ctx, []*rqproto.Statement{{Sql: statementSQL}})
	if err != nil {
		return err
	}

	if len(results) == 0 || results[0] == nil {
		return nil
	}

	if executeErr := results[0].GetError(); executeErr != "" {
		return errors.New(executeErr)
	}

	return nil
}
