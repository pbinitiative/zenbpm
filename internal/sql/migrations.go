package sql

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

const DefaultMigrationsDir = "internal/sql/migrations"
const migrationsInitFilename = "0000_init.up.sql"

type Migrations []MigrationData

type MigrationData struct {
	Filename string
	SQL      string
}

func GetUpMigrations(migrationDir string) (Migrations, error) {
	migrationDirEntries, err := readMigrationDir(migrationDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read migration directory: %w", err)
	}

	res := make(Migrations, 0, len(migrationDirEntries))
	for _, f := range migrationDirEntries {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".up.sql") {
			continue
		}
		content, err := readMigrationFile(migrationDir, f.Name())
		if err != nil {
			return nil, fmt.Errorf("failed to read %s: %w", f.Name(), err)
		}

		res = append(res, MigrationData{
			Filename: f.Name(),
			SQL:      string(content),
		})
	}
	return res, nil
}

func GetRollbackMigration(migrationDir string, migrationFilename string) (*MigrationData, error) {
	rollbackFilename := strings.Replace(migrationFilename, ".up.sql", ".down.sql", 1)

	content, err := readMigrationFile(migrationDir, rollbackFilename)
	if err != nil {
		return nil, fmt.Errorf("failed to read rollback file %s: %w", rollbackFilename, err)
	}

	return &MigrationData{
		Filename: rollbackFilename,
		SQL:      string(content),
	}, nil
}

func GetMigrationInitSql(migrationDir string) (*string, error) {
	content, err := readMigrationFile(migrationDir, migrationsInitFilename)
	if err != nil {
		return nil, fmt.Errorf("failed to read migration init file %s: %w", migrationsInitFilename, err)
	}

	fileContent := string(content)

	return &fileContent, nil
}

func readMigrationDir(migrationDir string) ([]os.DirEntry, error) {
	filesystemDir, err := filesystemMigrationDir(migrationDir)
	if err != nil {
		return nil, err
	}
	if filesystemDir == "" {
		return nil, nil
	}

	entries, err := os.ReadDir(filesystemDir)
	if err == nil {
		return entries, nil
	}
	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to read filesystem migration directory %s: %w", filesystemDir, err)
	}

	return nil, nil
}

func readMigrationFile(migrationDir string, filename string) ([]byte, error) {
	filesystemDir, err := filesystemMigrationDir(migrationDir)
	if err != nil {
		return nil, err
	}
	if filesystemDir == "" {
		return nil, nil
	}

	filePath := filepath.Join(filesystemDir, filename)
	content, err := os.ReadFile(filePath)
	if err == nil {
		return content, nil
	}
	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to read filesystem migration file %s: %w", filePath, err)
	}

	return nil, nil
}

func filesystemMigrationDir(migrationDir string) (string, error) {
	if migrationDir == "" {
		return "", nil
	}

	normalizedDir := filepath.Clean(migrationDir)
	if filepath.IsAbs(normalizedDir) {
		return normalizedDir, nil
	}

	projectRoot, err := findProjectRoot()
	if err != nil {
		return "", fmt.Errorf("failed to find project root: %w", err)
	}

	resolvedDir := filepath.Join(projectRoot, normalizedDir)
	relToRoot, err := filepath.Rel(projectRoot, resolvedDir)
	if err != nil {
		return "", fmt.Errorf("failed to resolve migration directory %s from project root %s: %w", migrationDir, projectRoot, err)
	}
	if relToRoot == ".." || strings.HasPrefix(relToRoot, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("migration directory %s escapes project root %s", migrationDir, projectRoot)
	}

	return resolvedDir, nil
}

func findProjectRoot() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("failed to get caller file")
	}

	dir := filepath.Dir(file)

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("go.mod not found")
		}

		dir = parent
	}
}
