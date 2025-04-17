package sql

import (
	"embed"
	"fmt"
	"path/filepath"
	"strings"
)

//go:embed migrations
var migrations embed.FS

func GetMigrations() ([]string, error) {
	migDir, err := migrations.ReadDir("migrations")
	if err != nil {
		return nil, fmt.Errorf("failed to read migration directory: %w", err)
	}
	res := make([]string, 0, len(migDir))
	for _, f := range migDir {
		if !strings.HasSuffix(f.Name(), ".sql") {
			continue
		}
		if f.IsDir() {
			continue
		}
		content, err := migrations.ReadFile(filepath.Join("migrations", f.Name()))
		if err != nil {
			return nil, fmt.Errorf("failed to read %s: %w", f.Name(), err)
		}
		res = append(res, string(content))
	}
	return res, nil
}
