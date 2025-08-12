package sql

import (
	"embed"
	"fmt"
	"strings"
)

//go:embed migrations
var migrations embed.FS

type Migrations []MigrationData

type MigrationData struct {
	Filename string
	SQL      string
}

func GetMigrations() (Migrations, error) {
	migDir, err := migrations.ReadDir("migrations")
	if err != nil {
		return nil, fmt.Errorf("failed to read migration directory: %w", err)
	}
	res := make(Migrations, 0, len(migDir))
	for _, f := range migDir {
		if !strings.HasSuffix(f.Name(), ".sql") {
			continue
		}
		if f.IsDir() {
			continue
		}
		content, err := migrations.ReadFile("migrations/" + f.Name())
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
