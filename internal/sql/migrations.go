// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

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
