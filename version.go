// Package zenbpm exposes metadata shared by ZenBPM binaries.
package zenbpm

import (
	_ "embed"
	"strings"
)

//go:embed VERSION
var embeddedVersion string

// Version returns the application version stored in the repository's VERSION file.
func Version() string {
	return strings.TrimSpace(embeddedVersion)
}
