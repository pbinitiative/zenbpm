// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package client

import (
	"log/slog"
)

type Logger interface {
	// Error consumes a message format and
	Error(msg string)
}

type DefLogger struct {
	logger *slog.Logger
}

func (l *DefLogger) Error(msg string) {
	l.logger.Error(msg)
}
