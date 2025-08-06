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
