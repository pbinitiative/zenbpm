package zenclient

import (
	"log/slog"
)

type Logger interface {
	// Error consumes a message format and
	Error(msg string)
}

// InfoLogger is an optional extension of Logger. Loggers that implement it
// receive informational messages (e.g. reconnection progress) at info level;
// loggers that only implement Logger receive them via Error for backward compatibility.
type InfoLogger interface {
	Info(msg string)
}

type DefLogger struct {
	logger *slog.Logger
}

func (l *DefLogger) Error(msg string) {
	l.logger.Error(msg)
}

func (l *DefLogger) Info(msg string) {
	l.logger.Info(msg)
}
