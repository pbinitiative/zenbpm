package safego

import (
	"fmt"
	"log/slog"
	"runtime/debug"
)

type Logger interface {
	Error(msg string, args ...interface{})
}

func Go(name string, logger Logger, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Error(fmt.Sprintf("safego: panic in %s", name), "panic", r, "stack", string(debug.Stack()))
			}
		}()
		fn()
	}()
}

var DefaultLogger Logger = slogLogger{}

type slogLogger struct{}

func (slogLogger) Error(msg string, args ...interface{}) {
	slog.Error(msg, args...)
}

func Run(name string, logger Logger, fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			stack := string(debug.Stack())
			logger.Error(fmt.Sprintf("safego: panic in %s", name), "panic", r, "stack", stack)
			err = fmt.Errorf("panic in %s: %v\n%s", name, r, stack)
		}
	}()
	return fn()
}
