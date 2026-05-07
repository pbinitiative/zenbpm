package safego

import (
	"fmt"
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
