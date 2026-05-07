package safego

import (
	"context"
	"fmt"
	"runtime/debug"
)

type Logger interface {
	Error(msg string, args ...interface{})
}

func Go(ctx context.Context, name string, logger Logger, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Error(fmt.Sprintf("safego: panic in %s: %v\n%s", name, r, debug.Stack()))
			}
		}()
		fn()
	}()
}
