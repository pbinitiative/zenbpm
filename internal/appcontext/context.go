package appcontext

import (
	"context"
)

type EXECUTION_CONTEXT string

var (
	ExecutionKey EXECUTION_CONTEXT = "executionKey"
)

func GetExucutionContext(ctx context.Context) (int64, bool) {
	executionContextKey := ctx.Value(ExecutionKey)
	if executionContextKey == nil {
		return 0, false
	}
	return executionContextKey.(int64), true
}
