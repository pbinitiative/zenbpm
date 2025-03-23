package storage

import (
	"context"
	"github.com/rqlite/rqlite/v8/command/proto"
)

// PersistentStorage
// Deprecated: to be replaced by PersistentStorageNew
type PersistentStorage interface {
	// TODO: once the storage interface is considered done, remove Query, Execute, and IsLeader
	Query(ctx context.Context, req *proto.QueryRequest) ([]*proto.QueryRows, error)
	Execute(ctx context.Context, req *proto.ExecuteRequest) ([]*proto.ExecuteQueryResponse, error)
	IsLeader(ctx context.Context) bool
}
