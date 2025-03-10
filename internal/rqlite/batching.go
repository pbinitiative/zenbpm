package rqlite

import (
	"context"

	"github.com/pbinitiative/zenbpm/internal/log"
	"github.com/rqlite/rqlite/v8/command/proto"
)

var cache = make(map[int64][]*proto.Statement)

func (persistence *BpmnEnginePersistenceRqlite) addToTransaction(key int64, stmt *proto.Statement) {
	cache[key] = append(cache[key], stmt)
}

// Flushes the transaction effectively writing the sql commands batch to the database cluster
func (persistence *BpmnEnginePersistenceRqlite) FlushTransaction(ctx context.Context) error {
	key := ctx.Value("executionKey").(int64)
	stmts, ok := cache[key]
	if !ok {
		log.Debugf(ctx, "Transaction with key %d not found", key)
		return nil
	}

	_, err := executeStatements(ctx, stmts, persistence.store)
	if err != nil {
		log.Errorf(ctx, "Failed to execute statements for key %d: %v", key, err)
		return err
	}

	delete(cache, key)
	return nil

}
