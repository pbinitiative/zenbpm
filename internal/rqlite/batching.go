package rqlite

import (
	"context"
	"errors"
	"sync"

	"github.com/pbinitiative/zenbpm/internal/log"
	"github.com/rqlite/rqlite/v8/command/proto"
)

var (
	cache = make(map[int64][]*proto.Statement)
	mu    sync.Mutex // Only protects the cache map itself
)

// Adds a statement to an executionKey's transaction
func (persistence *BpmnEnginePersistenceRqlite) addToTransaction(key int64, stmt *proto.Statement) {
	mu.Lock()
	if _, exists := cache[key]; !exists {
		cache[key] = []*proto.Statement{} // Initialize slice if first entry
	}
	mu.Unlock() // Unlock immediately after modifying the map

	// Safe to append since only one goroutine handles each key
	cache[key] = append(cache[key], stmt)
}

// Flushes the transaction, writing the SQL batch to the database
func (persistence *BpmnEnginePersistenceRqlite) FlushTransaction(ctx context.Context) error {
	key, ok := ctx.Value("executionKey").(int64)
	if !ok {
		return errors.New("No executionKey found in context")
	}

	mu.Lock()
	stmts, exists := cache[key]
	if !exists {
		mu.Unlock()
		log.Debugf(ctx, "Transaction with key %d not found", key)
		return nil
	}
	delete(cache, key) // Remove the key safely
	mu.Unlock()

	_, err := executeStatements(ctx, stmts, persistence.store)
	if err != nil {
		log.Errorf(ctx, "Failed to execute statements for key %d: %v", key, err)
		return err
	}

	return nil
}
