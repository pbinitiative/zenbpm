package rqlite

import (
	"context"
	"errors"

	"github.com/pbinitiative/zenbpm/internal/appcontext"
	"github.com/pbinitiative/zenbpm/internal/log"
	"github.com/rqlite/rqlite/v8/command/proto"
)

// Adds a statement to an executionKey's transaction
func (persistence *PersistenceRqlite) addToTransaction(key int64, stmt *proto.Statement) {
	persistence.mu.Lock()
	if _, exists := persistence.cache[key]; !exists {
		persistence.cache[key] = []*proto.Statement{} // Initialize slice if first entry
	}
	persistence.mu.Unlock() // Unlock immediately after modifying the map

	// Safe to append since only one goroutine handles each key
	persistence.cache[key] = append(persistence.cache[key], stmt)
}

// Flushes the transaction, writing the SQL batch to the database
func (persistence *PersistenceRqlite) FlushTransaction(ctx context.Context) error {
	key, ok := ctx.Value(appcontext.ExecutionKey).(int64)
	if !ok {
		return errors.New("No executionKey found in context")
	}

	persistence.mu.Lock()
	stmts, exists := persistence.cache[key]
	if !exists {
		persistence.mu.Unlock()
		log.Debugf(ctx, "Transaction with key %d not found", key)
		return nil
	}
	_, err := executeStatements(ctx, stmts, persistence.store)
	if err != nil {
		log.Errorf(ctx, "Failed to execute statements for key %d: %v", key, err)
		return err
	}
	delete(persistence.cache, key) // Remove the key safely
	persistence.mu.Unlock()

	return nil
}
