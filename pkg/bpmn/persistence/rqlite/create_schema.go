package rqlite

import "context"

func (persistence *BpmnEnginePersistenceRqlite) createSchema() error {
	return persistence.queries.CreateProcessInstanceTable(context.Background())
}
