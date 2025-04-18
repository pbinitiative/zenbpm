// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.28.0
// source: process_instance.sql

package sql

import (
	"context"
	"database/sql"
)

const findProcessInstances = `-- name: FindProcessInstances :many
SELECT "key", process_definition_key, created_at, state, variables 
FROM process_instance
WHERE COALESCE(?1, "key") = "key"
    AND COALESCE(?2, process_definition_key) = process_definition_key
ORDER BY created_at DESC
`

type FindProcessInstancesParams struct {
	Key                  sql.NullInt64 `json:"key"`
	ProcessDefinitionKey sql.NullInt64 `json:"process_definition_key"`
}

func (q *Queries) FindProcessInstances(ctx context.Context, arg FindProcessInstancesParams) ([]ProcessInstance, error) {
	rows, err := q.db.QueryContext(ctx, findProcessInstances, arg.Key, arg.ProcessDefinitionKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []ProcessInstance{}
	for rows.Next() {
		var i ProcessInstance
		if err := rows.Scan(
			&i.Key,
			&i.ProcessDefinitionKey,
			&i.CreatedAt,
			&i.State,
			&i.Variables,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getProcessInstance = `-- name: GetProcessInstance :one
SELECT "key", process_definition_key, created_at, state, variables 
FROM process_instance
WHERE key = ?1
`

func (q *Queries) GetProcessInstance(ctx context.Context, key int64) (ProcessInstance, error) {
	row := q.db.QueryRowContext(ctx, getProcessInstance, key)
	var i ProcessInstance
	err := row.Scan(
		&i.Key,
		&i.ProcessDefinitionKey,
		&i.CreatedAt,
		&i.State,
		&i.Variables,
	)
	return i, err
}

const saveProcessInstance = `-- name: SaveProcessInstance :exec
INSERT INTO process_instance (
    key, process_definition_key, created_at, state, variables
) VALUES (
    ?, ?, ?, ?, ?
)
 ON CONFLICT(key) DO UPDATE SET 
    state = excluded.state,
    variables = excluded.variables
`

type SaveProcessInstanceParams struct {
	Key                  int64  `json:"key"`
	ProcessDefinitionKey int64  `json:"process_definition_key"`
	CreatedAt            int64  `json:"created_at"`
	State                int    `json:"state"`
	Variables            string `json:"variables"`
}

func (q *Queries) SaveProcessInstance(ctx context.Context, arg SaveProcessInstanceParams) error {
	_, err := q.db.ExecContext(ctx, saveProcessInstance,
		arg.Key,
		arg.ProcessDefinitionKey,
		arg.CreatedAt,
		arg.State,
		arg.Variables,
	)
	return err
}
