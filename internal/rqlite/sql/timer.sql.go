// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.28.0
// source: timer.sql

package sql

import (
	"context"
	"database/sql"
)

const findTimers = `-- name: FindTimers :many
SELECT key, element_id, element_instance_key, process_definition_key, process_instance_key, state, created_at, due_at, duration
FROM timer
WHERE COALESCE(?1, process_instance_key) = process_instance_key AND
  COALESCE(?2, "element_instance_key") = "element_instance_key" AND
  (?3 IS  NULL OR
    "state" IN (SELECT value FROM json_each(?3)))
`

type FindTimersParams struct {
	ProcessInstanceKey sql.NullInt64 `json:"process_instance_key"`
	ElementInstanceKey sql.NullInt64 `json:"element_instance_key"`
	States             interface{}   `json:"states"`
}

type FindTimersRow struct {
	Key                  int64  `json:"key"`
	ElementID            string `json:"element_id"`
	ElementInstanceKey   int64  `json:"element_instance_key"`
	ProcessDefinitionKey int64  `json:"process_definition_key"`
	ProcessInstanceKey   int64  `json:"process_instance_key"`
	State                int    `json:"state"`
	CreatedAt            int64  `json:"created_at"`
	DueAt                int64  `json:"due_at"`
	Duration             int64  `json:"duration"`
}

func (q *Queries) FindTimers(ctx context.Context, arg FindTimersParams) ([]FindTimersRow, error) {
	rows, err := q.db.QueryContext(ctx, findTimers, arg.ProcessInstanceKey, arg.ElementInstanceKey, arg.States)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []FindTimersRow{}
	for rows.Next() {
		var i FindTimersRow
		if err := rows.Scan(
			&i.Key,
			&i.ElementID,
			&i.ElementInstanceKey,
			&i.ProcessDefinitionKey,
			&i.ProcessInstanceKey,
			&i.State,
			&i.CreatedAt,
			&i.DueAt,
			&i.Duration,
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

const saveTimer = `-- name: SaveTimer :exec
INSERT INTO timer
(key, element_id, element_instance_key, process_definition_key, process_instance_key, state, created_at, due_at, duration)
VALUES
(?,?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO UPDATE SET state = excluded.state
`

type SaveTimerParams struct {
	Key                  int64  `json:"key"`
	ElementID            string `json:"element_id"`
	ElementInstanceKey   int64  `json:"element_instance_key"`
	ProcessDefinitionKey int64  `json:"process_definition_key"`
	ProcessInstanceKey   int64  `json:"process_instance_key"`
	State                int    `json:"state"`
	CreatedAt            int64  `json:"created_at"`
	DueAt                int64  `json:"due_at"`
	Duration             int64  `json:"duration"`
}

func (q *Queries) SaveTimer(ctx context.Context, arg SaveTimerParams) error {
	_, err := q.db.ExecContext(ctx, saveTimer,
		arg.Key,
		arg.ElementID,
		arg.ElementInstanceKey,
		arg.ProcessDefinitionKey,
		arg.ProcessInstanceKey,
		arg.State,
		arg.CreatedAt,
		arg.DueAt,
		arg.Duration,
	)
	return err
}
