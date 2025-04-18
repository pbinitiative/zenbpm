// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.28.0
// source: job.sql

package sql

import (
	"context"
	"database/sql"
	"strings"
)

const findActiveJobsByType = `-- name: FindActiveJobsByType :many
SELECT
    "key", element_instance_key, element_id, process_instance_key, type, state, created_at, variables
FROM
    job
WHERE
    type = ?1
`

func (q *Queries) FindActiveJobsByType(ctx context.Context, type_ string) ([]Job, error) {
	rows, err := q.db.QueryContext(ctx, findActiveJobsByType, type_)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []Job{}
	for rows.Next() {
		var i Job
		if err := rows.Scan(
			&i.Key,
			&i.ElementInstanceKey,
			&i.ElementID,
			&i.ProcessInstanceKey,
			&i.Type,
			&i.State,
			&i.CreatedAt,
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

const findAllJobs = `-- name: FindAllJobs :many
SELECT
    "key", element_instance_key, element_id, process_instance_key, type, state, created_at, variables
FROM
    job
LIMIT ?2 offset ?1
`

type FindAllJobsParams struct {
	Offset int64 `json:"offset"`
	Size   int64 `json:"size"`
}

func (q *Queries) FindAllJobs(ctx context.Context, arg FindAllJobsParams) ([]Job, error) {
	rows, err := q.db.QueryContext(ctx, findAllJobs, arg.Offset, arg.Size)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []Job{}
	for rows.Next() {
		var i Job
		if err := rows.Scan(
			&i.Key,
			&i.ElementInstanceKey,
			&i.ElementID,
			&i.ProcessInstanceKey,
			&i.Type,
			&i.State,
			&i.CreatedAt,
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

const findJobByElementId = `-- name: FindJobByElementId :one
SELECT
    "key", element_instance_key, element_id, process_instance_key, type, state, created_at, variables
FROM
    job
WHERE
    element_id = ?1
    AND process_instance_key = ?2
`

type FindJobByElementIdParams struct {
	ElementID          string `json:"element_id"`
	ProcessInstanceKey int64  `json:"process_instance_key"`
}

func (q *Queries) FindJobByElementId(ctx context.Context, arg FindJobByElementIdParams) (Job, error) {
	row := q.db.QueryRowContext(ctx, findJobByElementId, arg.ElementID, arg.ProcessInstanceKey)
	var i Job
	err := row.Scan(
		&i.Key,
		&i.ElementInstanceKey,
		&i.ElementID,
		&i.ProcessInstanceKey,
		&i.Type,
		&i.State,
		&i.CreatedAt,
		&i.Variables,
	)
	return i, err
}

const findJobByJobKey = `-- name: FindJobByJobKey :one
SELECT
    "key", element_instance_key, element_id, process_instance_key, type, state, created_at, variables
FROM
    job
WHERE
    key = ?1
`

func (q *Queries) FindJobByJobKey(ctx context.Context, key int64) (Job, error) {
	row := q.db.QueryRowContext(ctx, findJobByJobKey, key)
	var i Job
	err := row.Scan(
		&i.Key,
		&i.ElementInstanceKey,
		&i.ElementID,
		&i.ProcessInstanceKey,
		&i.Type,
		&i.State,
		&i.CreatedAt,
		&i.Variables,
	)
	return i, err
}

const findJobByKey = `-- name: FindJobByKey :one
SELECT
    "key", element_instance_key, element_id, process_instance_key, type, state, created_at, variables
FROM
    job
WHERE
    key = ?1
`

func (q *Queries) FindJobByKey(ctx context.Context, key int64) (Job, error) {
	row := q.db.QueryRowContext(ctx, findJobByKey, key)
	var i Job
	err := row.Scan(
		&i.Key,
		&i.ElementInstanceKey,
		&i.ElementID,
		&i.ProcessInstanceKey,
		&i.Type,
		&i.State,
		&i.CreatedAt,
		&i.Variables,
	)
	return i, err
}

const findJobsWithStates = `-- name: FindJobsWithStates :many
SELECT
    "key", element_instance_key, element_id, process_instance_key, type, state, created_at, variables
FROM
    job
WHERE
    COALESCE(?1, "key") = "key"
    AND COALESCE(?2, process_instance_key) = process_instance_key
    AND COALESCE(?3, "element_id") = "element_id"
    AND COALESCE(?4, "type") = "type"
    AND (?5 IS NULL
        OR "state" IN (
            SELECT
                value
            FROM
                json_each(?5)))
`

type FindJobsWithStatesParams struct {
	Key                sql.NullInt64  `json:"key"`
	ProcessInstanceKey sql.NullInt64  `json:"process_instance_key"`
	ElementID          sql.NullString `json:"element_id"`
	Type               sql.NullString `json:"type"`
	States             interface{}    `json:"states"`
}

func (q *Queries) FindJobsWithStates(ctx context.Context, arg FindJobsWithStatesParams) ([]Job, error) {
	rows, err := q.db.QueryContext(ctx, findJobsWithStates,
		arg.Key,
		arg.ProcessInstanceKey,
		arg.ElementID,
		arg.Type,
		arg.States,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []Job{}
	for rows.Next() {
		var i Job
		if err := rows.Scan(
			&i.Key,
			&i.ElementInstanceKey,
			&i.ElementID,
			&i.ProcessInstanceKey,
			&i.Type,
			&i.State,
			&i.CreatedAt,
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

const findProcessInstanceJobsInState = `-- name: FindProcessInstanceJobsInState :many
SELECT
    "key", element_instance_key, element_id, process_instance_key, type, state, created_at, variables
FROM
    job
WHERE
    process_instance_key = ?1
    AND state IN (/*SLICE:states*/?)
`

type FindProcessInstanceJobsInStateParams struct {
	ProcessInstanceKey int64 `json:"process_instance_key"`
	States             []int `json:"states"`
}

func (q *Queries) FindProcessInstanceJobsInState(ctx context.Context, arg FindProcessInstanceJobsInStateParams) ([]Job, error) {
	query := findProcessInstanceJobsInState
	var queryParams []interface{}
	queryParams = append(queryParams, arg.ProcessInstanceKey)
	if len(arg.States) > 0 {
		for _, v := range arg.States {
			queryParams = append(queryParams, v)
		}
		query = strings.Replace(query, "/*SLICE:states*/?", strings.Repeat(",?", len(arg.States))[1:], 1)
	} else {
		query = strings.Replace(query, "/*SLICE:states*/?", "NULL", 1)
	}
	rows, err := q.db.QueryContext(ctx, query, queryParams...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []Job{}
	for rows.Next() {
		var i Job
		if err := rows.Scan(
			&i.Key,
			&i.ElementInstanceKey,
			&i.ElementID,
			&i.ProcessInstanceKey,
			&i.Type,
			&i.State,
			&i.CreatedAt,
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

const saveJob = `-- name: SaveJob :exec
INSERT INTO job(key, element_id, element_instance_key, process_instance_key, type, state, created_at, variables)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT
    DO UPDATE SET
        state = excluded.state,
				variables = excluded.variables
`

type SaveJobParams struct {
	Key                int64  `json:"key"`
	ElementID          string `json:"element_id"`
	ElementInstanceKey int64  `json:"element_instance_key"`
	ProcessInstanceKey int64  `json:"process_instance_key"`
	Type               string `json:"type"`
	State              int    `json:"state"`
	CreatedAt          int64  `json:"created_at"`
	Variables          string `json:"variables"`
}

func (q *Queries) SaveJob(ctx context.Context, arg SaveJobParams) error {
	_, err := q.db.ExecContext(ctx, saveJob,
		arg.Key,
		arg.ElementID,
		arg.ElementInstanceKey,
		arg.ProcessInstanceKey,
		arg.Type,
		arg.State,
		arg.CreatedAt,
		arg.Variables,
	)
	return err
}
