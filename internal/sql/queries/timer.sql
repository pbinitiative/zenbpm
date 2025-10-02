-- name: SaveTimer :exec
INSERT INTO timer(key, element_id, element_instance_key, process_definition_key, process_instance_key, state, created_at, due_at, execution_token)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT
    DO UPDATE SET
        state = excluded.state;

-- name: FindTimers :many
SELECT
    *
FROM
    timer
WHERE
    COALESCE(sqlc.narg('process_instance_key'), process_instance_key) = process_instance_key
    AND COALESCE(sqlc.narg('element_instance_key'), "element_instance_key") = "element_instance_key"
    AND (sqlc.narg('states') IS NULL
        OR "state" IN (
            SELECT
                value
            FROM
                json_each(?3)));

-- name: FindElementTimers :many
SELECT
    *
FROM
    timer
WHERE
    element_instance_key = @element_instance_key
    AND state = @state;

-- name: FindTokenTimers :many
SELECT
    *
FROM
    timer
WHERE
    execution_token = @execution_token
    AND state = @state;

-- name: FindProcessInstanceTimersInState :many
SELECT
    *
FROM
    timer
WHERE
    process_instance_key = @process_instance_key
    AND state = @state;

-- name: FindTimersInStateTillDueAt :many
SELECT
    *
FROM
    timer
WHERE
    due_at < @due_at
    AND state = @state;
