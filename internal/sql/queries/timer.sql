-- name: SaveTimer :exec
INSERT INTO timer(key, element_id, element_instance_key, process_definition_key, process_instance_key, state, created_at, due_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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

-- name: FindTimersInState :many
SELECT
    *
FROM
    timer
WHERE
    process_instance_key = @process_instance_key
    AND state = @state;
