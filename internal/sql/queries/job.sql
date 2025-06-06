-- name: SaveJob :exec
INSERT INTO job(key, element_id, element_instance_key, process_instance_key, type, state, created_at, variables, execution_token)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT
    DO UPDATE SET
        state = excluded.state,
				variables = excluded.variables;

-- name: FindJobsWithStates :many
SELECT
    *
FROM
    job
WHERE
    COALESCE(sqlc.narg('key'), "key") = "key"
    AND COALESCE(sqlc.narg('process_instance_key'), process_instance_key) = process_instance_key
    AND COALESCE(sqlc.narg('element_id'), "element_id") = "element_id"
    AND COALESCE(sqlc.narg('type'), "type") = "type"
    AND (sqlc.narg('states') IS NULL
        OR "state" IN (
            SELECT
                value
            FROM
                json_each(?5)));

-- name: FindJobByKey :one
SELECT
    *
FROM
    job
WHERE
    key = sqlc.arg('key');

-- name: FindActiveJobsByType :many
SELECT
    *
FROM
    job
WHERE
    type = @type;

-- name: FindJobByElementId :one
SELECT
    *
FROM
    job
WHERE
    element_id = @element_id
    AND process_instance_key = @process_instance_key;

-- name: FindJobByJobKey :one
SELECT
    *
FROM
    job
WHERE
    key = @key;

-- name: FindProcessInstanceJobs :many
SELECT
    *
FROM
    job
WHERE
    process_instance_key = @process_instance_key;

-- name: FindProcessInstanceJobsInState :many
SELECT
    *
FROM
    job
WHERE
    process_instance_key = @process_instance_key
    AND state IN (sqlc.slice('states'));

-- name: FindAllJobs :many
SELECT
    *
FROM
    job
LIMIT @size offset @offset;
