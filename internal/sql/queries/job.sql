-- name: SaveJob :exec
INSERT INTO job(key, element_id, element_instance_key, process_instance_key, type, state, created_at, variables, execution_token)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT
    DO UPDATE SET
        state = excluded.state,
        variables = excluded.variables;

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
    type = @type
    AND state = 1;

-- ActivityStateActive
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

-- name: FindWaitingJobs :many
SELECT
    *
FROM
    job
WHERE
    key NOT IN (sqlc.slice('key_skip'))
    AND type IN (sqlc.slice('type'))
ORDER BY
    created ASC
LIMIT @size;
