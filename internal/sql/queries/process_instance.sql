-- name: SaveProcessInstance :exec
INSERT INTO process_instance(key, process_definition_key, created_at, state, variables, parent_process_execution_token)
    VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT (key)
    DO UPDATE SET
        state = excluded.state,
        variables = excluded.variables;

-- name: SetProcessInstanceTTL :exec
UPDATE
    process_instance
SET
    history_ttl_sec = CASE WHEN CAST(sqlc.narg('historyTTLSec') AS integer) IS NOT NULL THEN
        sqlc.narg('historyTTLSec')
    ELSE
        history_ttl_sec
    END,
    history_delete_sec = CASE WHEN CAST(sqlc.narg('historyDeleteSec') AS integer) IS NOT NULL THEN
        sqlc.narg('historyDeleteSec')
    ELSE
        history_delete_sec
    END
WHERE
    key = @key;

-- name: FindInactiveInstancesToDelete :many
SELECT
    pi.key
FROM
    process_instance AS pi
    LEFT JOIN execution_token AS et ON pi.parent_process_execution_token = et.key
    LEFT JOIN process_instance AS parent_pi ON et.process_instance_key = parent_pi.key
WHERE
    pi.state IN (4, 6, 9)
    AND (pi.parent_process_execution_token IS NULL
        OR parent_pi.state IN (4, 6, 9))
    AND (pi.history_delete_sec IS NULL
        OR pi.history_delete_sec < @currUnix);

-- name: FindActiveInstances :many
SELECT
    key
FROM
    process_instance
WHERE
    state IN (1, 8);

-- name: DeleteProcessInstances :exec
DELETE FROM process_instance
WHERE key IN (sqlc.slice('keys'));

-- name: FindProcessInstances :many
SELECT
    *
FROM
    process_instance
WHERE
    COALESCE(sqlc.narg('key'), "key") = "key"
    AND COALESCE(sqlc.narg('process_definition_key'), process_definition_key) = process_definition_key
ORDER BY
    created_at DESC;

-- name: FindProcessInstancesPage :many
SELECT
    *
FROM
    process_instance
WHERE
    process_definition_key = @process_definition_key
ORDER BY
    created_at DESC
LIMIT @size OFFSET @offst;

-- name: FindProcessByParentExecutionToken :many
SELECT
    *
FROM
    process_instance
WHERE
    parent_process_execution_token = @parent_process_execution_token;

-- name: GetProcessInstance :one
SELECT
    *
FROM
    process_instance
WHERE
    key = @key;

-- name: CountActiveProcessInstances :one
SELECT
    count(*)
FROM
    process_instance
WHERE
    state = 1;
