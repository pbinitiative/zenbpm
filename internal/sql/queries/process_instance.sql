-- name: SaveProcessInstance :exec
INSERT INTO process_instance(key, process_definition_key, created_at, state, variables, parent_process_execution_token)
    VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT (key)
    DO UPDATE SET
        state = excluded.state,
        variables = excluded.variables;

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
