-- /*
--  * Copyright 2021-present ZenBPM Contributors
--  * (based on git commit history).
--  *
--  * ZenBPM project is available under two licenses:
--  * - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
--  * - Enterprise License (See LICENSE-ENTERPRISE.md)
--  */

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
