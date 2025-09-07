-- Copyright 2021-present ZenBPM Contributors
-- (based on git commit history).
--
-- ZenBPM project is available under two licenses:
--  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
--  - Enterprise License (See LICENSE-ENTERPRISE.md)

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

-- name: FindJobsFilter :many
SELECT
    *
FROM
    job
WHERE
    COALESCE(sqlc.narg('type'), type) = type
    AND COALESCE(sqlc.narg('state'), state) = state
LIMIT @size offset @offset;

-- name: FindWaitingJobs :many
SELECT
    *
FROM
    job
WHERE
    state = 1
    AND key NOT IN (sqlc.slice('key_skip'))
    AND type IN (sqlc.slice('type'))
ORDER BY
    created_at ASC
LIMIT ?; -- https://github.com/sqlc-dev/sqlc/issues/2452
