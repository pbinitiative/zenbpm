-- Copyright 2021-present ZenBPM Contributors
-- (based on git commit history).
--
-- ZenBPM project is available under two licenses:
--  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
--  - Enterprise License (See LICENSE-ENTERPRISE.md)

-- name: SaveToken :exec
INSERT INTO execution_token(key, element_instance_key, element_id, process_instance_key, state, created_at)
    VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT
    DO UPDATE SET
        state = excluded.state,
        element_instance_key = excluded.element_instance_key,
        element_id = excluded.element_id;

-- name: GetTokensInState :many
SELECT
    *
FROM
    execution_token
WHERE state = @state;

-- name: GetTokensForProcessInstance :many
SELECT
    *
FROM
    execution_token
WHERE process_instance_key = @process_instance_key;

-- name: GetTokens :many
SELECT
    *
FROM
    execution_token
WHERE
    key IN (sqlc.slice('keys'));
