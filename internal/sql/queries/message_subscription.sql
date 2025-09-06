-- Copyright 2021-present ZenBPM Contributors
-- (based on git commit history).
--
-- ZenBPM project is available under two licenses:
--  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
--  - Enterprise License (See LICENSE-ENTERPRISE.md)

-- name: SaveMessageSubscription :exec
INSERT INTO message_subscription(key, element_instance_key, element_id, process_definition_key, process_instance_key, name, state,
    created_at, correlation_key, execution_token)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT
    DO UPDATE SET
        state = excluded.state;

-- name: FindMessageSubscriptions :many
SELECT
    *
FROM
    message_subscription
WHERE
    COALESCE(sqlc.narg('execution_token'), "execution_token") = "execution_token"
    AND COALESCE(sqlc.narg('process_instance_key'), process_instance_key) = process_instance_key
    AND COALESCE(sqlc.narg('element_id'), element_id) = element_id
    AND (sqlc.narg('states') IS NULL
        OR "state" IN (
            SELECT
                value
            FROM
                json_each(?4)));

-- name: FindTokenMessageSubscriptions :many
SELECT
    *
FROM
    message_subscription
WHERE
    execution_token = @execution_token
    AND state = @state;

-- name: FindProcessInstanceMessageSubscriptions :many
SELECT
    *
FROM
    message_subscription
WHERE
    process_instance_key = @process_instance_key
    AND state = @state;
