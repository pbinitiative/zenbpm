-- Copyright 2021-present ZenBPM Contributors
-- (based on git commit history).
--
-- ZenBPM project is available under two licenses:
--  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
--  - Enterprise License (See LICENSE-ENTERPRISE.md)

-- name: SaveMessageSubscriptionPointer :exec
INSERT INTO message_subscription_pointer(key, state, created_at, name, correlation_key, message_subscription_key, execution_token_key)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(key)
        DO UPDATE SET
            state = excluded.state;

-- name: SetStateForMessageSubscriptionPointers :exec
UPDATE message_subscription_pointer
SET state = @state
WHERE execution_token_key = @execution_token_key;

-- name: FindMessageSubscriptionPointer :one
SELECT
    *
FROM
    message_subscription_pointer
WHERE
    correlation_key = @correlation_key
    AND name = @name
    AND state = @filter_state;
