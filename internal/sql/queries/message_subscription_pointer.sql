-- Copyright 2021-present ZenBPM Contributors
-- (based on git commit history).
--
-- ZenBPM project is available under two licenses:
--  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
--  - Enterprise License (See LICENSE-ENTERPRISE.md)

-- name: SaveMessageSubscriptionPointer :exec
INSERT INTO message_subscription_pointer(state, created_at, name, correlation_key, message_subscription_key, execution_token_key)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(name,correlation_key)
        DO UPDATE SET
            state = excluded.state,
						created_at = excluded.created_at,
						message_subscription_key = excluded.message_subscription_key,
						execution_token_key = excluded.execution_token_key;

-- name: FindMessageSubscriptionPointer :one
SELECT
    *
FROM
    message_subscription_pointer
WHERE
    correlation_key = @correlation_key
    AND name = @name
    AND state = @filter_state;
