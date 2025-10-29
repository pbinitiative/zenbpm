-- name: SaveMessageSubscriptionPointer :exec
INSERT INTO message_subscription_pointer(state, created_at, name, correlation_key, message_subscription_key, execution_token_key)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(name,correlation_key)
        DO UPDATE SET
            state = excluded.state,
						created_at = excluded.created_at,
						message_subscription_key = excluded.message_subscription_key,
						execution_token_key = excluded.execution_token_key;

-- name: GetMessageSubscriptionPointer :one
SELECT
    *
FROM
    message_subscription_pointer
WHERE
    correlation_key = @correlation_key
    AND name = @name
    AND state = @filter_state;
