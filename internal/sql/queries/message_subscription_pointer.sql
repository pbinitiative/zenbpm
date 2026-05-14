-- name: SaveMessageSubscriptionPointer :exec
INSERT INTO message_subscription_pointer(state, created_at, name, correlation_key, message_subscription_key)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(name,correlation_key)
        DO UPDATE SET
            state = excluded.state,
						created_at = excluded.created_at,
						message_subscription_key = excluded.message_subscription_key;

-- name: DeleteProcessDefinitionsMessageSubscriptionPointers :exec
DELETE FROM message_subscription_pointer
WHERE message_subscription_key IN (
    SELECT key FROM message_subscription
    WHERE process_definition_key IN (sqlc.slice('processDefinitionKeys'))
        AND process_instance_key IS NULL
        AND execution_token IS NULL
);

-- name: FindMessageSubscriptionPointer :one
SELECT
    *
FROM
    message_subscription_pointer
WHERE
    state = @filter_state
    AND correlation_key = @correlation_key
    AND name = @name;
