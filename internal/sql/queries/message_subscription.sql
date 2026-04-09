-- name: SaveMessageSubscription :exec
INSERT INTO message_subscription(key, element_id, process_definition_key, process_instance_key, name, state,
    created_at, correlation_key, execution_token, type)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT
    DO UPDATE SET
        state = excluded.state;

-- name: DeleteProcessInstancesMessageSubscriptions :exec
DELETE FROM message_subscription
WHERE process_instance_key IN (sqlc.slice('keys'));

-- name: FindMessageSubscriptions :many
SELECT
    *
FROM
    message_subscription
WHERE
    COALESCE(sqlc.narg('execution_token'), "execution_token") = "execution_token"
    AND COALESCE(sqlc.narg('process_instance_key'), process_instance_key) = process_instance_key
    AND COALESCE(sqlc.narg('element_id'), element_id) = element_id
    AND state IN sqlc.slice('states');

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
    
-- name: FindMessageSubscriptionByNameAndCorrelationKeyAndState :one
SELECT
    *
FROM
    message_subscription
WHERE
    state = @state
    AND name = @name
    AND correlation_key = @correlation_key;

-- name: GetMessageSubscriptionByKey :one
SELECT
    *
FROM
    message_subscription
WHERE
    key = @key
    AND state = @state;
