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

-- name: DeleteProcessDefinitionsMessageSubscriptions :exec
-- Deletes only definition-level rows (type 3 == runtime.MessageSubscriptionTypeDefinition).
-- See pkg/bpmn/runtime/types.go for the discriminator constants.
DELETE FROM message_subscription
WHERE process_definition_key IN (sqlc.slice('processDefinitionKeys'))
    AND type = 3;


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
    AND ((@correlation_key IS NULL AND correlation_key IS NULL) OR correlation_key = @correlation_key)
-- Deterministic tie-break: when multiple subscriptions share (name, correlation_key, state) always resolve
-- to the lowest key rather than an arbitrary row.
ORDER BY key ASC
LIMIT 1;

-- name: FindDefinitionMessageSubscription :one
-- Matches only definition-level rows (type 3 == runtime.MessageSubscriptionTypeDefinition).
-- See pkg/bpmn/runtime/types.go for the discriminator constants.
SELECT
    *
FROM
    message_subscription
WHERE
    process_definition_key = @process_definition_key
    AND element_id = @element_id
    AND name = @name
    AND state = @state
    AND type = 3;

-- name: GetMessageSubscriptionByKey :one
SELECT
    *
FROM
    message_subscription
WHERE
    key = @key
    AND state = @state;

-- name: FindProcessInstanceMessageSubscriptionsPage :many
SELECT
    *,
    COUNT(*) OVER () AS total_count
FROM
    message_subscription
WHERE
    process_instance_key = @process_instance_key
    AND COALESCE(sqlc.narg('state'), state) = state
ORDER BY key ASC
LIMIT @size OFFSET @offset;
