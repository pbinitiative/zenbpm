-- name: SaveErrorSubscription :exec
INSERT INTO error_subscription(element_instance_key, key, element_id, process_definition_key, process_instance_key, error_code, state,
    created_at, execution_token)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT
    DO UPDATE SET
        state = excluded.state;

-- name: FindTokenErrorSubscriptions :many
SELECT
    *
FROM
    error_subscription
WHERE
    execution_token = @execution_token
  AND state = @state;

-- name: FindProcessInstanceErrorSubscriptions :many
SELECT
    *
FROM
    error_subscription
WHERE
    process_instance_key = @process_instance_key
  AND state = @state;
