-- name: SaveErrorSubscription :exec
INSERT INTO error_subscription(element_instance_key, key, element_id, process_definition_key, process_instance_key, error_code, state,
    created_at, execution_token)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT
    DO UPDATE SET
        state = excluded.state;

-- name: DeleteProcessInstancesErrorSubscriptions :exec
DELETE FROM error_subscription
WHERE process_instance_key IN (sqlc.slice('keys'));

-- name: FindTokenErrorSubscriptions :many
SELECT
    *
FROM
    error_subscription
WHERE
    execution_token = @execution_token
  AND state = @state;

-- name: FindProcessInstanceErrorSubscriptions :many
-- Pinned to idx_fk_error_subscription_process_instance_key. The planner currently picks it
-- correctly, but pinning here makes the contract explicit and prevents the generic
-- idx_error_subscription_execution_token_state from shadowing it under different data
-- distributions. See TestHotPathIndexes.
SELECT
    *
FROM
    error_subscription INDEXED BY idx_fk_error_subscription_process_instance_key
WHERE
    process_instance_key = @process_instance_key
  AND state = @state;

-- name: FindProcessInstanceErrorSubscriptionsPage :many
-- Pinned to idx_fk_error_subscription_process_instance_key. See note on
-- FindProcessInstanceErrorSubscriptions.
SELECT
    *,
    COUNT(*) OVER () AS total_count
FROM
    error_subscription INDEXED BY idx_fk_error_subscription_process_instance_key
WHERE
    process_instance_key = @process_instance_key
    AND COALESCE(sqlc.narg('state'), state) = state
ORDER BY key ASC
LIMIT @size OFFSET @offset;
