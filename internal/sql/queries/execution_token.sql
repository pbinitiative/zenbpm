-- name: SaveToken :exec
INSERT INTO execution_token(key, element_instance_key, element_id, process_instance_key, state, created_at)
    VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT
    DO UPDATE SET
        state = excluded.state,
        element_instance_key = excluded.element_instance_key,
        element_id = excluded.element_id;

-- name: DeleteProcessInstancesTokens :exec
DELETE FROM execution_token
WHERE process_instance_key IN (sqlc.slice('keys'));

-- name: GetTokensInState :many
SELECT
    *
FROM
    execution_token
WHERE state = @state;

-- name: GetTokensForProcessInstance :many
-- Pinned to idx_fk_execution_token_process_instance_key. The newer idx_execution_token_state
-- is a generic state index that the planner would otherwise prefer for the leading state IN (...),
-- causing a partition-wide scan for one process instance's tokens.
SELECT
    *
FROM
    execution_token INDEXED BY idx_fk_execution_token_process_instance_key
WHERE process_instance_key = @process_instance_key
    AND state IN (sqlc.slice('states'));

-- name: GetAllTokensForProcessInstance :many
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
