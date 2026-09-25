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

-- name: GetRunningTokensAfter :many
SELECT
    token.*
FROM
    execution_token AS token INDEXED BY idx_execution_token_state
    JOIN process_instance AS pi ON pi.key = token.process_instance_key
WHERE token.state = @state
    AND pi.state IN (1, 8) -- ActivityStateActive, ActivityStateReady
    AND token.key > @after_token_key
ORDER BY token.key
LIMIT @row_limit;

-- name: GetRecoverableRunningTokens :many
SELECT
    token.*
FROM
    execution_token AS token INDEXED BY idx_execution_token_state
    JOIN process_instance AS pi ON pi.key = token.process_instance_key
WHERE token.state = @state
    AND pi.state IN (1, 8) -- ActivityStateActive, ActivityStateReady
    AND token.key > @after_token_key
    AND COALESCE(
        (
            SELECT MAX(COALESCE(history.completed_at, history.created_at))
            FROM flow_element_instance AS history INDEXED BY idx_flow_element_instance_execution_token_key
            WHERE history.execution_token_key = token.key
        ),
        token.created_at
    ) < CAST(@running_before AS INTEGER)
ORDER BY token.key
LIMIT @row_limit;

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
