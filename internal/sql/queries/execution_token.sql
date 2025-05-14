-- name: SaveToken :exec
INSERT INTO execution_token(key, element_instance_key, element_id, process_instance_key, state, created_at)
    VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT
    DO UPDATE SET
        state = excluded.state,
        element_instance_key = excluded.element_instance_key,
        element_id = excluded.element_id;

-- name: GetTokensInStateForPartition :many
SELECT
    *
FROM
    execution_token
WHERE (key & 4190208) >> 12 = @partition
    AND state = @state;
