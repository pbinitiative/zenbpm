-- name: IncrementFlowNodeCount :exec
UPDATE process_instance
SET flow_node_count = flow_node_count + 1
WHERE key = @process_instance_key;

-- name: GetFlowNodeCount :one
SELECT
    CAST(COALESCE((
        SELECT flow_node_count
        FROM process_instance
        WHERE key = @process_instance_key
    ), 0) AS INTEGER) AS flow_node_count;

-- name: ResetProcessInstanceFlowNodeCount :exec
UPDATE process_instance
SET flow_node_count = 0
WHERE key = @process_instance_key;
