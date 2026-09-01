-- name: IncrementFlowNodeCount :exec
UPDATE process_instance
SET flow_node_count = flow_node_count + 1
WHERE key = @process_instance_key;

-- name: GetFlowNodeCount :one
SELECT
    flow_node_count
FROM
    process_instance
WHERE
    key = @process_instance_key;

-- name: ResetProcessInstanceFlowNodeCount :exec
UPDATE process_instance
SET flow_node_count = 0
WHERE key = @process_instance_key;
