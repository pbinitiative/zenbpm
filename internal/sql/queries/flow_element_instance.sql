-- name: SaveFlowElementInstance :exec
INSERT INTO flow_element_instance(key, element_id, process_instance_key, created_at, execution_token_key, input_variables, output_variables)
    VALUES (?, ? ,? ,?, ?, ?, ?)
ON CONFLICT
    DO UPDATE SET
       input_variables = excluded.input_variables;

-- name: UpdateOutputFlowElementInstance :exec
INSERT INTO flow_element_instance(key, element_id, process_instance_key, created_at, execution_token_key, input_variables, output_variables)
    VALUES (?, ? ,? ,?, ?, ?, ?)
ON CONFLICT
    DO UPDATE SET
       output_variables = excluded.output_variables;

-- name: DeleteFlowElementInstance :exec
DELETE FROM flow_element_instance
WHERE process_instance_key IN (sqlc.slice('keys'));

-- name: GetFlowElementInstances :many
SELECT
    *,
    COUNT(*) OVER() AS total_count
FROM
    flow_element_instance
WHERE
    process_instance_key = @process_instance_key
LIMIT @limit OFFSET @offset;


-- name: GetFlowElementInstanceByTokenKey :one
SELECT
    *
FROM
    flow_element_instance
WHERE
    execution_token_key = @execution_token_key
ORDER BY created_at DESC;


-- name: CountFlowElementInstances :one
SELECT
    count(*)
FROM
    flow_element_instance
WHERE
    process_instance_key = @process_instance_key;
