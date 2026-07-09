-- name: SaveFlowElementInstance :exec
INSERT INTO flow_element_instance(key, element_id, element_type, process_instance_key, created_at, execution_token_key, input_variables, output_variables, completed_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, sqlc.narg('completed_at'))
ON CONFLICT
    DO UPDATE SET
       input_variables = excluded.input_variables;

-- name: UpdateOutputFlowElementInstance :exec
INSERT INTO flow_element_instance(key, element_id, element_type, process_instance_key, created_at, execution_token_key, input_variables, output_variables, completed_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, sqlc.narg('completed_at'))
ON CONFLICT
    DO UPDATE SET
       output_variables = excluded.output_variables,
       completed_at = COALESCE(flow_element_instance.completed_at, excluded.completed_at);

-- name: DeleteFlowElementInstance :exec
DELETE FROM flow_element_instance
WHERE process_instance_key IN (sqlc.slice('keys'));

-- name: FindFlowElementInstances :many
SELECT
    *,
    COUNT(*) OVER() AS total_count
FROM flow_element_instance
WHERE
    CAST(sqlc.narg('sort') AS TEXT) IS CAST(sqlc.narg('sort') AS TEXT)
    AND process_instance_key = @process_instance_key
ORDER BY
  CASE CAST(?1 AS TEXT) WHEN 'createdAt_asc'  THEN created_at END ASC,
  CASE CAST(?1 AS TEXT) WHEN 'createdAt_desc' THEN created_at END DESC,
  created_at DESC
LIMIT @limit OFFSET @offset;

-- name: GetFlowElementInstances :many
SELECT
    *,
    COUNT(*) OVER() AS total_count
FROM
    flow_element_instance
WHERE
    CAST(sqlc.narg('sort') AS TEXT) IS CAST(sqlc.narg('sort') AS TEXT)
    AND process_instance_key = @process_instance_key
ORDER BY
  CASE CAST(?1 AS TEXT) WHEN 'createdAt_asc'  THEN created_at END ASC,
  CASE CAST(?1 AS TEXT) WHEN 'createdAt_desc' THEN created_at END DESC,
  created_at DESC
LIMIT @limit OFFSET @offset;


-- name: GetFlowElementInstanceByTokenKey :one
SELECT
    *
FROM
    flow_element_instance
WHERE
    execution_token_key = @execution_token_key
ORDER BY created_at DESC;

-- name: GetFlowElementInstanceByKey :one
SELECT
    *
FROM
    flow_element_instance
WHERE
    key = @key;


-- name: CountFlowElementInstances :one
SELECT
    count(*)
FROM
    flow_element_instance
WHERE
    process_instance_key = @process_instance_key;
