-- name: SaveFlowElementHistory :exec
INSERT INTO flow_element_history(key, element_id, process_instance_key, created_at)
    VALUES (?, ? ,? ,?)
ON CONFLICT
    DO UPDATE SET
       process_instance_key = excluded.process_instance_key,
       element_id = excluded.element_id;

-- name: GetFlowElementHistory :many
SELECT
    *
FROM
    flow_element_history
WHERE
    process_instance_key = @process_instance_key;
