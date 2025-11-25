-- name: SaveFlowElementHistory :exec
INSERT INTO flow_element_history(element_instance_key, element_id, process_instance_key, created_at)
    VALUES (?, ? ,? ,?)
ON CONFLICT
    DO UPDATE SET
       process_instance_key = excluded.process_instance_key,
       element_id = excluded.element_id;

-- name: DeleteFlowElementHistory :exec
DELETE FROM flow_element_history
WHERE process_instance_key IN (sqlc.slice('keys'));

-- name: GetFlowElementHistory :many
SELECT
    *
FROM
    flow_element_history
WHERE
    process_instance_key = @process_instance_key;
