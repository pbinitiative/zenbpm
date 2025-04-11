-- name: SaveActivityInstance :exec
INSERT INTO activity_instance(key, process_instance_key, process_definition_key, created_at, state, element_id, bpmn_element_type)
    VALUES (?, ?, ?, ?, ?, ?, ?);

-- name: FindActivityInstances :many
SELECT
    *
FROM
    activity_instance
WHERE
    process_instance_key = @process_instance_key
ORDER BY
    key ASC;
