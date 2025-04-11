-- name: SaveProcessDefinition :exec
INSERT INTO process_definition(key, version, bpmn_process_id, bpmn_data, bpmn_checksum, bpmn_resource_name)
    VALUES (?, ?, ?, ?, ?, ?);

-- name: FindProcessDefinitions :many
SELECT
    *
FROM
    process_definition
WHERE
    COALESCE(sqlc.narg('key'), "key") = "key"
    AND COALESCE(sqlc.narg('bpmn_process_id'), bpmn_process_id) = bpmn_process_id
ORDER BY
    version DESC;

-- name: FindProcessDefinitionByKey :one
SELECT
    *
FROM
    process_definition
WHERE
    key = @key;

-- name: FindLatestProcessDefinitionById :one
SELECT
    *
FROM
    process_definition
WHERE
    bpmn_process_id = @bpmn_process_id
ORDER BY
    version DESC
LIMIT 1;

-- name: FindProcessDefinitionsByIds :many
SELECT
    *
FROM
    process_definition
WHERE
    bpmn_process_id IN (@bpmn_process_ids)
ORDER BY
    version asc
LIMIT 1;
