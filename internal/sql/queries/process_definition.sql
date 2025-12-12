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

-- name: FindAllProcessDefinitions :many
SELECT
    *
FROM
    process_definition
ORDER BY
    version DESC;

-- name: GetProcessDefinitionsPage :many
SELECT
    *,
    COUNT(*) OVER() AS total_count
FROM
    process_definition
ORDER BY
    version DESC
LIMIT @limit
OFFSET @offset;

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

-- name: FindProcessDefinitionsById :many
SELECT
    *
FROM
    process_definition
WHERE
    bpmn_process_id = @bpmn_process_ids
ORDER BY
    version asc;

-- name: FindProcessDefinitionsByKeys :many
SELECT
    *
FROM
    process_definition
WHERE
    key IN (sqlc.slice('keys'));

-- name: GetDefinitionKeyByChecksum :one
SELECT
    key
FROM
    process_definition
WHERE
    bpmn_checksum = @bpmn_checksum
LIMIT 1;
