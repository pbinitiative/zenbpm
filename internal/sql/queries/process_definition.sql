-- name: SaveProcessDefinition :exec
INSERT INTO process_definition(key, version, bpmn_process_id, bpmn_data, bpmn_checksum, bpmn_resource_name)
    VALUES (?, ?, ?, ?, ?, ?);

-- name: FindProcessDefinitionsPage :many
SELECT
    *
FROM
    process_definition
ORDER BY
    version DESC
LIMIT @limit
OFFSET @offset;

-- name: CountProcessDefinitions :one
SELECT
    COUNT(1)
FROM
    process_definition;

-- name: GetProcessDefinitionByKey :one
SELECT
    *
FROM
    process_definition
WHERE
    key = @key;

-- name: GetLatestProcessDefinitionById :one
SELECT
    *
FROM
    process_definition
WHERE
    bpmn_process_id = @bpmn_process_id
ORDER BY
    version DESC
LIMIT 1;

-- name: GetProcessDefinitionsById :many
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
