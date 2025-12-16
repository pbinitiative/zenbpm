-- name: SaveDmnResourceDefinition :exec
INSERT INTO dmn_resource_definition(key, version, dmn_resource_definition_id, dmn_data, dmn_checksum, dmn_resource_name)
    VALUES (?, ?, ?, ?, ?, ?);

-- name: FindDmnResourceDefinitionByKey :one
SELECT
    *
FROM
    dmn_resource_definition
WHERE
    key = @key;

-- name: FindLatestDmnResourceDefinitionById :one
SELECT
    *
FROM
    dmn_resource_definition
WHERE
    dmn_resource_definition_id = @dmn_resource_definition_id
ORDER BY
    version DESC
LIMIT 1;

-- name: FindDmnResourceDefinitionsById :many
SELECT
    *
FROM
    dmn_resource_definition
WHERE
    dmn_resource_definition_id = @dmn_resource_definition_id
ORDER BY
    version DESC;

-- name: FindAllDmnResourceDefinitions :many
SELECT
    *
FROM
    dmn_resource_definition
ORDER BY
    version DESC;

-- name: GetDmnResourceDefinitionKeyByChecksum :one
SELECT
    key
FROM
    dmn_resource_definition
WHERE
    dmn_checksum = @dmn_checksum
LIMIT 1;

