-- name: SaveDecisionDefinition :exec
INSERT INTO decision_definition(key, version, dmn_id, dmn_data, dmn_checksum, dmn_resource_name)
    VALUES (?, ?, ?, ?, ?, ?);

-- name: GetDecisionDefinitionByKey :one
SELECT
    *
FROM
    decision_definition
WHERE
    key = @key;

-- name: GetDecisionDefinitionsById :many
SELECT
    *
FROM
    decision_definition
WHERE
    dmn_id = @dmn_id
ORDER BY
    version ASC;

-- name: FindAllDecisionDefinitions :many
SELECT
    *
FROM
    decision_definition
ORDER BY
    version DESC;

-- name: GetDecisionDefinitionKeyByChecksum :one
SELECT
    key
FROM
    decision_definition
WHERE
    dmn_checksum = @dmn_checksum
LIMIT 1;

