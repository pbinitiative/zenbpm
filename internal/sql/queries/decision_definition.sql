-- name: SaveDecisionDefinition :exec
INSERT INTO decision_definition(key, version, dmn_id, dmn_data, dmn_checksum, dmn_resource_name)
    VALUES (?, ?, ?, ?, ?, ?);

-- name: FindDecisionDefinitionByKey :one
SELECT
    *
FROM
    decision_definition
WHERE
    key = @key;

-- name: FindLatestDecisionDefinitionById :one
SELECT
    *
FROM
    decision_definition
WHERE
    dmn_id = @dmn_id
ORDER BY
    version DESC
LIMIT 1;

-- name: FindDecisionDefinitionsById :many
SELECT
    *
FROM
    decision_definition
WHERE
    dmn_id = @dmn_id
ORDER BY
    version desc;