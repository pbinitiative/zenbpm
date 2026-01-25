-- name: SaveDecisionDefinition :exec
INSERT INTO decision_definition(key, version, decision_id, version_tag, dmn_resource_definition_id, dmn_resource_definition_key)
    VALUES (?, ?, ?, ?, ?, ?);

-- name: FindDecisionDefinitionByIdAndDmnResourceDefinitionKey :one
SELECT
    *
FROM
    decision_definition
WHERE
    dmn_resource_definition_key = @dmn_resource_definition_key
    and decision_id = @decision_id;

-- name: FindLatestDecisionDefinitionById :one
SELECT
    *
FROM
    decision_definition
WHERE
    decision_id = @decision_id
ORDER BY
    version DESC
LIMIT 1;

-- name: FindLatestDecisionDefinitionByIdAndVersionTag :one
SELECT
    *
FROM
    decision_definition
WHERE
    decision_id = @decision_id
    AND version_tag = @version_tag
ORDER BY
    version DESC
LIMIT 1;

-- name: FindLatestDecisionDefinitionByIdAndDecisionDefinitionId :one
SELECT
    *
FROM
    decision_definition d
WHERE
    decision_id = @decision_id
    AND dmn_resource_definition_id = @dmn_resource_definition_id
ORDER BY
    version DESC
LIMIT 1;

-- name: FindDecisionDefinitionsById :many
SELECT
    *
FROM
    decision_definition
WHERE
    decision_id = @decision_id
ORDER BY
    version DESC;
