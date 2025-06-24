-- name: SaveDecisionDefinition :exec
INSERT INTO decision_definition(key, version, dmn_id, dmn_data, dmn_checksum, dmn_resource_name)
    VALUES (?, ?, ?, ?, ?, ?);

-- name: FindDecisionDefinitionsByIds :many
SELECT
    *
FROM
    decision_definition
WHERE
    dmn_id IN (@dmn_decision_ids)
ORDER BY
    version asc
LIMIT 1;
