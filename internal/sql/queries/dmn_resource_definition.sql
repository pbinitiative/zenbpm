-- name: SaveDmnResourceDefinition :exec
INSERT INTO dmn_resource_definition(key, version, dmn_resource_definition_id, dmn_data, dmn_checksum, dmn_definition_name)
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
    drd.*, COUNT(*) OVER () AS total_count
FROM
    dmn_resource_definition as drd
WHERE
    -- force sqlc to keep sort_by_order param by mentioning it in a where clause which is always true
    CASE WHEN @sort_by_order IS NULL THEN 1 ELSE 1 END
    AND (
        CAST(@only_latest AS INTEGER) = 0
        OR drd.version = (
            SELECT MAX(drd2.version)
            FROM dmn_resource_definition AS drd2
            WHERE drd2.dmn_resource_definition_id = drd.dmn_resource_definition_id
        )
    )
    AND
    CASE WHEN @dmn_resource_definition_id IS NOT NULL THEN
         drd.dmn_resource_definition_id = @dmn_resource_definition_id
    ELSE
        1
    END
    AND
    CASE WHEN @dmn_definition_name IS NOT NULL THEN
         lower(drd.dmn_definition_name) like concat('%', lower(@dmn_definition_name), '%')
    ELSE
        1
    END
ORDER BY
    -- workaround for sqlc which does not replace params in order by
    CASE CAST(?1 AS TEXT) WHEN 'dmnDefinitionName_asc' THEN drd.dmn_definition_name END ASC,
    CASE CAST(?1 AS TEXT) WHEN 'dmnDefinitionName_desc' THEN drd.dmn_definition_name END DESC,
    CASE CAST(?1 AS TEXT) WHEN 'dmnResourceDefinitionId_asc' THEN drd.dmn_resource_definition_id END ASC,
    CASE CAST(?1 AS TEXT) WHEN 'dmnResourceDefinitionId_desc' THEN drd.dmn_resource_definition_id END DESC,
    CASE CAST(?1 AS TEXT) WHEN 'version_asc' THEN drd.version END ASC,
    CASE CAST(?1 AS TEXT) WHEN 'version_desc' THEN drd.version END DESC,
    CASE CAST(?1 AS TEXT) WHEN 'key_asc' THEN drd.key END ASC,
    CASE CAST(?1 AS TEXT) WHEN 'key_desc' THEN drd.key END DESC,
    drd.dmn_resource_definition_id ASC, drd.version DESC

LIMIT @size OFFSET @offset;

-- name: GetDmnResourceDefinitionKeyByChecksum :one
SELECT
    key
FROM
    dmn_resource_definition
WHERE
    dmn_checksum = @dmn_checksum
LIMIT 1;

