-- Copyright 2021-present ZenBPM Contributors
-- (based on git commit history).
--
-- ZenBPM project is available under two licenses:
--  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
--  - Enterprise License (See LICENSE-ENTERPRISE.md)

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

