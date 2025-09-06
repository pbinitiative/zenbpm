-- Copyright 2021-present ZenBPM Contributors
-- (based on git commit history).
--
-- ZenBPM project is available under two licenses:
--  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
--  - Enterprise License (See LICENSE-ENTERPRISE.md)

-- name: SaveIncident :exec
INSERT INTO incident(key, element_id, element_instance_key, process_instance_key, message, created_at, resolved_at, execution_token)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT
    DO UPDATE SET
        resolved_at = excluded.resolved_at;

-- name: FindIncidents :many
SELECT
    *
FROM
    incident
WHERE
    COALESCE(sqlc.narg('process_instance_key'), process_instance_key) = process_instance_key
    AND COALESCE(sqlc.narg('element_instance_key'), "element_instance_key") = "element_instance_key";

-- name: FindIncidentByKey :one
SELECT
    *
FROM
    incident
WHERE
    key = @key;


-- name: FindIncidentsByProcessInstanceKey :many
SELECT
    *
FROM
    incident
WHERE
    process_instance_key = @process_instance_key;
