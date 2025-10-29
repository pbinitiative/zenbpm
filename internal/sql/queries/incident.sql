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

-- name: GetIncidentByKey :one
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

-- name: GetIncidentsByProcessInstanceKey :many
SELECT
    *
FROM
    incident
WHERE
    process_instance_key = @process_instance_key;
