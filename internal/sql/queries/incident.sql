-- name: SaveIncident :exec
INSERT INTO incident(key, element_id, element_instance_key, process_instance_key, message, created_at, resolved_at, execution_token)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT
    DO UPDATE SET
        resolved_at = excluded.resolved_at;

-- name: DeleteProcessInstancesIncidents :exec
DELETE FROM incident
WHERE process_instance_key IN (sqlc.slice('keys'));

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

-- name: FindIncidentsPageByProcessInstanceKey :many
SELECT
    *,
    COUNT(*) OVER () AS total_count
FROM
    incident
WHERE
    process_instance_key = @process_instance_key
    AND CASE 
        WHEN CAST(sqlc.narg('state') AS TEXT) IS NULL THEN 1 = 1
        WHEN CAST(sqlc.narg('state') AS TEXT)  = 'resolved' THEN resolved_at IS NOT NULL
        WHEN CAST(sqlc.narg('state') AS TEXT)  = 'unresolved' THEN resolved_at IS NULL
        ELSE 1 = 1  -- return all if state is not 'resolved' or 'unresolved'
    END
LIMIT @size OFFSET @offset;

-- name: FindIncidentsByExecutionTokenKey :many
SELECT
    *
FROM
    incident
WHERE
    execution_token = @execution_token;
