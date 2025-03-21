-- name: SaveJob :exec
INSERT INTO job
(key, element_id, element_instance_key, process_instance_key, type, state, created_at)
VALUES
(?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO UPDATE SET state = excluded.state;

-- name: FindJobsWithStates :many
SELECT * FROM job WHERE 
    COALESCE(sqlc.narg('key'), "key") = "key" AND
    COALESCE(sqlc.narg('process_instance_key'), process_instance_key) = process_instance_key AND
    COALESCE(sqlc.narg('element_id'), "element_id") = "element_id" AND
    COALESCE(sqlc.narg('type'), "type") = "type" AND
    (sqlc.narg('states') IS  NULL OR
    "state" IN (SELECT value FROM json_each(?5)));

-- name: FindJobByKey :one
SELECT * FROM job WHERE key = sqlc.arg('key');