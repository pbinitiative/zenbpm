-- name: SaveTimer :exec
INSERT INTO timer
(element_id, element_instance_key, process_key, process_instance_key, state, created_at, due_at, duration)
VALUES
(?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO UPDATE SET state = excluded.state;

-- name: FindTimers :many
SELECT element_id, element_instance_key, process_key, process_instance_key, state, created_at, due_at, duration
FROM timer
WHERE COALESCE(sqlc.narg('process_instance_key'), process_instance_key) = process_instance_key AND
  COALESCE(sqlc.narg('element_instance_key'), "element_instance_key") = "element_instance_key"