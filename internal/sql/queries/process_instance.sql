-- name: SaveProcessInstance :exec
INSERT INTO process_instance (
    key, process_definition_key, created_at, state, variable_holder, caught_events, activities
) VALUES (
    ?, ?, ?, ?, ?, ?, ?
)
 ON CONFLICT(key) DO UPDATE SET 
    state = excluded.state,
    variable_holder = excluded.variable_holder,
    caught_events = excluded.caught_events,
    activities = excluded.activities;

-- name: FindProcessInstances :many
SELECT * 
FROM process_instance
WHERE COALESCE(sqlc.narg('key'), "key") = "key"
    AND COALESCE(sqlc.narg('process_definition_key'), process_definition_key) = process_definition_key
ORDER BY created_at DESC;

-- name: GetProcessInstance :one
SELECT * 
FROM process_instance
WHERE key = @key;
