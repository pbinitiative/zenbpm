-- name: InsertProcessInstance :exec
INSERT INTO process_instance (
    key, process_definition_key, created_at, state, variable_holder, caught_events, activities
) VALUES (
    ?, ?, ?, ?, ?, ?, ?
) ON CONFLICT(key) DO UPDATE SET 
    state = excluded.state,
    variable_holder = excluded.variable_holder,
    caught_events = excluded.caught_events,
    activities = excluded.activities;

-- name: GetProcessInstances :many
SELECT * 
FROM process_instance
WHERE (
        created_at >= sqlc.narg('key')
        OR sqlc.narg('key') IS NULL
    ) AND (
        
        created_at >= sqlc.narg('process_definition_key')
        OR sqlc.narg('process_definition_key') IS NULL
    )
ORDER BY created_at DESC;
