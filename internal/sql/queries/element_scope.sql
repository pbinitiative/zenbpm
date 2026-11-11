-- name: SaveElementScope :exec
INSERT INTO element_scope(key, process_instance_key, phase, listener_index, input_variables, local_variables)
    VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT
    DO UPDATE SET
        phase = excluded.phase,
        listener_index = excluded.listener_index,
        input_variables = excluded.input_variables,
        local_variables = excluded.local_variables;

-- name: FindElementScope :one
SELECT
    *
FROM
    element_scope
WHERE
    key = @key;

-- name: DeleteElementScope :exec
DELETE FROM element_scope
WHERE key = @key;

-- name: DeleteProcessInstancesElementScopes :exec
DELETE FROM element_scope
WHERE process_instance_key IN (sqlc.slice('keys'));
