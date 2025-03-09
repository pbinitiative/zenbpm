-- name: SaveMessageSubscription :exec
INSERT INTO message_subscription
(key, element_instance_key,element_id,  process_definition_key, process_instance_key, name, state, created_at,origin_activity_key, origin_activity_state, origin_activity_id)
VALUES
(?, ?,?,  ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO UPDATE SET state = excluded.state;

-- name: FindMessageSubscriptions :many
SELECT key, element_instance_key,element_id,  process_definition_key, process_instance_key, name, state, created_at,origin_activity_key, origin_activity_state, origin_activity_id 
FROM message_subscription
WHERE COALESCE(sqlc.narg('origin_activity_key'), "origin_activity_key") = "origin_activity_key" AND
COALESCE(sqlc.narg('process_instance_key'), process_instance_key) = process_instance_key AND
COALESCE(sqlc.narg('element_id'), element_id) = element_id AND
(sqlc.narg('states') IS  NULL OR
    "state" IN (SELECT value FROM json_each(?4)));
