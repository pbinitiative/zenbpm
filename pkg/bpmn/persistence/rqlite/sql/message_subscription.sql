-- name: SaveMessageSubscription :exec
INSERT INTO message_subscription
(element_instance_key,element_id,  process_key, process_instance_key, name, state, created_at,origin_activity_key, origin_activity_state, origin_activity_id)
VALUES
(?,?,  ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO UPDATE SET state = excluded.state;

-- name: FindMessageSubscriptions :many
SELECT element_instance_key,element_id,  process_key, process_instance_key, name, state, created_at,origin_activity_key, origin_activity_state, origin_activity_id 
FROM message_subscription
WHERE COALESCE(sqlc.narg('origin_activity_key'), "origin_activity_key") = "origin_activity_key" AND
COALESCE(sqlc.narg('process_instance_key'), process_instance_key) = process_instance_key AND
COALESCE(sqlc.narg('element_id'), element_id) = element_id
