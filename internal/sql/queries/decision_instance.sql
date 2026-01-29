-- name: SaveDecisionInstance :exec
INSERT INTO decision_instance (key, decision_id, created_at, output_variables, evaluated_decisions, dmn_resource_definition_key,
                               decision_definition_key, process_instance_key, flow_element_instance_key)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: FindDecisionInstanceByKey :one
SELECT *
FROM decision_instance
WHERE key = @key;

-- name: DeleteProcessInstancesDecisionInstances :exec
DELETE FROM decision_instance
WHERE process_instance_key IN (sqlc.slice('keys'));

