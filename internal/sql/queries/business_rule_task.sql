-- name: SaveBusinessRuleTask :exec
INSERT INTO business_rule_task ( key, element_instance_key, element_id, process_instance_key, decision_id, created_at, output_variables, evaluated_decisions, execution_token_key)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: FindBusinessRuleTasksByExecutionTokenIds :many
SELECT
    *
FROM
    business_rule_task
WHERE
    execution_token_key IN (sqlc.slice('token_keys'));

-- name: DeleteProcessInstancesBusinessRuleTasks :exec
DELETE FROM business_rule_task 
WHERE process_instance_key IN (sqlc.slice('keys'));
