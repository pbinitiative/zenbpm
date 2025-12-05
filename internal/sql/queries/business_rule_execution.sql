-- name: SaveBusinessRuleExecution :exec
INSERT INTO business_rule_execution ( key,  decision_id, created_at, output_variables, evaluated_decisions)
VALUES (?, ?, ?, ?, ?);

-- name: FindBusinessRuleExecutionByKey :one
SELECT *
FROM business_rule_execution
WHERE key = @key;
