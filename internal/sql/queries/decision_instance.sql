-- name: SaveDecisionInstance :exec
INSERT INTO decision_instance ( key,  decision_id, created_at, output_variables, evaluated_decisions)
VALUES (?, ?, ?, ?, ?);

-- name: FindDecisionInstanceByKey :one
SELECT *
FROM decision_instance
WHERE key = @key;
