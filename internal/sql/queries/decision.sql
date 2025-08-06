
-- name: SaveDecision :exec
INSERT INTO decision(key, version, decision_id, version_tag, decision_definition_id, decision_definition_key)
    VALUES (?, ?, ?, ?, ?, ?);

-- name: FindDecisionByKey :one
SELECT
    *
FROM
    decision
WHERE
    key = @key;

-- name: FindLatestDecisionById :one
SELECT
    *
FROM
    decision
WHERE
    decision_id = @decision_id
ORDER BY
    version DESC
LIMIT 1;

-- name: FindLatestDecisionByIdAndVersionTag :one
SELECT
    *
FROM
    decision
WHERE
    decision_id = @decision_id
    AND version_tag = @version_tag
ORDER BY
    version DESC
LIMIT 1;

-- name: FindLatestDecisionByIdAndDecisionDefinitionId :one
SELECT
    *
FROM
    decision d
WHERE
    decision_id = @decision_id
    AND decision_definition_id = @decision_definition_id
ORDER BY
    version DESC
LIMIT 1;

-- name: FindDecisionsById :many
SELECT
    *
FROM
    decision
WHERE
    decision_id = @decision_id
ORDER BY
    version desc;
