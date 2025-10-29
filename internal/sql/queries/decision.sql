-- name: SaveDecision :exec
INSERT INTO decision(version, decision_id, version_tag, decision_definition_id, decision_definition_key)
    VALUES (?, ?, ?, ?, ?);

-- name: FindDecisionByIdAndDecisionDefinitionKey :one
SELECT
    *
FROM
    decision
WHERE
    decision_definition_key = @decision_definition_key
    and decision_id = @decision_id;

-- name: GetLatestDecisionById :one
SELECT
    *
FROM
    decision
WHERE
    decision_id = @decision_id
ORDER BY
    version DESC
LIMIT 1;

-- name: GetLatestDecisionByIdAndVersionTag :one
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

-- name: GetLatestDecisionByIdAndDecisionDefinitionId :one
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

-- name: GetDecisionsById :many
SELECT
    *
FROM
    decision
WHERE
    decision_id = @decision_id
ORDER BY
    version desc;
