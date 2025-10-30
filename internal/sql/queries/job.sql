-- name: SaveJob :exec
INSERT INTO job(key, element_id, element_instance_key, process_instance_key, type, state, created_at, variables, execution_token)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT
    DO UPDATE SET
        state = excluded.state,
        variables = excluded.variables;

-- name: DeleteProcessInstancesJobs :exec
DELETE FROM job
WHERE process_instance_key IN (sqlc.slice('keys'));

-- name: FindJobByKey :one
SELECT
    *
FROM
    job
WHERE
    key = sqlc.arg('key');

-- name: FindActiveJobsByType :many
SELECT
    *
FROM
    job
WHERE
    type = @type
    AND state = 1;

-- name: FindJobByElementId :one
SELECT
    *
FROM
    job
WHERE
    element_id = @element_id
    AND process_instance_key = @process_instance_key;

-- name: FindJobByJobKey :one
SELECT
    *
FROM
    job
WHERE
    key = @key;

-- name: FindProcessInstanceJobs :many
SELECT
    *
FROM
    job
WHERE
    process_instance_key = @process_instance_key;

-- name: FindProcessInstanceJobsInState :many
SELECT
    *
FROM
    job
WHERE
    process_instance_key = @process_instance_key
    AND state IN (sqlc.slice('states'));

-- name: FindAllJobs :many
SELECT
    *
FROM
    job
LIMIT @size offset @offset;

-- name: FindJobsFilter :many
SELECT
    *
FROM
    job
WHERE
    COALESCE(sqlc.narg('type'), type) = type
    AND COALESCE(sqlc.narg('state'), state) = state
LIMIT @size offset @offset;

-- name: FindWaitingJobs :many
SELECT
    *
FROM
    job
WHERE
    state = 1
    AND key NOT IN (sqlc.slice('key_skip'))
    AND type IN (sqlc.slice('type'))
ORDER BY
    created_at ASC
LIMIT ?; -- https://github.com/sqlc-dev/sqlc/issues/2452

-- name: CountWaitingJobs :one
SELECT
    count(*)
FROM
    job
WHERE
    state = 1;

-- name: FindJobsAdvancedFilter :many
WITH filter_json AS (
    SELECT json(@filters_json) as data
)
SELECT
    job.*
FROM
    job
        CROSS JOIN
    filter_json, json_each(COALESCE(json_extract(filter_json.data, '$.filters'), '[]')) AS filter
WHERE
    COALESCE(sqlc.narg('type'), job.type) = job.type
  AND (@state IS NULL OR state = @state)
  AND (@created_after IS NULL OR created_at >= @created_after)
  AND (@created_before IS NULL OR created_at <= @created_before)
  AND (
    CASE json_extract(filter.value, '$.operation')
        WHEN '=' THEN json_extract(variables, '$.' || json_extract(filter.value, '$.name')) = json_extract(filter.value, '$.value')
        WHEN '!=' THEN json_extract(variables, '$.' || json_extract(filter.value, '$.name')) != json_extract(filter.value, '$.value')
        WHEN '>' THEN CAST(json_extract(variables, '$.' || json_extract(filter.value, '$.name')) AS REAL) > CAST(json_extract(filter.value, '$.value') AS REAL)
        WHEN '<' THEN CAST(json_extract(variables, '$.' || json_extract(filter.value, '$.name')) AS REAL) < CAST(json_extract(filter.value, '$.value') AS REAL)
        WHEN '>=' THEN CAST(json_extract(variables, '$.' || json_extract(filter.value, '$.name')) AS REAL) >= CAST(json_extract(filter.value, '$.value') AS REAL)
        WHEN '<=' THEN CAST(json_extract(variables, '$.' || json_extract(filter.value, '$.name')) AS REAL) <= CAST(json_extract(filter.value, '$.value') AS REAL)
        WHEN 'LIKE' THEN json_extract(variables, '$.' || json_extract(filter.value, '$.name')) LIKE json_extract(filter.value, '$.value')
        ELSE 1
        END
    )
GROUP BY job.key
HAVING COUNT(*) = (SELECT COUNT(*) FROM filter_json, json_each(COALESCE(json_extract(filter_json.data, '$.filters'), '[]')))
ORDER BY created_at DESC
LIMIT @size OFFSET @offset;
