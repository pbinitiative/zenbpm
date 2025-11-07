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
),
     filters AS (
         SELECT json_extract(value, '$.name') AS name,
                json_extract(value, '$.operation') AS op,
                json_extract(value, '$.value') AS val
         FROM filter_json, json_each(json_extract(filter_json.data, '$.filters'))
     )
SELECT j.*
FROM job j
WHERE COALESCE(sqlc.narg('type'), j.type) = j.type
  AND (@state IS NULL OR j.state = @state)
  AND (@created_after IS NULL OR j.created_at >= @created_after)
  AND (@created_before IS NULL OR j.created_at <= @created_before)
  AND NOT EXISTS (
    SELECT 1 FROM filters f
    WHERE NOT (
        CASE f.op
            WHEN '='  THEN json_extract(j.variables, '$.' || f.name) = f.val
            WHEN '!=' THEN json_extract(j.variables, '$.' || f.name) != f.val
            WHEN '>'  THEN CAST(json_extract(j.variables, '$.' || f.name) AS REAL) > CAST(f.val AS REAL)
            WHEN '<'  THEN CAST(json_extract(j.variables, '$.' || f.name) AS REAL) < CAST(f.val AS REAL)
            WHEN '>=' THEN CAST(json_extract(j.variables, '$.' || f.name) AS REAL) >= CAST(f.val AS REAL)
            WHEN '<=' THEN CAST(json_extract(j.variables, '$.' || f.name) AS REAL) <= CAST(f.val AS REAL)
            WHEN 'LIKE' THEN json_extract(j.variables, '$.' || f.name) LIKE f.val
            ELSE 1
            END
        )
)
ORDER BY j.created_at DESC
LIMIT @size OFFSET @offset;
