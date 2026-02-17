-- name: SaveJob :exec
INSERT INTO job(key, element_id, element_instance_key, process_instance_key, type, state, created_at, variables, execution_token, assignee)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT
    DO UPDATE SET
        state = excluded.state,
        variables = excluded.variables,
        assignee = excluded.assignee;

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
    *,
    COUNT(*) OVER () AS total_count
FROM
    job
WHERE
    process_instance_key = @process_instance_key
LIMIT @size OFFSET @offset;

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

-- name: GetWaitingJobs :many
SELECT
    *
FROM
    job
WHERE
    state = 1
    AND type IN (sqlc.slice('type'))
    AND key NOT IN (sqlc.slice('key_skip'))
ORDER BY
    created_at ASC
LIMIT ?; -- https://github.com/sqlc-dev/sqlc/issues/2452

-- name: FindTokenJobsInState :many
SELECT
    *
FROM
    job
WHERE
    execution_token = @execution_token_key
    AND state IN (sqlc.slice('states'));

-- name: CountWaitingJobs :one
SELECT
    count(*)
FROM
    job
WHERE
    state = 1;


-- name: FindJobs :many
SELECT
  j.*,
  COUNT(*) OVER() AS total_count
FROM job AS j
WHERE
-- force sqlc to keep sort param
  CAST(sqlc.narg('sort') AS TEXT) IS CAST(sqlc.narg('sort') AS TEXT)
  AND COALESCE(sqlc.narg('type'), type) = type
  AND COALESCE(sqlc.narg('state'), state) = state
  AND (CAST(sqlc.narg('process_instance_key') AS INTEGER) IS NULL OR j.process_instance_key = CAST(sqlc.narg('process_instance_key') AS TEXT)) 
  AND (CAST(sqlc.narg('assignee') AS TEXT) IS NULL OR j.assignee = CAST(sqlc.narg('assignee') AS TEXT)) 
  
ORDER BY
-- workaround for sqlc does not replace params in order by
  CASE CAST(?1 AS TEXT) WHEN 'created_at_asc'  THEN j.created_at END ASC,
  CASE CAST(?1 AS TEXT) WHEN 'created_at_desc' THEN j.created_at END DESC,
  CASE CAST(?1 AS TEXT) WHEN 'key_asc' THEN j."key" END ASC,
  CASE CAST(?1 AS TEXT) WHEN 'key_desc' THEN j."key" END DESC,
  CASE CAST(?1 AS TEXT) WHEN 'type_asc' THEN j."type" END ASC,
  CASE CAST(?1 AS TEXT) WHEN 'type_desc' THEN j."type" END DESC,
  CASE CAST(?1 AS TEXT) WHEN 'state_asc' THEN j.state END ASC,
  CASE CAST(?1 AS TEXT) WHEN 'state_desc' THEN j.state END DESC,
  j."key" DESC

LIMIT @limit
OFFSET @offset;
