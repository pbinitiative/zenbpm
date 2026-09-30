-- name: SaveJob :exec
-- retry_backoff is left out of the update on purpose: the policy is fixed when the job is created.
-- delivery_token is left out as well: only RecordJobDelivery writes it, so that a job the engine read
-- before a delivery was recorded does not take the token back when it is saved.
INSERT INTO job(key, element_id, element_type, element_instance_key, process_instance_key, type, state, created_at, input_variables, output_variables, execution_token, assignee, retries, attempts, retry_at, last_failure_message, retry_backoff, retries_set_by_operator, delivery_token, failed_delivery_token)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT
    DO UPDATE SET
        state = excluded.state,
        input_variables = excluded.input_variables,
        output_variables = excluded.output_variables,
        assignee = excluded.assignee,
        retries = excluded.retries,
        attempts = excluded.attempts,
        retry_at = excluded.retry_at,
        last_failure_message = excluded.last_failure_message,
        retries_set_by_operator = excluded.retries_set_by_operator,
        failed_delivery_token = excluded.failed_delivery_token;

-- name: RecordJobDelivery :execrows
-- Raises the delivery token of a job the job manager hands out, provided it still waits for a worker
-- and no other delivery was recorded since it was loaded. No row affected = do not hand it out.
UPDATE job
SET delivery_token = @delivery_token
WHERE key = @key
    AND state = 1
    AND delivery_token = @loaded_delivery_token;

-- name: WithdrawJobDelivery :execrows
-- Takes back the token of a delivery the job manager recorded but never sent, so that the delivery
-- before it counts again. Only while no other delivery was recorded since; the token was never handed
-- out, so issuing it again later is harmless.
UPDATE job
SET delivery_token = @previous_delivery_token
WHERE key = @key
    AND delivery_token = @delivery_token;

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
    AND state = 1
    AND (retry_at IS NULL OR retry_at <= CAST(@now AS INTEGER));

-- name: FindJobByJobKey :one
SELECT
    *
FROM
    job
WHERE
    key = @key;

-- name: FindProcessInstanceJobs :many
SELECT
    sqlc.embed(job),
    COUNT(*) OVER () AS total_count
FROM
    job
WHERE
    process_instance_key = @process_instance_key
LIMIT @size OFFSET @offset;

-- name: FindProcessInstanceJobsInState :many
-- Pinned to idx_fk_job_process_instance_key. The planner currently picks it correctly, but
-- pinning here makes the contract explicit and prevents the generic idx_job_execution_token_state
-- from shadowing it under different data distributions. See TestHotPathIndexes.
SELECT
    *
FROM
    job INDEXED BY idx_fk_job_process_instance_key
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
    AND (retry_at IS NULL OR retry_at <= CAST(@now AS INTEGER))
ORDER BY
    created_at ASC
LIMIT ?; -- https://github.com/sqlc-dev/sqlc/issues/2452

-- name: GetJobsInStateByTokenKey :many
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
  sqlc.embed(j),
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
