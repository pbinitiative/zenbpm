-- name: SaveJobFailure :exec
INSERT INTO job_failure(key, job_key, process_instance_key, attempt, failed_at, retry_at, message, incident_key, delivery_token)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: FindJobFailuresByJobKey :many
SELECT
    *
FROM
    job_failure
WHERE
    job_key = @job_key
ORDER BY
    failed_at DESC,
    key DESC;

-- name: HasJobFailureOfDelivery :one
-- Whether the job recorded a failure naming the delivery token; idx_job_failure_job_key narrows it to
-- the failures of the job.
SELECT EXISTS (
    SELECT 1
    FROM job_failure
    WHERE job_key = @job_key
        AND delivery_token = @delivery_token
);

-- name: FindJobFailuresPage :many
SELECT
    *
FROM
    job_failure
WHERE
    job_key = @job_key
ORDER BY
    failed_at DESC,
    key DESC
LIMIT @size OFFSET @offset;

-- name: CountJobFailures :one
-- Counted apart from the page: a page beyond the last one has no row to carry a window count.
SELECT
    COUNT(*)
FROM
    job_failure
WHERE
    job_key = @job_key;

-- name: DeleteProcessInstancesJobFailures :exec
DELETE FROM job_failure
WHERE process_instance_key IN (sqlc.slice('keys'));
