-- Remaining attempts before a failure without an error code becomes an incident. Initialised
-- from zenbpm:taskDefinition retries (default: the engine's jobs.defaultRetries). Existing rows
-- get 1, which is the behaviour they had: one failure, one incident.
ALTER TABLE job ADD COLUMN retries INTEGER NOT NULL DEFAULT 1;
-- Failures without an error code since the job was created or its incident was resolved.
ALTER TABLE job ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0;
-- Unix millis before which a failed-but-retryable job must not be handed out again. NULL = now.
ALTER TABLE job ADD COLUMN retry_at INTEGER;
-- The message of the last failure without an error code, for operators reading the job.
ALTER TABLE job ADD COLUMN last_failure_message TEXT;
-- The backoff policy of the definition at job creation, normalised ("PT10S,PT1M"); NULL = none,
-- the engine's jobs.defaultRetryBackoff applies.
ALTER TABLE job ADD COLUMN retry_backoff TEXT;
-- 1 while retries and retry_at are an operator's, set through the retries endpoint: the resolution of
-- the job's incident keeps them instead of restoring the definition's retries. 0 once a resolution
-- kept them or an incident ended the series they belonged to.
ALTER TABLE job ADD COLUMN retries_set_by_operator INTEGER NOT NULL DEFAULT 0;
-- Token of the latest delivery of the job to a worker: the job manager raises it by one with every
-- delivery it hands out, before it sends it, and lowers it only to take back a delivery it never
-- sent; not even the resolution of an incident resets it. A failure naming an earlier token belongs
-- to a delivery which was superseded. 0 = never delivered.
ALTER TABLE job ADD COLUMN delivery_token INTEGER NOT NULL DEFAULT 0;
-- Token of the delivery whose failure the job recorded last, so that a repeat of that failure
-- changes nothing. 0 = none recorded for a named delivery.
ALTER TABLE job ADD COLUMN failed_delivery_token INTEGER NOT NULL DEFAULT 0;

-- One row per failure without an error code; deleted with the instance's jobs.
CREATE TABLE IF NOT EXISTS job_failure(
    key INTEGER PRIMARY KEY, -- int64 snowflake id of the failure
    job_key INTEGER NOT NULL, -- int64 reference to the job which failed
    process_instance_key INTEGER NOT NULL, -- int64 reference to process instance
    attempt INTEGER NOT NULL, -- 1-based number of the failure within the job's series
    failed_at INTEGER NOT NULL, -- unix millis of when the worker reported the failure
    retry_at INTEGER, -- unix millis before which the job is not handed out again; NULL = at once or no retry
    message TEXT NOT NULL, -- the failure message the worker sent
    incident_key INTEGER, -- set on the failure which exhausted the retries and created an incident
    delivery_token INTEGER -- the delivery the failure was reported for; NULL when the request named none
);
CREATE INDEX IF NOT EXISTS idx_job_failure_job_key ON job_failure(job_key);
CREATE INDEX IF NOT EXISTS idx_fk_job_failure_process_instance_key ON job_failure(process_instance_key);

-- The job an incident was created for; NULL for incidents without a job.
ALTER TABLE incident ADD COLUMN job_key INTEGER;
