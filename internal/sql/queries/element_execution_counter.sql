-- name: IncrementElementExecutionCount :exec
INSERT INTO element_execution_counter (process_instance_key, element_id, execution_count)
    VALUES (@process_instance_key, @element_id, 1)
ON CONFLICT (process_instance_key, element_id)
    DO UPDATE SET
        execution_count = execution_count + 1;

-- name: GetElementExecutionCount :one
SELECT
    execution_count
FROM
    element_execution_counter
WHERE
    process_instance_key = @process_instance_key
    AND element_id = @element_id;

-- name: AllowProcessInstanceExecutionRetry :exec
UPDATE element_execution_counter
SET execution_count = MAX(execution_count - 1, 0)
WHERE process_instance_key = @process_instance_key;

-- name: DeleteElementExecutionCounters :exec
DELETE FROM element_execution_counter
WHERE process_instance_key IN (sqlc.slice('keys'));
