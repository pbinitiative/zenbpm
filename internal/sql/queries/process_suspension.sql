-- name: IsProcessSuspended :one
SELECT EXISTS (
    SELECT 1
    FROM process_suspension
    WHERE bpmn_process_id = ?
      AND suspended = 1
) AS suspended;

-- name: GetProcessSuspension :one
SELECT suspended, version
FROM process_suspension
WHERE bpmn_process_id = ?;

-- name: CreateProcessSuspension :execrows
INSERT INTO process_suspension(bpmn_process_id, suspended, version)
    VALUES (?, ?, ?)
ON CONFLICT
    DO NOTHING;

-- name: UpdateProcessSuspension :execrows
UPDATE process_suspension
SET suspended = @suspended,
    version = @version
WHERE bpmn_process_id = @bpmn_process_id
  AND version = @expected_version;

-- name: ApplyProcessSuspension :exec
INSERT INTO process_suspension(bpmn_process_id, suspended, version)
    VALUES (?, ?, ?)
ON CONFLICT
    DO UPDATE SET
        suspended = excluded.suspended,
        version = excluded.version
    WHERE excluded.version > process_suspension.version;
