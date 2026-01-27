-- name: SaveProcessInstance :exec
INSERT INTO process_instance(key, process_definition_key, created_at, state, variables, parent_process_execution_token, parent_process_target_element_id, parent_process_target_element_instance_key, process_type, business_key)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (key)
    DO UPDATE SET
        state = excluded.state,
        variables = excluded.variables,
        business_key = excluded.business_key;

-- name: SetProcessInstanceTTL :exec
UPDATE
    process_instance
SET
    history_ttl_sec = CASE WHEN CAST(sqlc.narg('historyTTLSec') AS integer) IS NOT NULL THEN
        sqlc.narg('historyTTLSec')
    ELSE
        history_ttl_sec
    END,
    history_delete_sec = CASE WHEN CAST(sqlc.narg('historyDeleteSec') AS integer) IS NOT NULL THEN
        sqlc.narg('historyDeleteSec')
    ELSE
        history_delete_sec
    END
WHERE
    key = @key;

-- name: FindInactiveInstancesToDelete :many
SELECT
    pi.key
FROM
    process_instance AS pi
    LEFT JOIN execution_token AS et ON pi.parent_process_execution_token = et.key
    LEFT JOIN process_instance AS parent_pi ON et.process_instance_key = parent_pi.key
WHERE
    pi.state IN (4, 6, 9)
    AND (pi.parent_process_execution_token IS NULL
        OR parent_pi.state IN (4, 6, 9))
    AND (pi.history_delete_sec IS NULL
        OR pi.history_delete_sec < @currUnix);

-- name: FindActiveInstances :many
SELECT
    key
FROM
    process_instance
WHERE
    state IN (1, 8);

-- name: DeleteProcessInstances :exec
DELETE FROM process_instance
WHERE key IN (sqlc.slice('keys'));


-- name: FindProcessInstancesPage :many
SELECT
    pi.*, pd.bpmn_process_id,
    COUNT(*) OVER () AS total_count
FROM
    process_instance AS pi
    INNER JOIN process_definition AS pd ON pi.process_definition_key = pd.key
WHERE
    -- force sqlc to keep sort_by_order param by mentioning it in a where clause which is always true
    CASE WHEN @sort_by_order IS NULL THEN 1 ELSE 1 END
    AND
    CASE WHEN @process_definition_key <> 0 THEN
        pi.process_definition_key = @process_definition_key
    ELSE
        1
    END
    AND CASE WHEN @parent_instance_key <> 0 THEN
        pi.parent_process_execution_token IN (
            SELECT
                execution_token.key
            FROM
                execution_token
            WHERE
                execution_token.process_instance_key = @parent_instance_key)
    ELSE
        1
    END
    AND
    CASE WHEN @business_key IS NOT NULL THEN
        pi.business_key = @business_key
    ELSE
        1
    END
    AND
    CASE WHEN @bpmn_process_id IS NOT NULL THEN
        pd.bpmn_process_id = @bpmn_process_id
    ELSE
        1
    END
    AND
    CASE WHEN @created_from IS NOT NULL THEN
       pi.created_at >= @created_from
    ELSE
        1
    END
    AND
    CASE WHEN @created_to IS NOT NULL THEN
       pi.created_at <= @created_to
    ELSE
        1
    END
    AND
    CASE WHEN @state IS NOT NULL THEN
       pi.state = @state
    ELSE
        1
    END
ORDER BY
-- workaround for sqlc which does not replace params in order by
  CASE CAST(?1 AS TEXT) WHEN 'createdAt_asc'  THEN pi.created_at END ASC,
  CASE CAST(?1 AS TEXT) WHEN 'createdAt_desc' THEN pi.created_at END DESC,
  CASE CAST(?1 AS TEXT) WHEN 'key_asc' THEN pi."key" END ASC,
  CASE CAST(?1 AS TEXT) WHEN 'key_desc' THEN pi."key" END DESC,
  CASE CAST(?1 AS TEXT) WHEN 'state_asc' THEN pi.state END ASC,
  CASE CAST(?1 AS TEXT) WHEN 'state_desc' THEN pi.state END DESC,
  pi.created_at DESC

LIMIT @size OFFSET @offset;

-- name: FindProcessByParentExecutionToken :many
SELECT
    *
FROM
    process_instance
WHERE
    parent_process_execution_token = @parent_process_execution_token;

-- name: GetProcessInstance :one
SELECT
    *
FROM
    process_instance
WHERE
    key = @key;

-- name: CountActiveProcessInstances :one
SELECT
    count(*)
FROM
    process_instance
WHERE
    state = 1;
