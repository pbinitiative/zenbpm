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
        OR pi.history_delete_sec < @currUnix)
LIMIT @limit;

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
    CASE WHEN @activity_id IS NOT NULL THEN
        EXISTS (
            SELECT 1 FROM execution_token et
            WHERE et.process_instance_key = pi.key
              AND et.element_id = @activity_id
        )
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
    AND
    -- workaround for sqlc
    (
    CASE WHEN @filter_type_call_activity IS NULL AND @filter_type_multi_instance IS NULL AND @filter_type_default IS NULL AND @filter_type_sub_process IS NULL THEN
       1
    ELSE
         (@filter_type_call_activity IS NOT NULL AND pi.process_type = @filter_type_call_activity)
         OR
         (@filter_type_multi_instance IS NOT NULL AND pi.process_type = @filter_type_multi_instance)
         OR
         (@filter_type_default IS NOT NULL AND pi.process_type = @filter_type_default)
         OR
         (@filter_type_sub_process IS NOT NULL AND pi.process_type = @filter_type_sub_process)
    END
    )
    -- end of workaround
ORDER BY
-- workaround for sqlc which does not replace params in order by
  CASE CAST(?1 AS TEXT) WHEN 'createdAt_asc'  THEN pi.created_at END ASC,
  CASE CAST(?1 AS TEXT) WHEN 'createdAt_desc' THEN pi.created_at END DESC,
  CASE CAST(?1 AS TEXT) WHEN 'key_asc' THEN pi."key" END ASC,
  CASE CAST(?1 AS TEXT) WHEN 'key_desc' THEN pi."key" END DESC,
  CASE CAST(?1 AS TEXT) WHEN 'state_asc' THEN pi.state END ASC,
  CASE CAST(?1 AS TEXT) WHEN 'state_desc' THEN pi.state END DESC,
  CASE CAST(?1 AS TEXT) WHEN 'businessKey_asc'  THEN pi.business_key END ASC,
  CASE CAST(?1 AS TEXT) WHEN 'businessKey_desc' THEN pi.business_key END DESC,
  pi.created_at DESC

LIMIT @size OFFSET @offset;

-- name: FindProcessesByParentExecutionToken :many
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

-- name: GetElementStatisticsByProcessInstanceKey :many
WITH relevant_instances AS (
    SELECT @process_instance_key AS "key"
    UNION ALL
    SELECT pi_child.key
    FROM process_instance AS pi_child
    JOIN execution_token AS et ON pi_child.parent_process_execution_token = et.key
    WHERE et.process_instance_key = @process_instance_key
      AND pi_child.process_type = 4
),
sub_flags AS (
    SELECT
        parent_process_execution_token                      AS token_key,
        MAX(CASE WHEN state = 1 THEN 1 ELSE 0 END)         AS has_active_sub,
        1                                                   AS has_any_sub
    FROM process_instance
    WHERE process_type = 4
    GROUP BY parent_process_execution_token
),
token_stats AS (
    SELECT
        et.element_id,
        CAST(COUNT(CASE WHEN et.state IN (1, 2) AND COALESCE(sf.has_active_sub, 0) = 0 THEN 1 END) AS INTEGER) AS active_count,
        0 AS incident_count,
        CAST(COUNT(CASE WHEN et.state = 3       AND COALESCE(sf.has_any_sub,    0) = 0 THEN 1 END) AS INTEGER) AS completed_count,
        CAST(COUNT(CASE WHEN et.state = 4       AND COALESCE(sf.has_any_sub,    0) = 0 THEN 1 END) AS INTEGER) AS terminated_count
    FROM execution_token AS et
    LEFT JOIN sub_flags sf ON sf.token_key = et.key
    WHERE et.process_instance_key IN (SELECT "key" FROM relevant_instances)
      AND et.state IN (1, 2, 3, 4)
    GROUP BY et.element_id
),
incident_stats AS (
    SELECT
        i.element_id,
        0 AS active_count,
        CAST(COUNT(*) AS INTEGER) AS incident_count,
        0 AS completed_count,
        0 AS terminated_count
    FROM incident AS i
    LEFT JOIN sub_flags sf ON sf.token_key = i.execution_token
    WHERE i.process_instance_key IN (SELECT "key" FROM relevant_instances)
      AND i.resolved_at IS NULL
      AND COALESCE(sf.has_active_sub, 0) = 0
    GROUP BY i.element_id
)
SELECT
    element_id,
    CAST(SUM(active_count)     AS INTEGER) AS active_count,
    CAST(SUM(incident_count)   AS INTEGER) AS incident_count,
    CAST(SUM(completed_count)  AS INTEGER) AS completed_count,
    CAST(SUM(terminated_count) AS INTEGER) AS terminated_count
FROM (
    SELECT element_id, active_count, incident_count, completed_count, terminated_count FROM token_stats
    UNION ALL
    SELECT element_id, active_count, incident_count, completed_count, terminated_count FROM incident_stats
) AS combined
GROUP BY element_id;
