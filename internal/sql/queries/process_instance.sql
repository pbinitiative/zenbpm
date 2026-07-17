-- name: SaveProcessInstance :exec
INSERT INTO process_instance(key, process_definition_key, created_at, state, variables, parent_process_execution_token, parent_process_target_element_id, parent_process_target_element_instance_key, process_type, business_key, start_element_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (key)
    DO UPDATE SET
        state = excluded.state,
        variables = excluded.variables,
        business_key = excluded.business_key,
        start_element_id = excluded.start_element_id;

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
    -- et.key IS NULL covers both "no parent" (the LEFT JOIN never matches a NULL
    -- parent_process_execution_token) and "the parent token row no longer exists"
    -- (normally because history cleanup already removed the parent's history rows).
    -- Either way there is no parent token row left to wait for.
    AND (et.key IS NULL
        OR parent_pi.state IN (4, 6, 9))
    -- history_delete_sec IS NULL means no history TTL was configured for the
    -- instance: retain it forever (NULL never satisfies the comparison).
    AND pi.history_delete_sec < @currUnix
ORDER BY pi.created_at DESC, pi.key DESC /* Descendants are created after their parents. */
LIMIT @limit;

-- name: FindActiveInstances :many
SELECT
    key
FROM
    process_instance
WHERE
    state IN (1, 8);

-- name: FindActiveProcessInstancesByDefinitionKeyAndStartElementId :many
SELECT
    *
FROM
    process_instance
WHERE
    process_definition_key = @process_definition_key
    AND start_element_id = @start_element_id
    AND state IN (1, 8);

-- name: DeleteProcessInstances :exec
DELETE FROM process_instance
WHERE key IN (sqlc.slice('keys'));


-- name: FindProcessInstancesPage :many
WITH process_instance_candidates AS (
    -- Use the definition/created_at index only when both parts of its prefix
    -- are selective.
    SELECT pi.*, pd.bpmn_process_id
    FROM
        process_instance AS pi
        INNER JOIN process_definition AS pd ON pi.process_definition_key = pd.key
    WHERE
        (@process_definition_key <> 0 OR @bpmn_process_id IS NOT NULL)
        AND sqlc.narg('created_from') IS NOT NULL
        AND pi.process_definition_key IN (
            SELECT candidate.key
            FROM process_definition AS candidate
            WHERE
                (@process_definition_key = 0 OR candidate.key = @process_definition_key)
                AND (@bpmn_process_id IS NULL OR candidate.bpmn_process_id = @bpmn_process_id)
        )
        AND pi.created_at BETWEEN
            CAST(sqlc.narg('created_from') AS INTEGER)
            AND COALESCE(CAST(sqlc.narg('created_to') AS INTEGER), 9223372036854775807)

    UNION ALL

    -- Preserve the sequential scan for listings without an index-selective
    -- definition and lower time bound. The branches are mutually exclusive.
    SELECT pi.*, pd.bpmn_process_id
    FROM
        process_instance AS pi NOT INDEXED
        INNER JOIN process_definition AS pd ON pi.process_definition_key = pd.key
    WHERE
        CASE WHEN
            (@process_definition_key <> 0 OR @bpmn_process_id IS NOT NULL)
            AND sqlc.narg('created_from') IS NOT NULL
        THEN 0 ELSE 1 END
        AND CASE WHEN @process_definition_key <> 0 THEN
            pi.process_definition_key = @process_definition_key
        ELSE
            1
        END
        AND CASE WHEN @bpmn_process_id IS NOT NULL THEN
            pd.bpmn_process_id = @bpmn_process_id
        ELSE
            1
        END
        AND CASE WHEN sqlc.narg('created_from') IS NOT NULL THEN
            pi.created_at >= sqlc.narg('created_from')
        ELSE
            1
        END
        AND CASE WHEN sqlc.narg('created_to') IS NOT NULL THEN
            pi.created_at <= sqlc.narg('created_to')
        ELSE
            1
        END
),
paged AS (
SELECT
    process_instance_candidates.*,
    COUNT(*) OVER () AS total_count
FROM
    process_instance_candidates
WHERE
    -- Keep sort_by_order as the first parameter: ORDER BY refers to it as ?1.
    CASE WHEN @sort_by_order IS NULL THEN 1 ELSE 1 END
    AND CASE WHEN @parent_instance_key <> 0 THEN
        process_instance_candidates.parent_process_execution_token IN (
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
        process_instance_candidates.business_key = @business_key
    ELSE
        1
    END
    AND
    CASE WHEN @activity_id IS NOT NULL THEN
        EXISTS (
            SELECT 1 FROM execution_token et
            WHERE et.process_instance_key = process_instance_candidates.key
              AND et.element_id = @activity_id
        )
    ELSE
        1
    END
    AND
    CASE WHEN @state IS NOT NULL THEN
       process_instance_candidates.state = @state
    ELSE
        1
    END
    AND
    -- workaround for sqlc
    (
    CASE WHEN @filter_type_call_activity IS NULL AND @filter_type_multi_instance IS NULL AND @filter_type_default IS NULL AND @filter_type_sub_process IS NULL THEN
       1
    ELSE
         (@filter_type_call_activity IS NOT NULL AND process_instance_candidates.process_type = @filter_type_call_activity)
         OR
         (@filter_type_multi_instance IS NOT NULL AND process_instance_candidates.process_type = @filter_type_multi_instance)
         OR
         (@filter_type_default IS NOT NULL AND process_instance_candidates.process_type = @filter_type_default)
         OR
         (@filter_type_sub_process IS NOT NULL AND process_instance_candidates.process_type = @filter_type_sub_process)
    END
    )
    -- end of workaround
ORDER BY
-- workaround for sqlc which does not replace params in order by
  CASE CAST(?1 AS TEXT) WHEN 'createdAt_asc'  THEN process_instance_candidates.created_at END ASC,
  CASE CAST(?1 AS TEXT) WHEN 'createdAt_desc' THEN process_instance_candidates.created_at END DESC,
  CASE CAST(?1 AS TEXT) WHEN 'key_asc' THEN process_instance_candidates."key" END ASC,
  CASE CAST(?1 AS TEXT) WHEN 'key_desc' THEN process_instance_candidates."key" END DESC,
  CASE CAST(?1 AS TEXT) WHEN 'state_asc' THEN process_instance_candidates.state END ASC,
  CASE CAST(?1 AS TEXT) WHEN 'state_desc' THEN process_instance_candidates.state END DESC,
  CASE CAST(?1 AS TEXT) WHEN 'businessKey_asc'  THEN process_instance_candidates.business_key END ASC,
  CASE CAST(?1 AS TEXT) WHEN 'businessKey_desc' THEN process_instance_candidates.business_key END DESC,
  CASE CAST(?1 AS TEXT) WHEN 'bpmnProcessId_asc'  THEN process_instance_candidates.bpmn_process_id END ASC,
  CASE CAST(?1 AS TEXT) WHEN 'bpmnProcessId_desc' THEN process_instance_candidates.bpmn_process_id END DESC,
  process_instance_candidates.created_at DESC

LIMIT @size OFFSET @offset
)
SELECT
    paged.key,
    paged.process_definition_key,
    paged.business_key,
    paged.created_at,
    paged.state,
    paged.variables,
    paged.parent_process_execution_token,
    paged.parent_process_target_element_id,
    paged.parent_process_target_element_instance_key,
    paged.process_type,
    paged.history_ttl_sec,
    paged.history_delete_sec,
    paged.start_element_id,
    paged.bpmn_process_id,
    CAST((
        SELECT COUNT(*)
        FROM incident AS i
        WHERE i.process_instance_key = paged.key
          AND i.resolved_at IS NULL
    ) AS INTEGER) AS incident_count,
    paged.total_count
FROM paged
WHERE
    -- Keep sort_by_order as the first parameter: ORDER BY refers to it as ?1.
    CASE WHEN @sort_by_order IS NULL THEN 1 ELSE 1 END
ORDER BY
  CASE CAST(?1 AS TEXT) WHEN 'createdAt_asc'  THEN paged.created_at END ASC,
  CASE CAST(?1 AS TEXT) WHEN 'createdAt_desc' THEN paged.created_at END DESC,
  CASE CAST(?1 AS TEXT) WHEN 'key_asc' THEN paged."key" END ASC,
  CASE CAST(?1 AS TEXT) WHEN 'key_desc' THEN paged."key" END DESC,
  CASE CAST(?1 AS TEXT) WHEN 'state_asc' THEN paged.state END ASC,
  CASE CAST(?1 AS TEXT) WHEN 'state_desc' THEN paged.state END DESC,
  CASE CAST(?1 AS TEXT) WHEN 'businessKey_asc'  THEN paged.business_key END ASC,
  CASE CAST(?1 AS TEXT) WHEN 'businessKey_desc' THEN paged.business_key END DESC,
  CASE CAST(?1 AS TEXT) WHEN 'bpmnProcessId_asc'  THEN paged.bpmn_process_id END ASC,
  CASE CAST(?1 AS TEXT) WHEN 'bpmnProcessId_desc' THEN paged.bpmn_process_id END DESC,
  paged.created_at DESC;

-- name: FindChildProcessInstancesPage :many
WITH paged AS (
SELECT
    pi.*, pd.bpmn_process_id,
    COUNT(*) OVER () AS total_count
FROM
    execution_token AS parent_token
    INNER JOIN process_instance AS pi ON pi.parent_process_execution_token = parent_token.key
    INNER JOIN process_definition AS pd ON pi.process_definition_key = pd.key
WHERE
    -- Keep sort_by_order as the first parameter for the positional ORDER BY workaround below.
    CASE WHEN @sort_by_order IS NULL THEN 1 ELSE 1 END
    AND parent_token.process_instance_key = @parent_instance_key
    AND pi.process_type IN (
        @filter_type_call_activity,
        @filter_type_multi_instance,
        @filter_type_sub_process
    )
    AND CASE WHEN @state IS NOT NULL THEN
        pi.state = @state
    ELSE
        1
    END
ORDER BY
    -- sqlc does not replace named parameters in ORDER BY, so ?1 refers to sort_by_order above.
    CASE CAST(?1 AS TEXT) WHEN 'createdAt_asc' THEN pi.created_at END ASC,
    CASE CAST(?1 AS TEXT) WHEN 'createdAt_desc' THEN pi.created_at END DESC,
    CASE CAST(?1 AS TEXT) WHEN 'key_asc' THEN pi."key" END ASC,
    CASE CAST(?1 AS TEXT) WHEN 'key_desc' THEN pi."key" END DESC,
    CASE CAST(?1 AS TEXT) WHEN 'state_asc' THEN pi.state END ASC,
    CASE CAST(?1 AS TEXT) WHEN 'state_desc' THEN pi.state END DESC,
    CASE CAST(?1 AS TEXT) WHEN 'businessKey_asc' THEN pi.business_key END ASC,
    CASE CAST(?1 AS TEXT) WHEN 'businessKey_desc' THEN pi.business_key END DESC,
    CASE CAST(?1 AS TEXT) WHEN 'bpmnProcessId_asc' THEN pd.bpmn_process_id END ASC,
    CASE CAST(?1 AS TEXT) WHEN 'bpmnProcessId_desc' THEN pd.bpmn_process_id END DESC,
    pi.created_at DESC
LIMIT @size OFFSET @offset
)
SELECT
    paged.key,
    paged.process_definition_key,
    paged.business_key,
    paged.created_at,
    paged.state,
    paged.variables,
    paged.parent_process_execution_token,
    paged.parent_process_target_element_id,
    paged.parent_process_target_element_instance_key,
    paged.process_type,
    paged.history_ttl_sec,
    paged.history_delete_sec,
    paged.start_element_id,
    paged.bpmn_process_id,
    CAST((
        SELECT COUNT(*)
        FROM incident AS i
        WHERE i.process_instance_key = paged.key
          AND i.resolved_at IS NULL
    ) AS INTEGER) AS incident_count,
    paged.total_count
FROM paged
ORDER BY
    CASE CAST(?1 AS TEXT) WHEN 'createdAt_asc' THEN paged.created_at END ASC,
    CASE CAST(?1 AS TEXT) WHEN 'createdAt_desc' THEN paged.created_at END DESC,
    CASE CAST(?1 AS TEXT) WHEN 'key_asc' THEN paged."key" END ASC,
    CASE CAST(?1 AS TEXT) WHEN 'key_desc' THEN paged."key" END DESC,
    CASE CAST(?1 AS TEXT) WHEN 'state_asc' THEN paged.state END ASC,
    CASE CAST(?1 AS TEXT) WHEN 'state_desc' THEN paged.state END DESC,
    CASE CAST(?1 AS TEXT) WHEN 'businessKey_asc' THEN paged.business_key END ASC,
    CASE CAST(?1 AS TEXT) WHEN 'businessKey_desc' THEN paged.business_key END DESC,
    CASE CAST(?1 AS TEXT) WHEN 'bpmnProcessId_asc' THEN paged.bpmn_process_id END ASC,
    CASE CAST(?1 AS TEXT) WHEN 'bpmnProcessId_desc' THEN paged.bpmn_process_id END DESC,
    paged.created_at DESC;

-- name: FindProcessesByParentExecutionToken :many
SELECT
    *
FROM
    process_instance
WHERE
    parent_process_execution_token = @parent_process_execution_token;

-- name: CountActiveSubProcessInstances :one
SELECT
    CAST(COUNT(*) AS INTEGER)
FROM
    process_instance AS child
    INNER JOIN execution_token AS et ON child.parent_process_execution_token = et.key
WHERE
    et.process_instance_key = @process_instance_key
    AND child.process_type = @process_type
    AND child.state IN (@active_state, @ready_state);

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
