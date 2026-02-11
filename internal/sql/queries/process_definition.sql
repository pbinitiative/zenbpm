-- name: SaveProcessDefinition :exec
INSERT INTO process_definition(key, version, bpmn_process_id, bpmn_data, bpmn_checksum,  bpmn_process_name)
    VALUES (?, ?, ?, ?, ?, ?);

-- name: FindProcessDefinitions :many
SELECT
  pd."key",
  pd.version,
  pd.bpmn_process_id,
  pd.bpmn_process_name,
  COUNT(*) OVER() AS total_count
FROM process_definition AS pd
WHERE
-- force sqlc to keep sort param
  CAST(sqlc.narg('sort') AS TEXT) IS CAST(sqlc.narg('sort') AS TEXT)
  AND (CAST(sqlc.narg('bpmn_process_id_filter') AS TEXT) IS NULL OR pd.bpmn_process_id = CAST(sqlc.narg('bpmn_process_id_filter') AS TEXT))
  AND (
    CAST(sqlc.arg('only_latest') AS INTEGER) = 0
    OR pd.version = (
      SELECT MAX(pd2.version)
      FROM process_definition AS pd2
      WHERE pd2.bpmn_process_id = pd.bpmn_process_id
        AND (CAST(sqlc.narg('bpmn_process_id_filter') AS TEXT) IS NULL OR pd2.bpmn_process_id = CAST(sqlc.narg('bpmn_process_id_filter') AS TEXT))
    )
  )
  
ORDER BY
-- workaround for sqlc does not replace params in order by
  CASE CAST(?1 AS TEXT) WHEN 'version_asc'  THEN pd.version END ASC,
  CASE CAST(?1 AS TEXT) WHEN 'version_desc' THEN pd.version END DESC,
  CASE CAST(?1 AS TEXT) WHEN 'key_asc' THEN pd."key" END ASC,
  CASE CAST(?1 AS TEXT) WHEN 'key_desc' THEN pd."key" END DESC,
  CASE CAST(?1 AS TEXT) WHEN 'bpmnProcessId_asc' THEN pd.bpmn_process_id END ASC,
  CASE CAST(?1 AS TEXT) WHEN 'bpmnProcessId_desc' THEN pd.bpmn_process_id END DESC,
  CASE CAST(?1 AS TEXT) WHEN 'bpmnProcessName_asc' THEN pd.bpmn_process_name END ASC,
  CASE CAST(?1 AS TEXT) WHEN 'bpmnProcessName_desc' THEN pd.bpmn_process_name END DESC,
  pd."key" DESC

LIMIT @limit
OFFSET @offset;

-- name: FindAllProcessDefinitions :many
SELECT
    *
FROM
    process_definition
ORDER BY
    version DESC;



-- name: FindProcessDefinitionByKey :one
SELECT
    *
FROM
    process_definition
WHERE
    key = @key;

-- name: FindLatestProcessDefinitionById :one
SELECT
    *
FROM
    process_definition
WHERE
    bpmn_process_id = @bpmn_process_id
ORDER BY
    version DESC
LIMIT 1;

-- name: FindProcessDefinitionsById :many
SELECT
    *
FROM
    process_definition
WHERE
    bpmn_process_id = @bpmn_process_ids
ORDER BY
    version asc;

-- name: FindProcessDefinitionsByKeys :many
SELECT
    *
FROM
    process_definition
WHERE
    key IN (sqlc.slice('keys'));

-- name: GetDefinitionKeyByChecksum :one
SELECT
    key
FROM
    process_definition
WHERE
    bpmn_checksum = @bpmn_checksum
LIMIT 1;

-- name: FindProcessDefinitionStatistics :many
WITH filtered_definitions AS (
  SELECT
    pd."key",
    pd.version,
    pd.bpmn_process_id,
    pd.bpmn_process_name
  FROM process_definition AS pd
  WHERE
    -- force sqlc to keep sort param
    CAST(sqlc.narg('sort') AS TEXT) IS CAST(sqlc.narg('sort') AS TEXT)
    -- name filter (partial match)
    AND (CAST(sqlc.narg('name_filter') AS TEXT) IS NULL OR pd.bpmn_process_name LIKE '%' || CAST(sqlc.narg('name_filter') AS TEXT) || '%')
    -- onlyLatest filter
    AND (
      CAST(sqlc.arg('only_latest') AS INTEGER) = 0
      OR pd.version = (
        SELECT MAX(pd2.version)
        FROM process_definition AS pd2
        WHERE pd2.bpmn_process_id = pd.bpmn_process_id
      )
    )
    -- force sqlc to register both boolean flags before slices to maintain positional parameter order
    AND CAST(sqlc.arg('use_bpmn_process_id_in') AS INTEGER) IS CAST(sqlc.arg('use_bpmn_process_id_in') AS INTEGER)
    AND CAST(sqlc.arg('use_definition_key_in') AS INTEGER) IS CAST(sqlc.arg('use_definition_key_in') AS INTEGER)
    -- bpmnProcessId IN filter (boolean flag + slice, each slice appears only once)
    AND (CAST(sqlc.arg('use_bpmn_process_id_in') AS INTEGER) = 0 OR pd.bpmn_process_id IN (sqlc.slice('bpmn_process_id_in')))
    -- definitionKey IN filter (boolean flag + slice, each slice appears only once)
    AND (CAST(sqlc.arg('use_definition_key_in') AS INTEGER) = 0 OR pd."key" IN (sqlc.slice('definition_key_in')))
),
instance_counts AS (
  SELECT
    pi.process_definition_key,
    COUNT(*) AS total_instances,
    SUM(CASE WHEN pi.state = 1 THEN 1 ELSE 0 END) AS active_count,
    SUM(CASE WHEN pi.state = 4 THEN 1 ELSE 0 END) AS completed_count,
    SUM(CASE WHEN pi.state = 9 THEN 1 ELSE 0 END) AS terminated_count,
    SUM(CASE WHEN pi.state = 6 THEN 1 ELSE 0 END) AS failed_count
  FROM process_instance AS pi
  WHERE pi.process_definition_key IN (SELECT "key" FROM filtered_definitions)
  GROUP BY pi.process_definition_key
),
incident_counts AS (
  SELECT
    pi.process_definition_key,
    COUNT(*) AS total_incidents,
    SUM(CASE WHEN inc.resolved_at IS NULL THEN 1 ELSE 0 END) AS unresolved_count
  FROM incident AS inc
  INNER JOIN process_instance AS pi ON inc.process_instance_key = pi.key
  WHERE pi.process_definition_key IN (SELECT "key" FROM filtered_definitions)
  GROUP BY pi.process_definition_key
)
SELECT
  fd."key",
  fd.version,
  fd.bpmn_process_id,
  fd.bpmn_process_name,
  COALESCE(ic.total_instances, 0) AS total_instances,
  COALESCE(ic.active_count, 0) AS active_count,
  COALESCE(ic.completed_count, 0) AS completed_count,
  COALESCE(ic.terminated_count, 0) AS terminated_count,
  COALESCE(ic.failed_count, 0) AS failed_count,
  COALESCE(incc.total_incidents, 0) AS total_incidents,
  COALESCE(incc.unresolved_count, 0) AS unresolved_count,
  COUNT(*) OVER() AS total_count
FROM filtered_definitions AS fd
LEFT JOIN instance_counts AS ic ON fd."key" = ic.process_definition_key
LEFT JOIN incident_counts AS incc ON fd."key" = incc.process_definition_key
ORDER BY
  -- workaround for sqlc - sort parameter handling
  CASE CAST(?3 AS TEXT) WHEN 'name_asc' THEN fd.bpmn_process_name END ASC,
  CASE CAST(?3 AS TEXT) WHEN 'name_desc' THEN fd.bpmn_process_name END DESC,
  CASE CAST(?3 AS TEXT) WHEN 'bpmnProcessId_asc' THEN fd.bpmn_process_id END ASC,
  CASE CAST(?3 AS TEXT) WHEN 'bpmnProcessId_desc' THEN fd.bpmn_process_id END DESC,
  CASE CAST(?3 AS TEXT) WHEN 'version_asc' THEN fd.version END ASC,
  CASE CAST(?3 AS TEXT) WHEN 'version_desc' THEN fd.version END DESC,
  CASE CAST(?3 AS TEXT) WHEN 'instanceCount_asc' THEN COALESCE(ic.total_instances, 0) END ASC,
  CASE CAST(?3 AS TEXT) WHEN 'instanceCount_desc' THEN COALESCE(ic.total_instances, 0) END DESC,
  CASE CAST(?3 AS TEXT) WHEN 'incidentCount_asc' THEN COALESCE(incc.total_incidents, 0) END ASC,
  CASE CAST(?3 AS TEXT) WHEN 'incidentCount_desc' THEN COALESCE(incc.total_incidents, 0) END DESC,
  fd."key" DESC
LIMIT @limit
OFFSET @offset;
