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
