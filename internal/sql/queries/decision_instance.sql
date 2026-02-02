-- name: SaveDecisionInstance :exec
INSERT INTO decision_instance (key, decision_id, created_at, output_variables, evaluated_decisions, dmn_resource_definition_key,
                               decision_definition_key, process_instance_key, flow_element_instance_key)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: FindDecisionInstanceByKey :one
SELECT *
FROM decision_instance
WHERE key = @key;

-- name: DeleteProcessInstancesDecisionInstances :exec
DELETE FROM decision_instance
WHERE process_instance_key IN (sqlc.slice('keys'));

-- name: FindDecisionInstancesPage :many
SELECT
    di."key", di.decision_id, di.created_at, di.dmn_resource_definition_key, di.decision_definition_key, di.process_instance_key, di.flow_element_instance_key,
    COUNT(*) OVER () AS total_count
FROM
    decision_instance AS di
WHERE
  -- force sqlc to keep sort_by_order param by mentioning it in a where clause which is always true
  CASE WHEN @sort_by_order IS NULL THEN 1 ELSE 1 END
  AND
  CASE WHEN @dmn_resource_definition_key <> 0 THEN
      di.dmn_resource_definition_key = @dmn_resource_definition_key
  ELSE
      1
  END
  AND CASE WHEN @dmn_resource_definition_id <> 0 THEN
      di.dmn_resource_definition_key IN (
          SELECT
              dmn_resource_definition.key
          FROM
              dmn_resource_definition
          WHERE
              dmn_resource_definition.dmn_resource_definition_id = @dmn_resource_definition_id)
  ELSE
      1
  END
  AND
  CASE WHEN @process_instance_key IS NOT NULL THEN
      di.process_instance_key = @process_instance_key
  ELSE
      1
  END
  AND
  CASE WHEN @evaluated_from IS NOT NULL THEN
      di.created_at >= @evaluated_from
  ELSE
      1
  END
  AND
  CASE WHEN @evaluated_to IS NOT NULL THEN
      di.created_at <= @evaluated_to
  ELSE
      1
  END
ORDER BY
-- workaround for sqlc which does not replace params in order by
CASE CAST(?1 AS TEXT) WHEN 'evaluatedAt_asc'  THEN di.created_at END ASC,
CASE CAST(?1 AS TEXT) WHEN 'evaluatedAt_desc' THEN di.created_at END DESC,
CASE CAST(?1 AS TEXT) WHEN 'key_asc' THEN di."key" END ASC,
CASE CAST(?1 AS TEXT) WHEN 'key_desc' THEN di."key" END DESC,
di.created_at DESC

LIMIT @size OFFSET @offset;