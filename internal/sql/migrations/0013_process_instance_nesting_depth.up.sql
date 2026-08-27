-- Tracks how deep the process instance is in the parent-child chain
-- (call activities, sub processes, multi-instance bodies). The upmost
-- parent process instance has nesting_depth = 0. Used to detect
-- potential infinite loops of process instances spawning child instances.
ALTER TABLE process_instance ADD COLUMN nesting_depth integer NOT NULL DEFAULT 0;

WITH RECURSIVE process_instance_nesting_depth(key, nesting_depth) AS (
	SELECT key, 0
	FROM process_instance
	WHERE parent_process_execution_token IS NULL

	UNION ALL

	SELECT child.key, parent.nesting_depth + 1
	FROM process_instance AS child
	JOIN execution_token AS parent_token ON parent_token.key = child.parent_process_execution_token
	JOIN process_instance_nesting_depth AS parent ON parent.key = parent_token.process_instance_key
)
UPDATE process_instance
SET nesting_depth = (
	SELECT calculated.nesting_depth
	FROM process_instance_nesting_depth AS calculated
	WHERE calculated.key = process_instance.key
)
WHERE key IN (SELECT key FROM process_instance_nesting_depth);
