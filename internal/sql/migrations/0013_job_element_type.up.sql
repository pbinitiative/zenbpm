ALTER TABLE job ADD COLUMN element_type TEXT NOT NULL DEFAULT '';

-- Backfill from flow_element_instance.element_type. The engine writes the
-- flow_element_instance row before the job row (see pkg/bpmn/jobs.go), so
-- flow_element_instance is the source of truth for BPMN element kind.
-- Orphan rows (no matching flow element instance) retain DEFAULT ''.
UPDATE job
SET element_type = (
    SELECT flow_element_instance.element_type
    FROM flow_element_instance
    WHERE flow_element_instance.key = job.element_instance_key
)
WHERE EXISTS (
    SELECT 1
    FROM flow_element_instance
    WHERE flow_element_instance.key = job.element_instance_key
);
