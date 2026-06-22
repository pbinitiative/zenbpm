DROP INDEX IF EXISTS idx_process_instance_definition_start_element_state;

ALTER TABLE process_instance DROP COLUMN start_element_id;
