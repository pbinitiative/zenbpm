ALTER TABLE process_instance ADD COLUMN start_element_id text;

CREATE INDEX IF NOT EXISTS idx_process_instance_definition_start_element_state
    ON process_instance(process_definition_key, start_element_id, state);
