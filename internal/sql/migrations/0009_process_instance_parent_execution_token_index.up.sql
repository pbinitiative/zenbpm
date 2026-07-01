CREATE INDEX IF NOT EXISTS idx_process_instance_parent_execution_token
    ON process_instance(parent_process_execution_token);
