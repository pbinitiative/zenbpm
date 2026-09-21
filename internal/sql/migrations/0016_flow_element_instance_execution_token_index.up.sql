-- Periodic Running-token reconciliation derives token age from its latest flow history.
-- Keep that lookup proportional to one token's history instead of scanning all history rows.
CREATE INDEX IF NOT EXISTS idx_flow_element_instance_execution_token_key
    ON flow_element_instance(execution_token_key);
