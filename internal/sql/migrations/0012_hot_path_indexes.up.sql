-- Indexes for persistence queries that run on engine hot paths.
-- Keep the state indexes regular (rather than partial) because the sqlc queries
-- bind state values at runtime and SQLite cannot prove a bound parameter
-- satisfies a partial-index predicate while preparing the statement.

CREATE INDEX IF NOT EXISTS idx_decision_instance_process_instance_key
    ON decision_instance(process_instance_key);

CREATE INDEX IF NOT EXISTS idx_job_execution_token_state
    ON job(execution_token, state);

CREATE INDEX IF NOT EXISTS idx_timer_state_due_at
    ON timer(state, due_at);

CREATE INDEX IF NOT EXISTS idx_timer_execution_token_state
    ON timer(execution_token, state);

CREATE INDEX IF NOT EXISTS idx_message_subscription_execution_token_state
    ON message_subscription(execution_token, state);

CREATE INDEX IF NOT EXISTS idx_error_subscription_execution_token_state
    ON error_subscription(execution_token, state);

CREATE INDEX IF NOT EXISTS idx_incident_execution_token
    ON incident(execution_token);

CREATE INDEX IF NOT EXISTS idx_flow_element_instance_execution_token_created_at
    ON flow_element_instance(execution_token_key, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_execution_token_state
    ON execution_token(state);

CREATE INDEX IF NOT EXISTS idx_process_instance_state
    ON process_instance(state);
