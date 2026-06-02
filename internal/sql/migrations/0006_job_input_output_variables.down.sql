CREATE TABLE job_old(
    key INTEGER PRIMARY KEY,
    element_instance_key integer NOT NULL,
    element_id text NOT NULL,
    process_instance_key integer NOT NULL,
    type TEXT NOT NULL,
    state integer NOT NULL,
    created_at integer NOT NULL,
    variables text NOT NULL,
    execution_token integer NOT NULL,
    assignee text,
    FOREIGN KEY (process_instance_key) REFERENCES process_instance(key)
);

INSERT INTO job_old(key, element_instance_key, element_id, process_instance_key, type, state, created_at, variables, execution_token, assignee)
SELECT key, element_instance_key, element_id, process_instance_key, type, state, created_at, input_variables, execution_token, assignee
FROM job;

DROP TABLE job;
ALTER TABLE job_old RENAME TO job;

CREATE INDEX IF NOT EXISTS idx_job_state_type_created_at ON job(state,type,created_at) where state = 1;
CREATE INDEX IF NOT EXISTS idx_fk_job_process_instance_key ON job(process_instance_key);
