PRAGMA foreign_keys = OFF;

-- SQLite does not support ALTER COLUMN, so we recreate the timer table with
-- element_instance_key, process_instance_key and execution_token made nullable.
CREATE TABLE timer_new(
    key INTEGER PRIMARY KEY, -- int64 snowflake id of the timer where node is partition id which handles the process instance
    element_instance_key integer, -- int64 id of the element instance
    element_id text NOT NULL, -- string id of the element from xml definition
    process_definition_key integer NOT NULL, -- int64 reference to process definition
    process_instance_key integer, -- int64 reference to process instance
    state integer NOT NULL, -- pkg/bpmn/runtime/types.go:ActivityState
    created_at integer NOT NULL, -- unix millis of when the instance of the message subscription was created
    due_at integer NOT NULL, -- unix millis of when timer should fire
    execution_token integer, -- key of the execution_token that created timer
    FOREIGN KEY (process_instance_key) REFERENCES process_instance(key),
    FOREIGN KEY (process_definition_key) REFERENCES process_definition(key)
);

INSERT INTO timer_new SELECT * FROM timer;

DROP TABLE timer;

ALTER TABLE timer_new RENAME TO timer;

CREATE INDEX IF NOT EXISTS idx_fk_timer_process_instance_key ON timer(process_instance_key);
CREATE INDEX IF NOT EXISTS idx_fk_timer_process_definition_key ON timer(process_definition_key);

PRAGMA foreign_keys = ON;
