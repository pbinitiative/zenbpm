PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS error_subscription(
   key INTEGER PRIMARY KEY, -- int64 snowflake id of the error subscription where node is partition id which handles the process instance
   element_instance_key integer NOT NULL, -- int64 id of the element instance
   element_id text NOT NULL, -- string id of the element from xml definition
   process_definition_key integer NOT NULL, -- int64 reference to process definition
   process_instance_key integer NOT NULL, -- int64 reference to process instance
   error_code text, -- error code resolved from the BPMN error definition
   state integer NOT NULL, -- error subscription state
   created_at integer NOT NULL, -- unix millis of when the instance of the error subscription was created
   execution_token integer NOT NULL, -- key of the execution_token that created error_subscription
   FOREIGN KEY (process_instance_key) REFERENCES process_instance(key), -- reference to process instance
   FOREIGN KEY (process_definition_key) REFERENCES process_definition(key) -- reference to process definition
);

CREATE INDEX IF NOT EXISTS idx_fk_error_subscription_process_instance_key ON error_subscription(process_instance_key);
CREATE INDEX IF NOT EXISTS idx_fk_error_subscription_process_definition_key ON error_subscription(process_definition_key);
