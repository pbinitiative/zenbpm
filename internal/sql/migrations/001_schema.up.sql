PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS migration(
    name text NOT NULL,
    ran_at integer NOT NULL
);

-- table that holds information about all the process instances
CREATE TABLE IF NOT EXISTS process_instance(
    key INTEGER PRIMARY KEY, -- int64 snowflake id where node is partition id which handles the process instance
    process_definition_key integer NOT NULL, -- int64 reference to process definition
    created_at integer NOT NULL, -- unix millis of when the process instance was created
    state integer NOT NULL, -- pkg/bpmn/runtime/types.go:ActivityState
    variables text NOT NULL, -- serialized json variables of the process instance
    parent_process_execution_token integer, -- key of the execution_token of the parent process
    FOREIGN KEY (process_definition_key) REFERENCES process_definition(key) -- process definition that describes this process instance
);

-- table that holds information about all the process definitions
CREATE TABLE IF NOT EXISTS process_definition(
    key INTEGER PRIMARY KEY, -- int64 id of the process definition
    version integer NOT NULL, -- int64 version of the process defitition
    bpmn_process_id text NOT NULL, -- id of the process from xml definition
    bpmn_data text NOT NULL, -- raw string of the process definition
    bpmn_checksum BLOB NOT NULL, -- md5 checksum of the process definition
    bpmn_resource_name text NOT NULL -- resource name from deployment
);

-- table that holds message subscriptions on process instances
CREATE TABLE IF NOT EXISTS message_subscription(
    -- TODO: what about starting events with message listener?
    key INTEGER PRIMARY KEY, -- int64 snowflake id of the message subscription where node is partition id which handles the process instance
    element_instance_key integer NOT NULL, -- int64 id of the element instance
    element_id text NOT NULL, -- string id of the element from xml definition
    process_definition_key integer NOT NULL, -- int64 reference to process definition
    process_instance_key integer NOT NULL, -- int64 reference to process instance
    name text NOT NULL, -- message name from the definition
    state integer NOT NULL, -- pkg/bpmn/runtime/types.go:ActivityState
    created_at integer NOT NULL, -- unix millis of when the instance of the message subscription was created
    correlation_key text NOT NULL, -- correlation key used to correlate message in the engine
    execution_token integer NOT NULL, -- key of the execution_token that created message_subscription
    FOREIGN KEY (process_instance_key) REFERENCES process_instance(key), -- reference to process instance
    FOREIGN KEY (process_definition_key) REFERENCES process_definition(key) -- reference to process definition
);

CREATE TABLE IF NOT EXISTS timer(
    key INTEGER PRIMARY KEY, -- int64 snowflake id of the timer where node is partition id which handles the process instance
    element_instance_key integer NOT NULL, -- int64 id of the element instance
    element_id text NOT NULL, -- string id of the element from xml definition
    process_definition_key integer NOT NULL, -- int64 reference to process definition
    process_instance_key integer NOT NULL, -- int64 reference to process instance
    state integer NOT NULL, -- pkg/bpmn/runtime/types.go:ActivityState
    created_at integer NOT NULL, -- unix millis of when the instance of the message subscription was created
    due_at integer NOT NULL, -- unix millis of when timer should fire
    execution_token integer NOT NULL, -- key of the execution_token that created timer
    FOREIGN KEY (process_instance_key) REFERENCES process_instance(key), -- reference to process instance
    FOREIGN KEY (process_definition_key) REFERENCES process_definition(key) -- reference to process definition
);

CREATE TABLE IF NOT EXISTS job(
    key INTEGER PRIMARY KEY, -- int64 snowflake id of the job where node is partition id which handles the process instance
    element_instance_key integer NOT NULL, -- int64 id of the element instance
    element_id text NOT NULL, -- string id of the element from xml definition
    process_instance_key integer NOT NULL, -- int64 reference to process instance
    type TEXT NOT NULL, -- zeebe:taskDefinition type from the xml definition
    state integer NOT NULL, -- pkg/bpmn/runtime/types.go:ActivityState
    created_at integer NOT NULL, -- unix millis of when the instance of the job was created
    variables text NOT NULL, -- serialized json variables of the process instance
    execution_token integer NOT NULL, -- key of the execution_token that created job
    FOREIGN KEY (process_instance_key) REFERENCES process_instance(key) -- reference to process instance
);

CREATE INDEX idx_job_type ON job(type);

CREATE TABLE IF NOT EXISTS execution_token(
    key INTEGER PRIMARY KEY, -- int64 snowflake id of the token
    element_instance_key integer NOT NULL, -- int64 id of the element instance
    element_id text NOT NULL, -- string id of the element from xml definition
    process_instance_key integer NOT NULL, -- int64 reference to process instance
    state integer NOT NULL, -- pkg/bpmn/runtime/types.go:TokenState
    created_at integer NOT NULL, -- unix millis of when the instance of the token was created
    FOREIGN KEY (process_instance_key) REFERENCES process_instance(key) -- reference to process instance
);

-- table that holds information about all the process instance visited nodes and transitions
CREATE TABLE IF NOT EXISTS flow_element_history(
    key INTEGER PRIMARY KEY, -- int64 snowflake id of flow element history item
    element_id text NOT NULL, -- string id of the element from xml definition
    process_instance_key integer NOT NULL, -- int64 id of process instance
    created_at integer NOT NULL -- unix millis of when the process flow element was started
);

CREATE TABLE IF NOT EXISTS incident(
    key INTEGER PRIMARY KEY, -- int64 snowflake id of the incident where node is partition id which handles the process instance
    element_instance_key integer NOT NULL, -- int64 id of the element instance
    element_id text NOT NULL, -- string id of the element from xml definition
    process_instance_key integer NOT NULL, -- int64 reference to process instance
    message text NOT NULL, -- message of the incident
    created_at integer NOT NULL, -- unix millis of when the instance of the incident was created
    resolved_at integer, -- unix millis of when the instance of the incident was resolved
    execution_token integer NOT NULL, -- key of the execution_token that created job
    FOREIGN KEY (process_instance_key) REFERENCES process_instance(key) -- reference to process instance
);

-- table that holds information about all the decision definitions
CREATE TABLE IF NOT EXISTS decision_definition(
    key INTEGER PRIMARY KEY, -- int64 id of the decision definition
    version integer NOT NULL, -- int64 version of the decision definition
    dmn_id text NOT NULL, -- id of the decision from xml definition
    dmn_data text NOT NULL, -- string of the decision definition
    dmn_checksum BLOB NOT NULL, -- md5 checksum of the decision definition
    dmn_resource_name text NOT NULL -- resource name from deployment
);

-- table that holds information about all the decision
CREATE TABLE IF NOT EXISTS decision(
    version INTEGER NOT NULL, -- int64 version of the decision
    decision_id TEXT NOT NULL, -- id of the decision from xml
    version_tag TEXT NOT NULL, -- string version tag of the decision (user defined)
    decision_definition_id TEXT NOT NULL, -- id of the decision definition from xml definition
    decision_definition_key INTEGER NOT NULL, -- int64 reference to decision definition
    FOREIGN KEY (decision_definition_key) REFERENCES decision_definition(key) -- reference to decision definition
);

-- TODO: create a table for dumb activities like gateway/...
-- TODO: create a table for flow
