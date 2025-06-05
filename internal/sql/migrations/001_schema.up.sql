PRAGMA foreign_keys = ON;

-- table that holds information about all the process instances
CREATE TABLE IF NOT EXISTS process_instance(
    key INTEGER PRIMARY KEY, -- int64 snowflake id where node is partition id which handles the process instance
    process_definition_key INTEGER NOT NULL, -- int64 reference to process definition
    created_at INTEGER NOT NULL, -- unix millis of when the process instance was created
    state INTEGER NOT NULL, -- pkg/bpmn/runtime/types.go:ActivityState
    variables TEXT NOT NULL, -- serialized json variables of the process instance
    parent_process_execution_token INTEGER, -- key of the execution_token of the parent process
    FOREIGN KEY (process_definition_key) REFERENCES process_definition(key) -- process definition that describes this process instance
);

-- table that holds information about all the process definitions
CREATE TABLE IF NOT EXISTS process_definition(
    key INTEGER PRIMARY KEY, -- int64 id of the process definition
    version INTEGER NOT NULL, -- int64 version of the process defitition
    bpmn_process_id TEXT NOT NULL, -- id of the process from xml definition
    bpmn_data TEXT NOT NULL, -- raw string of the process definition
    bpmn_checksum BLOB NOT NULL, -- md5 checksum of the process definition
    bpmn_resource_name TEXT NOT NULL -- resource name from deployment
);

-- table that holds message subscriptions on process instances
CREATE TABLE IF NOT EXISTS message_subscription(
    -- TODO: what about starting events with message listener?
    key INTEGER PRIMARY KEY, -- int64 snowflake id of the message subscription where node is partition id which handles the process instance
    element_instance_key INTEGER NOT NULL, -- int64 id of the element instance
    element_id TEXT NOT NULL, -- string id of the element from xml definition
    process_definition_key INTEGER NOT NULL, -- int64 reference to process definition
    process_instance_key INTEGER NOT NULL, -- int64 reference to process instance
    name TEXT NOT NULL, -- message name from the definition
    state INTEGER NOT NULL, -- pkg/bpmn/runtime/types.go:ActivityState
    created_at INTEGER NOT NULL, -- unix millis of when the instance of the message subscription was created
    correlation_key TEXT NOT NULL, -- correlation key used to correlate message in the engine
    execution_token INTEGER NOT NULL, -- key of the execution_token that created message_subscription
    FOREIGN KEY (process_instance_key) REFERENCES process_instance(key), -- reference to process instance
    FOREIGN KEY (process_definition_key) REFERENCES process_definition(key) -- reference to process definition
);

CREATE TABLE IF NOT EXISTS timer(
    key INTEGER PRIMARY KEY, -- int64 snowflake id of the timer where node is partition id which handles the process instance
    element_instance_key INTEGER NOT NULL, -- int64 id of the element instance
    element_id TEXT NOT NULL, -- string id of the element from xml definition
    process_definition_key INTEGER NOT NULL, -- int64 reference to process definition
    process_instance_key INTEGER NOT NULL, -- int64 reference to process instance
    state INTEGER NOT NULL, -- pkg/bpmn/runtime/types.go:ActivityState
    created_at INTEGER NOT NULL, -- unix millis of when the instance of the message subscription was created
    due_at INTEGER NOT NULL, -- unix millis of when timer should fire
    execution_token INTEGER NOT NULL, -- key of the execution_token that created timer
    FOREIGN KEY (process_instance_key) REFERENCES process_instance(key), -- reference to process instance
    FOREIGN KEY (process_definition_key) REFERENCES process_definition(key) -- reference to process definition
);

CREATE TABLE IF NOT EXISTS job(
    key INTEGER PRIMARY KEY, -- int64 snowflake id of the job where node is partition id which handles the process instance
    element_instance_key INTEGER NOT NULL, -- int64 id of the element instance
    element_id TEXT NOT NULL, -- string id of the element from xml definition
    process_instance_key INTEGER NOT NULL, -- int64 reference to process instance
    type TEXT NOT NULL, -- zeebe:taskDefinition type from the xml definition
    state INTEGER NOT NULL, -- pkg/bpmn/runtime/types.go:ActivityState
    created_at INTEGER NOT NULL, -- unix millis of when the instance of the job was created
    variables TEXT NOT NULL, -- serialized json variables of the process instance
    execution_token INTEGER NOT NULL, -- key of the execution_token that created job
    FOREIGN KEY (process_instance_key) REFERENCES process_instance(key) -- reference to process instance
);

CREATE TABLE IF NOT EXISTS execution_token(
    key INTEGER PRIMARY KEY, -- int64 snowflake id of the token
    element_instance_key INTEGER NOT NULL, -- int64 id of the element instance
    element_id TEXT NOT NULL, -- string id of the element from xml definition
    process_instance_key INTEGER NOT NULL, -- int64 reference to process instance
    state INTEGER NOT NULL, -- pkg/bpmn/runtime/types.go:TokenState
    created_at INTEGER NOT NULL, -- unix millis of when the instance of the token was created
    FOREIGN KEY (process_instance_key) REFERENCES process_instance(key) -- reference to process instance
);

-- TODO: create a table for dumb activities like gateway/...
-- TODO: create a table for flow
