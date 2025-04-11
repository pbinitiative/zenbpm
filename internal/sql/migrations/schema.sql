PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS process_instance(
    key INTEGER PRIMARY KEY,
    process_definition_key integer NOT NULL,
    created_at integer NOT NULL,
    state integer NOT NULL,
    variable_holder text NOT NULL,
    caught_events text NOT NULL,
    activities text NOT NULL,
    FOREIGN KEY (process_definition_key) REFERENCES process_definition(key)
);

CREATE TABLE IF NOT EXISTS process_definition(
    key INTEGER PRIMARY KEY,
    version integer NOT NULL,
    bpmn_process_id text NOT NULL,
    bpmn_data text NOT NULL,
    bpmn_checksum BLOB NOT NULL,
    bpmn_resource_name text NOT NULL
);

CREATE TABLE IF NOT EXISTS message_subscription(
    key INTEGER PRIMARY KEY,
    element_instance_key integer NOT NULL,
    element_id text NOT NULL,
    process_definition_key integer NOT NULL,
    process_instance_key integer NOT NULL,
    name text NOT NULL,
    state integer NOT NULL,
    created_at integer NOT NULL,
    origin_activity_key integer NOT NULL,
    origin_activity_state integer NOT NULL,
    origin_activity_id text NOT NULL,
    FOREIGN KEY (process_instance_key) REFERENCES process_instance(key),
    FOREIGN KEY (process_definition_key) REFERENCES process_definition(key)
);

CREATE TABLE IF NOT EXISTS timer(
    key INTEGER PRIMARY KEY,
    element_id text NOT NULL,
    element_instance_key integer NOT NULL,
    process_definition_key integer NOT NULL,
    process_instance_key integer NOT NULL,
    state integer NOT NULL,
    created_at integer NOT NULL,
    due_at integer NOT NULL,
    duration integer NOT NULL, -- saved in milliseconds
    FOREIGN KEY (process_instance_key) REFERENCES process_instance(key),
    FOREIGN KEY (process_definition_key) REFERENCES process_definition(key)
);

CREATE TABLE IF NOT EXISTS job(
    key INTEGER PRIMARY KEY,
    element_id text NOT NULL,
    element_instance_key integer NOT NULL,
    process_instance_key integer NOT NULL,
    type TEXT NOT NULL,
    state integer NOT NULL,
    created_at integer NOT NULL,
    FOREIGN KEY (process_instance_key) REFERENCES process_instance(key)
);

CREATE TABLE IF NOT EXISTS activity_instance(
    key INTEGER PRIMARY KEY,
    process_instance_key integer NOT NULL,
    process_definition_key integer NOT NULL,
    created_at integer NOT NULL,
    state text NOT NULL,
    element_id text NOT NULL,
    bpmn_element_type text NOT NULL,
    FOREIGN KEY (process_instance_key) REFERENCES process_instance(key),
    FOREIGN KEY (process_definition_key) REFERENCES process_definition(key)
);
