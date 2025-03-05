PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS process_instance (
    key INTEGER PRIMARY KEY,
    process_definition_key INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    state INTEGER NOT NULL,
    variable_holder TEXT NOT NULL,
    caught_events TEXT NOT NULL,
    activities TEXT NOT NULL,
    FOREIGN KEY(process_definition_key) REFERENCES process_definition(key)
);

CREATE TABLE IF NOT EXISTS process_definition (
	key INTEGER PRIMARY KEY,
	version INTEGER NOT NULL,
	bpmn_process_id TEXT NOT NULL,
	bpmn_data TEXT NOT NULL,
	bpmn_checksum BLOB NOT NULL,
	bpmn_resource_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS message_subscription (
	element_instance_key INTEGER PRIMARY KEY,
	element_id TEXT NOT NULL,
	process_key INTEGER NOT NULL,
	process_instance_key INTEGER NOT NULL,
	name TEXT NOT NULL,
	state INTEGER NOT NULL,
	created_at INTEGER NOT NULL,
	origin_activity_key INTEGER NOT NULL,
	origin_activity_state INTEGER NOT NULL,
	origin_activity_id TEXT NOT NULL,
	FOREIGN KEY(process_instance_key) REFERENCES process_instance(key)
	FOREIGN KEY(process_key) REFERENCES process_definition(key)
	);

CREATE TABLE IF NOT EXISTS timer (
	element_id TEXT NOT NULL,
	element_instance_key INTEGER NOT NULL,
	process_key INTEGER NOT NULL,
	process_instance_key INTEGER NOT NULL,
	state INTEGER NOT NULL,
	created_at INTEGER NOT NULL,
	due_at INTEGER NOT NULL,
	duration INTEGER NOT NULL,
	FOREIGN KEY(process_instance_key) REFERENCES process_instance(key)
	FOREIGN KEY(process_key) REFERENCES process_definition(key)
	);

CREATE TABLE IF NOT EXISTS job (
	key INTEGER PRIMARY KEY,
	element_id TEXT NOT NULL,
	element_instance_key INTEGER NOT NULL,
	process_instance_key INTEGER NOT NULL,
	state INTEGER NOT NULL,
	created_at INTEGER NOT NULL,
	FOREIGN KEY(process_instance_key) REFERENCES process_instance(key)
	);

CREATE TABLE IF NOT EXISTS activity_instance (
	key INTEGER PRIMARY KEY,
	process_instance_key INTEGER NOT NULL,
	process_definition_key INTEGER NOT NULL,
	created_at INTEGER NOT NULL,
	state TEXT NOT NULL,

	element_id TEXT NOT NULL,
	bpmn_element_type TEXT NOT NULL,

	FOREIGN KEY(process_instance_key) REFERENCES process_instance(key)
	FOREIGN KEY(process_definition_key) REFERENCES process_definition(key)
	);