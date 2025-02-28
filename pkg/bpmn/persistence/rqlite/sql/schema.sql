-- name: CreateProcessInstanceTable :exec
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
