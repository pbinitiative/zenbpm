PRAGMA foreign_keys = OFF;

-- SQLite does not support ALTER COLUMN/DROP COLUMN reliably, so we recreate both
-- message_subscription_pointer (removing execution_token_key) and
-- message_subscription (making process_instance_key, correlation_key and execution_token
-- nullable, and adding the type column).

CREATE TABLE message_subscription_pointer_new(
    name text NOT NULL, -- message name from the definition
    correlation_key text NOT NULL, -- correlation key used to correlate message in the engine
    state integer NOT NULL, -- reflects message_subscription state
    created_at integer NOT NULL, -- unix millis of when the pointer of the message subscription was created
    message_subscription_key integer NOT NULL, -- key of the message_subscription which this points to
    PRIMARY KEY (name, correlation_key)
);

INSERT INTO message_subscription_pointer_new
    SELECT name, correlation_key, state, created_at, message_subscription_key
    FROM message_subscription_pointer;

DROP TABLE message_subscription_pointer;

ALTER TABLE message_subscription_pointer_new RENAME TO message_subscription_pointer;

CREATE UNIQUE INDEX IF NOT EXISTS unique_name_correlation_key_waiting ON message_subscription_pointer(name, correlation_key)
WHERE
    state = 1;

-- Recreate message_subscription with nullable process_instance_key, correlation_key,
-- execution_token and the new type column.
CREATE TABLE message_subscription_new(
    key INTEGER PRIMARY KEY, -- int64 snowflake id of the message subscription where node is partition id which handles the process instance
    element_id text NOT NULL, -- string id of the element from xml definition
    process_definition_key integer NOT NULL, -- int64 reference to process definition
    process_instance_key integer, -- int64 reference to process instance
    name text NOT NULL, -- message name from the definition
    state integer NOT NULL, -- pkg/bpmn/runtime/types.go:ActivityState
    created_at integer NOT NULL, -- unix millis of when the instance of the message subscription was created
    correlation_key text, -- correlation key used to correlate message in the engine
    execution_token integer, -- key of the execution_token that created message_subscription
    type integer NOT NULL, -- pkg/bpmn/runtime/types.go:MessageSubscriptionType
    FOREIGN KEY (process_instance_key) REFERENCES process_instance(key), -- reference to process instance
    FOREIGN KEY (process_definition_key) REFERENCES process_definition(key) -- reference to process definition
);

INSERT INTO message_subscription_new(key, element_id, process_definition_key, process_instance_key, name, state, created_at, correlation_key, execution_token, type)
    SELECT key, element_id, process_definition_key, process_instance_key, name, state,
           created_at, correlation_key, execution_token, 1
    FROM message_subscription;

DROP TABLE message_subscription;

ALTER TABLE message_subscription_new RENAME TO message_subscription;

CREATE INDEX IF NOT EXISTS idx_fk_message_subscription_process_instance_key ON message_subscription(process_instance_key);
CREATE INDEX IF NOT EXISTS idx_fk_message_subscription_process_definition_key ON message_subscription(process_definition_key);
-- Hot-path index for FindMessageSubscriptionByNameAndCorrelationKeyAndState: every inbound
-- message is correlated by (name, state) — avoid a full table scan.
CREATE INDEX IF NOT EXISTS idx_message_subscription_name_state ON message_subscription(name, state);

-- Verify no FK violations were introduced by the recreate (we ran under foreign_keys = OFF).
-- SQLite's RAISE() only works inside triggers, so we abort the migration via a CHECK
-- constraint on a temporary assertion table: pragma_foreign_key_check returns one row per
-- violation, so a non-zero count fails the CHECK(ok = 0) and rolls the migration back.
CREATE TEMP TABLE _migration_0005_fk_check(
    ok INTEGER NOT NULL CHECK (ok = 0)
);
INSERT INTO _migration_0005_fk_check(ok) SELECT COUNT(*) FROM pragma_foreign_key_check;
DROP TABLE _migration_0005_fk_check;

PRAGMA foreign_keys = ON;

