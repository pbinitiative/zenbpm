PRAGMA foreign_keys = OFF;

-- Revert changes from 0005: restore execution_token_key in message_subscription_pointer
-- and restore NOT NULL constraints on process_instance_key, correlation_key, execution_token
-- in message_subscription while removing the type column.
--
-- WARNING: This down migration is LOSSY:
--   * Definition-level message subscriptions (type = 3) have NULL process_instance_key,
--     NULL correlation_key and NULL execution_token, which are incompatible with the
--     NOT-NULL + FK columns of the pre-0005 schema. We DELETE them outright rather than
--     coercing them to 0 / '' (which would leave FK-violating orphan rows once
--     foreign_keys is re-enabled).
--     Callers must re-deploy their process definitions after rolling back to recreate the
--     definition-level subscriptions for message start events.
--   * The type discriminator column is dropped, so the Token / Instance / Definition
--     distinction is irrecoverably lost. Re-applying 0005.up.sql will assign type = 1
--     (Token) to every surviving row.

DELETE FROM message_subscription WHERE type = 3;

-- Defensive cleanup: any remaining row with NULLs in newly-restored NOT NULL columns is
-- orphaned and unsafe to keep — drop it rather than masking with 0 / ''.
DELETE FROM message_subscription
WHERE execution_token IS NULL
   OR correlation_key IS NULL
   OR process_instance_key IS NULL;

CREATE TABLE message_subscription_pointer_new(
    name text NOT NULL, -- message name from the definition
    correlation_key text NOT NULL, -- correlation key used to correlate message in the engine
    state integer NOT NULL, -- reflects message_subscription state
    created_at integer NOT NULL, -- unix millis of when the pointer of the message subscription was created
    message_subscription_key integer NOT NULL, -- key of the message_subscription which this points to
    execution_token_key integer NOT NULL, -- key of the execution_token that created message_subscription
    PRIMARY KEY (name, correlation_key)
);

INSERT INTO message_subscription_pointer_new
    SELECT name, correlation_key, state, created_at, message_subscription_key, 0
    FROM message_subscription_pointer;

DROP TABLE message_subscription_pointer;

ALTER TABLE message_subscription_pointer_new RENAME TO message_subscription_pointer;

CREATE UNIQUE INDEX IF NOT EXISTS unique_name_correlation_key_waiting ON message_subscription_pointer(name, correlation_key)
WHERE
    state = 1;

-- Restore message_subscription without the type column and with NOT NULL constraints.
CREATE TABLE message_subscription_new(
    key INTEGER PRIMARY KEY, -- int64 snowflake id of the message subscription where node is partition id which handles the process instance
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

INSERT INTO message_subscription_new
    SELECT key, element_id, process_definition_key, process_instance_key, name, state,
           created_at, correlation_key, execution_token
    FROM message_subscription;

DROP TABLE message_subscription;

ALTER TABLE message_subscription_new RENAME TO message_subscription;

CREATE INDEX IF NOT EXISTS idx_fk_message_subscription_process_instance_key ON message_subscription(process_instance_key);
CREATE INDEX IF NOT EXISTS idx_fk_message_subscription_process_definition_key ON message_subscription(process_definition_key);

PRAGMA foreign_keys = ON;

