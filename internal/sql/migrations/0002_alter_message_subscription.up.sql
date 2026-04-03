BEGIN TRANSACTION;

CREATE TABLE new_message_subscription (
    -- TODO: what about starting events with message listener?
    key INTEGER PRIMARY KEY, -- int64 snowflake id of the message subscription where node is partition id which handles the process instance
    element_id text NOT NULL, -- string id of the element from xml definition
    process_definition_key integer, -- int64 reference to process definition
    process_instance_key integer, -- int64 reference to process instance
    name text NOT NULL, -- message name from the definition
    state integer NOT NULL, -- pkg/bpmn/runtime/types.go:ActivityState
    created_at integer NOT NULL, -- unix millis of when the instance of the message subscription was created
    correlation_key text, -- correlation key used to correlate message in the engine
    execution_token integer, -- key of the execution_token that created message_subscription
    FOREIGN KEY (process_instance_key) REFERENCES process_instance(key), -- reference to process instance
    FOREIGN KEY (process_definition_key) REFERENCES process_definition(key) -- reference to process definition
);

INSERT INTO new_message_subscription (key, element_id, process_definition_key, process_instance_key,name,state,created_at,correlation_key,execution_token)
SELECT key, element_id, process_definition_key, process_instance_key,name,state,created_at,correlation_key,execution_token FROM message_subscription;

DROP TABLE message_subscription;

ALTER TABLE new_message_subscription RENAME TO message_subscription;

CREATE TABLE new_message_subscription_pointer(
    name text NOT NULL, -- message name from the definition
    correlation_key text NOT NULL, -- correlation key used to correlate message in the engine
    state integer NOT NULL, -- reflects message_subscription state
    created_at integer NOT NULL, -- unix millis of when the pointer of the message subscription was created
    message_subscription_key integer NOT NULL, -- key of the message_subscription which this points to
    execution_token_key integer, -- key of the execution_token that created message_subscription
    PRIMARY KEY (name, correlation_key)
);

INSERT INTO new_message_subscription_pointer (name, correlation_key, state, created_at, message_subscription_key, execution_token_key)
SELECT name, correlation_key, state, created_at, message_subscription_key, execution_token_key FROM message_subscription_pointer;

DROP TABLE message_subscription_pointer;

ALTER TABLE new_message_subscription_pointer RENAME TO message_subscription_pointer;

ALTER TABLE message_subscription_pointer ADD process_instance_key integer;

COMMIT;
