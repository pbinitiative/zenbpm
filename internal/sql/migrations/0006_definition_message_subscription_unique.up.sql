-- Enforce the invariant that at most one ACTIVE process-definition-level message subscription
-- (type = 3 == runtime.MessageSubscriptionTypeDefinition, state = 1 == runtime.ActivityStateActive)
-- can exist per (process_definition_key, element_id, name). This is the storage-level safety net for the
-- re-arm TOCTOU of instantiating receive tasks: two concurrently-completing instances of the same
-- definition can no longer both insert an active definition subscription for the same receive task.
CREATE UNIQUE INDEX IF NOT EXISTS unique_active_definition_message_subscription
    ON message_subscription(process_definition_key, element_id, name)
WHERE
    type = 3 AND state = 1;
