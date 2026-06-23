-- Enforce the invariant that at most one ACTIVE process-definition-level message subscription
-- (type = 3 == runtime.MessageSubscriptionTypeDefinition, state = 1 == runtime.ActivityStateActive)
-- can exist per (process_definition_key, element_id, name). This is the storage-level safety net for the
-- re-arm TOCTOU of instantiating receive tasks: two concurrently-completing instances of the same
-- definition can no longer both insert an active definition subscription for the same receive task.
-- Cluster-wide correctness is provided by routing definition-level publishes and recovery to the owning
-- subscription partition; this index remains the final storage-level safety net inside that partition.
CREATE UNIQUE INDEX IF NOT EXISTS unique_active_definition_message_subscription
    ON message_subscription(process_definition_key, element_id, name)
WHERE
    type = 3 AND state = 1;

-- Definition-level subscriptions did not have routing pointers before keyed definition publishes were
-- introduced. Backfill one active pointer per message name so already-deployed message start events and
-- instantiating receive tasks remain publishable after upgrade.
INSERT INTO message_subscription_pointer(state, created_at, name, correlation_key, message_subscription_key)
SELECT
    ms.state,
    ms.created_at,
    ms.name,
    '',
    ms.key
FROM
    message_subscription AS ms
WHERE
    ms.type = 3
    AND ms.state = 1
    AND ms.key IN (
        SELECT
            MAX(key)
        FROM
            message_subscription
        WHERE
            type = 3
            AND state = 1
        GROUP BY
            name)
ON CONFLICT(name, correlation_key)
    DO UPDATE SET
        state = excluded.state,
        created_at = excluded.created_at,
        message_subscription_key = excluded.message_subscription_key
    WHERE
        message_subscription_pointer.state != 1;
