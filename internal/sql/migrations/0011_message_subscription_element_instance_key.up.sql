ALTER TABLE message_subscription
    ADD COLUMN element_instance_key INTEGER; -- int64 id of the subscribed element instance, when applicable
