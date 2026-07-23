-- Supports the data cleanup query (FindInactiveInstancesToDelete), which scans
-- for terminal instances whose retention window has elapsed. Without this index
-- every cleanup cycle does a full table scan of process_instance.
CREATE INDEX IF NOT EXISTS idx_process_instance_cleanup ON process_instance(history_delete_sec)
WHERE
    state IN (4, 6, 9);
