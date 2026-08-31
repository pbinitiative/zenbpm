-- Runtime execution-control table used to prevent infinite sequence-flow loops within a
-- single process instance. One row per process instance; the counter is incremented every
-- time an execution token activates any flow element of the instance.
-- This is NOT an audit table: rows live exactly as long as the process instance and are
-- deleted together with the rest of the instance data during history cleanup.
-- No backfill is performed: instances running at upgrade time simply start counting from 0.
ALTER TABLE incident ADD COLUMN incident_type TEXT NOT NULL DEFAULT '';

CREATE TABLE element_execution_counter (
    process_instance_key INTEGER NOT NULL,
    execution_count      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (process_instance_key)
);
