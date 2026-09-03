-- Runtime execution-control column used to prevent infinite sequence-flow loops within a
-- single process instance. The counter is incremented every time an execution token activates
-- any flow node of the instance. It lives on process_instance (like nesting_depth) so it shares
-- the lifecycle of the instance and is removed together with the rest of the instance data
-- during history cleanup.
-- No backfill is performed: instances running at upgrade time simply start counting from 0.
ALTER TABLE incident ADD COLUMN incident_type TEXT NOT NULL DEFAULT '';

ALTER TABLE process_instance ADD COLUMN flow_node_count INTEGER NOT NULL DEFAULT 0;
