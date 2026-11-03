-- The suspension of a BPMN process: while it is suspended no new instance of any of its versions starts.
-- Instances which are already running are not affected. A process without a row is not suspended.
CREATE TABLE IF NOT EXISTS process_suspension(
    bpmn_process_id TEXT PRIMARY KEY, -- id of the process from xml definition
    suspended INTEGER NOT NULL, -- 1 while the process is suspended, 0 once it is resumed
    version INTEGER NOT NULL -- sequence number of the last change; the partitions keep the change with the highest
);
