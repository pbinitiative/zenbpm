-- Tracks how deep the process instance is in the parent-child chain
-- (call activities, sub processes, multi-instance bodies). The upmost
-- parent process instance has execution_depth = 0. Used to detect
-- potential infinite loops of process instances spawning child instances.
ALTER TABLE process_instance ADD COLUMN execution_depth integer NOT NULL DEFAULT 0;
