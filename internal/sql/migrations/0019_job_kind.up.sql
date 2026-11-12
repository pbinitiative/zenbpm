-- What a job belongs to: 0 = its element's own work (a task, a message throw
-- event), 1 = an execution listener of its element. Fixed when the job is
-- created. Every job created before is the job of its element.
ALTER TABLE job ADD COLUMN kind INTEGER NOT NULL DEFAULT 0;
-- The event type of the execution listener whose job this is ('start', 'end');
-- NULL for the job of an element. Fixed when the job is created.
ALTER TABLE job ADD COLUMN listener_event_type TEXT;
