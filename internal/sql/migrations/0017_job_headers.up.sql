-- Static key/value task headers (taskHeaders extension) attached to a job and
-- handed to the job worker. Stored as a JSON object; '{}' when the BPMN element
-- defines no headers.
ALTER TABLE job ADD COLUMN headers TEXT NOT NULL DEFAULT '{}';
