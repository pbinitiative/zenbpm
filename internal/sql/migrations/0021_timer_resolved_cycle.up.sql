-- The cycle a timeCycle FEEL expression evaluated to (an ISO 8601 repeating
-- interval or a cron expression), kept so that every following timer of the
-- cycle repeats it instead of evaluating the expression again. NULL for a timer
-- whose definition is a literal, which is read from the model as before.
ALTER TABLE timer ADD COLUMN resolved_cycle TEXT;
