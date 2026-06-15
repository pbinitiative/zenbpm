ALTER TABLE job RENAME COLUMN variables TO input_variables;
ALTER TABLE job ADD COLUMN output_variables TEXT;
