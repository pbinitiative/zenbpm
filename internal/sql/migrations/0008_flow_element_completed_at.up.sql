ALTER TABLE flow_element_instance ADD COLUMN completed_at INTEGER;
ALTER TABLE flow_element_instance ADD COLUMN element_type TEXT NOT NULL DEFAULT '';
