-- covers process instance listing by definition ordered by created_at,
-- so the page query reads rows in index order instead of sorting all
-- instances of the definition in a temp b-tree
CREATE INDEX IF NOT EXISTS idx_process_instance_definition_created_at
    ON process_instance(process_definition_key, created_at);
