-- A numeric process version identifies exactly one deployed definition for a BPMN process id.
CREATE UNIQUE INDEX IF NOT EXISTS idx_process_definition_bpmn_process_id_version
    ON process_definition(bpmn_process_id, version);

-- version_tag holds the optional deployment version tag (zenbpm:versionTag value)
-- for each BPMN process definition. A non-null value is matched exactly when a
-- Call Activity uses bindingType="versionTag". Empty string means no tag.
ALTER TABLE process_definition ADD COLUMN version_tag text NOT NULL DEFAULT '';

-- A non-empty version tag is a stable Call Activity selector and must identify one definition.
CREATE UNIQUE INDEX IF NOT EXISTS idx_process_definition_bpmn_process_id_version_tag
    ON process_definition(bpmn_process_id, version_tag)
    WHERE version_tag != '';
