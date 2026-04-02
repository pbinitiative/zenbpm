PRAGMA foreign_keys = OFF;

-- RQLite does not support DROP CONSTRAINT, so we recreate the table without
-- the FOREIGN KEY on flow_element_instance_key.
CREATE TABLE decision_instance_new(
    key INTEGER PRIMARY KEY,
    decision_id TEXT NOT NULL,
    created_at integer NOT NULL,
    output_variables text NOT NULL,
    evaluated_decisions text NOT NULL,
    dmn_resource_definition_key INTEGER NOT NULL,
    decision_definition_key INTEGER NOT NULL,
    process_instance_key INTEGER,
    flow_element_instance_key INTEGER,
    FOREIGN KEY (dmn_resource_definition_key) REFERENCES dmn_resource_definition(key),
    FOREIGN KEY (decision_definition_key) REFERENCES decision_definition(key),
    FOREIGN KEY (process_instance_key) REFERENCES process_instance(key)
);

INSERT INTO decision_instance_new SELECT * FROM decision_instance;

DROP TABLE decision_instance;

ALTER TABLE decision_instance_new RENAME TO decision_instance;

PRAGMA foreign_keys = ON;
