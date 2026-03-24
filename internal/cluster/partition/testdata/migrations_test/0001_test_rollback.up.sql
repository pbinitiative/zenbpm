CREATE TABLE rollback_probe (
    id INTEGER PRIMARY KEY
);

COMMIT;

CREATE INDEX rollback_probe_id_idx ON rollback_probe (ida);
