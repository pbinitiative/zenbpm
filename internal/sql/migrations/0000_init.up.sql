PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS migration(
    name text PRIMARY KEY, -- migration filename
    ran_at integer NOT NULL -- unix millis of when the migration was applied
);
