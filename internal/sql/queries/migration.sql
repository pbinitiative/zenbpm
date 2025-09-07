-- Copyright 2021-present ZenBPM Contributors
-- (based on git commit history).
--
-- ZenBPM project is available under two licenses:
--  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
--  - Enterprise License (See LICENSE-ENTERPRISE.md)

-- name: SaveMigration :exec
INSERT INTO migration(name, ran_at)
    VALUES (@name, @ran_at);

-- name: GetMigrations :many
SELECT
    *
FROM
    migration;
