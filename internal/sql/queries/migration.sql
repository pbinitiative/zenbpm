-- name: SaveMigration :exec
INSERT INTO migration(name, ran_at)
    VALUES (@name, @ran_at);

-- name: GetMigrations :many
SELECT
    *
FROM
    migration;
