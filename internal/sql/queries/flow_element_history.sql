-- Copyright 2021-present ZenBPM Contributors
-- (based on git commit history).
--
-- ZenBPM project is available under two licenses:
--  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
--  - Enterprise License (See LICENSE-ENTERPRISE.md)

-- name: SaveFlowElementHistory :exec
INSERT INTO flow_element_history(key, element_id, process_instance_key, created_at)
    VALUES (?, ? ,? ,?)
ON CONFLICT
    DO UPDATE SET
       process_instance_key = excluded.process_instance_key,
       element_id = excluded.element_id;

-- name: GetFlowElementHistory :many
SELECT
    *
FROM
    flow_element_history
WHERE
    process_instance_key = @process_instance_key;
