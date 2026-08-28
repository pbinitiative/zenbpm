\set ON_ERROR_STOP on

BEGIN;

CREATE SCHEMA IF NOT EXISTS reporting;

CREATE TABLE IF NOT EXISTS reporting.dmn_resource_definition (
    key BIGINT PRIMARY KEY,
    version BIGINT NOT NULL,
    dmn_resource_definition_id TEXT NOT NULL,
    dmn_data TEXT NOT NULL,
    dmn_checksum BYTEA NOT NULL,
    dmn_definition_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reporting.decision_definition (
    key BIGINT PRIMARY KEY,
    version BIGINT NOT NULL,
    decision_id TEXT NOT NULL,
    version_tag TEXT NOT NULL,
    dmn_resource_definition_id TEXT NOT NULL,
    dmn_resource_definition_key BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS reporting.message_subscription_pointer (
    name TEXT NOT NULL,
    correlation_key TEXT NOT NULL,
    state BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    message_subscription_key BIGINT NOT NULL,
    PRIMARY KEY (name, correlation_key)
);

CREATE UNIQUE INDEX IF NOT EXISTS unique_name_correlation_key_waiting
    ON reporting.message_subscription_pointer (name, correlation_key)
    WHERE state = 1;

CREATE TABLE IF NOT EXISTS reporting.process_definition (
    key BIGINT PRIMARY KEY,
    version BIGINT NOT NULL,
    bpmn_process_id TEXT NOT NULL,
    bpmn_data TEXT NOT NULL,
    bpmn_checksum BYTEA NOT NULL,
    bpmn_process_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reporting.process_instance (
    key BIGINT PRIMARY KEY,
    process_definition_key BIGINT NOT NULL,
    business_key TEXT,
    created_at BIGINT NOT NULL,
    state BIGINT NOT NULL,
    variables TEXT NOT NULL,
    parent_process_execution_token BIGINT,
    parent_process_target_element_id TEXT,
    parent_process_target_element_instance_key BIGINT,
    process_type BIGINT NOT NULL,
    history_ttl_sec BIGINT,
    history_delete_sec BIGINT,
    start_element_id TEXT,
    nesting_depth BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS reporting.decision_instance (
    key BIGINT PRIMARY KEY,
    decision_id TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    output_variables TEXT NOT NULL,
    evaluated_decisions TEXT NOT NULL,
    dmn_resource_definition_key BIGINT NOT NULL,
    decision_definition_key BIGINT NOT NULL,
    process_instance_key BIGINT,
    flow_element_instance_key BIGINT
);

CREATE INDEX IF NOT EXISTS idx_decision_instance_process_instance_key
    ON reporting.decision_instance (process_instance_key);

CREATE TABLE IF NOT EXISTS reporting.error_subscription (
    key BIGINT PRIMARY KEY,
    element_instance_key BIGINT NOT NULL,
    element_id TEXT NOT NULL,
    process_definition_key BIGINT NOT NULL,
    process_instance_key BIGINT NOT NULL,
    error_code TEXT,
    state BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    execution_token BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fk_error_subscription_process_definition_key
    ON reporting.error_subscription (process_definition_key);

CREATE INDEX IF NOT EXISTS idx_fk_error_subscription_process_instance_key
    ON reporting.error_subscription (process_instance_key);

CREATE INDEX IF NOT EXISTS idx_error_subscription_execution_token_state
    ON reporting.error_subscription (execution_token, state);

CREATE TABLE IF NOT EXISTS reporting.execution_token (
    key BIGINT PRIMARY KEY,
    element_instance_key BIGINT NOT NULL,
    element_id TEXT NOT NULL,
    process_instance_key BIGINT NOT NULL,
    state BIGINT NOT NULL,
    created_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fk_execution_token_process_instance_key
    ON reporting.execution_token (process_instance_key);

CREATE INDEX IF NOT EXISTS idx_execution_token_state
    ON reporting.execution_token (state);

CREATE TABLE IF NOT EXISTS reporting.flow_element_instance (
    key BIGINT PRIMARY KEY,
    element_id TEXT NOT NULL,
    process_instance_key BIGINT NOT NULL,
    execution_token_key BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    input_variables TEXT NOT NULL,
    output_variables TEXT NOT NULL,
    completed_at BIGINT,
    element_type TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_fk_flow_element_instance_process_instance_key
    ON reporting.flow_element_instance (process_instance_key);

CREATE TABLE IF NOT EXISTS reporting.incident (
    key BIGINT PRIMARY KEY,
    element_instance_key BIGINT NOT NULL,
    element_id TEXT NOT NULL,
    process_instance_key BIGINT NOT NULL,
    message TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    resolved_at BIGINT,
    execution_token BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fk_incident_process_instance_key
    ON reporting.incident (process_instance_key);

CREATE INDEX IF NOT EXISTS idx_incident_execution_token
    ON reporting.incident (execution_token);

CREATE TABLE IF NOT EXISTS reporting.job (
    key BIGINT PRIMARY KEY,
    element_instance_key BIGINT NOT NULL,
    element_id TEXT NOT NULL,
    element_type TEXT NOT NULL DEFAULT '',
    process_instance_key BIGINT NOT NULL,
    type TEXT NOT NULL,
    state BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    input_variables TEXT NOT NULL,
    execution_token BIGINT NOT NULL,
    assignee TEXT,
    output_variables TEXT
);

CREATE INDEX IF NOT EXISTS idx_fk_job_process_instance_key
    ON reporting.job (process_instance_key);

CREATE INDEX IF NOT EXISTS idx_job_state_type_created_at
    ON reporting.job (state, type, created_at)
    WHERE state = 1;

CREATE INDEX IF NOT EXISTS idx_job_execution_token_state
    ON reporting.job (execution_token, state);

CREATE TABLE IF NOT EXISTS reporting.message_subscription (
    key BIGINT PRIMARY KEY,
    element_id TEXT NOT NULL,
    process_definition_key BIGINT NOT NULL,
    process_instance_key BIGINT,
    name TEXT NOT NULL,
    state BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    correlation_key TEXT,
    execution_token BIGINT,
    type BIGINT NOT NULL,
    element_instance_key BIGINT
);

CREATE INDEX IF NOT EXISTS idx_fk_message_subscription_process_definition_key
    ON reporting.message_subscription (process_definition_key);

CREATE INDEX IF NOT EXISTS idx_fk_message_subscription_process_instance_key
    ON reporting.message_subscription (process_instance_key);

CREATE INDEX IF NOT EXISTS idx_message_subscription_name_state
    ON reporting.message_subscription (name, state);

CREATE INDEX IF NOT EXISTS idx_message_subscription_execution_token_state
    ON reporting.message_subscription (execution_token, state);

CREATE UNIQUE INDEX IF NOT EXISTS unique_active_definition_message_subscription
    ON reporting.message_subscription (process_definition_key, element_id, name)
    WHERE type = 3 AND state = 1;

CREATE INDEX IF NOT EXISTS idx_fk_process_instance_process_definition_key
    ON reporting.process_instance (process_definition_key);

CREATE INDEX IF NOT EXISTS idx_process_instance_cleanup
    ON reporting.process_instance (history_delete_sec)
    WHERE state IN (4, 6, 9);

CREATE INDEX IF NOT EXISTS idx_process_instance_definition_start_element_state
    ON reporting.process_instance (process_definition_key, start_element_id, state);

CREATE INDEX IF NOT EXISTS idx_process_instance_parent_execution_token
    ON reporting.process_instance (parent_process_execution_token);

CREATE INDEX IF NOT EXISTS idx_process_instance_definition_created_at
    ON reporting.process_instance (process_definition_key, created_at);

CREATE INDEX IF NOT EXISTS idx_process_instance_state
    ON reporting.process_instance (state);

CREATE TABLE IF NOT EXISTS reporting.timer (
    key BIGINT PRIMARY KEY,
    element_instance_key BIGINT,
    element_id TEXT NOT NULL,
    process_definition_key BIGINT NOT NULL,
    process_instance_key BIGINT,
    state BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    due_at BIGINT NOT NULL,
    execution_token BIGINT
);

CREATE INDEX IF NOT EXISTS idx_fk_timer_process_definition_key
    ON reporting.timer (process_definition_key);

CREATE INDEX IF NOT EXISTS idx_fk_timer_process_instance_key
    ON reporting.timer (process_instance_key);

CREATE INDEX IF NOT EXISTS idx_timer_state_due_at
    ON reporting.timer (state, due_at);

CREATE INDEX IF NOT EXISTS idx_timer_execution_token_state
    ON reporting.timer (execution_token, state);

CREATE TABLE IF NOT EXISTS reporting.cdc_event (
    service_id TEXT NOT NULL,
    raft_index NUMERIC(20, 0) NOT NULL CHECK (raft_index >= 0),
    event_position INTEGER NOT NULL CHECK (event_position >= 0),
    node_id TEXT NOT NULL,
    commit_timestamp_ms BIGINT NOT NULL,
    operation TEXT NOT NULL,
    table_name TEXT,
    old_row_id BIGINT,
    new_row_id BIGINT,
    before_data JSONB,
    after_data JSONB,
    error TEXT,
    event_data JSONB NOT NULL,
    applied BOOLEAN NOT NULL DEFAULT FALSE,
    skip_reason TEXT,
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    applied_at TIMESTAMPTZ,
    PRIMARY KEY (service_id, raft_index, event_position)
);

CREATE INDEX IF NOT EXISTS idx_cdc_event_table_received_at
    ON reporting.cdc_event (table_name, received_at);

CREATE OR REPLACE FUNCTION reporting.normalize_cdc_row(
    p_table_name TEXT,
    p_row JSONB
) RETURNS JSONB
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    v_bytea_columns TEXT[];
    v_column TEXT;
BEGIN
    IF p_row IS NULL THEN
        RETURN NULL;
    END IF;

    CASE p_table_name
        WHEN 'dmn_resource_definition' THEN
            v_bytea_columns := ARRAY['dmn_checksum'];
        WHEN 'process_definition' THEN
            v_bytea_columns := ARRAY['bpmn_checksum'];
        ELSE
            RETURN p_row;
    END CASE;

    FOREACH v_column IN ARRAY v_bytea_columns LOOP
        IF p_row ? v_column AND jsonb_typeof(p_row -> v_column) = 'string' THEN
            p_row := jsonb_set(
                p_row,
                ARRAY[v_column],
                to_jsonb(E'\\x' || encode(decode(p_row ->> v_column, 'base64'), 'hex'))
            );
        END IF;
    END LOOP;

    RETURN p_row;
END;
$$;

CREATE OR REPLACE FUNCTION reporting.apply_cdc_event(
    p_service_id TEXT,
    p_node_id TEXT,
    p_raft_index NUMERIC,
    p_commit_timestamp_ms BIGINT,
    p_event_position INTEGER,
    p_operation TEXT,
    p_table_name TEXT,
    p_old_row_id BIGINT,
    p_new_row_id BIGINT,
    p_before JSONB,
    p_after JSONB,
    p_error TEXT,
    p_event JSONB
) RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    v_allowed_tables CONSTANT TEXT[] := ARRAY[
        'dmn_resource_definition',
        'decision_definition',
        'message_subscription_pointer',
        'process_definition',
        'process_instance',
        'decision_instance',
        'error_subscription',
        'execution_token',
        'flow_element_instance',
        'incident',
        'job',
        'message_subscription',
        'timer'
    ];
    v_before JSONB;
    v_after JSONB;
    v_columns TEXT;
    v_update_assignments TEXT;
    v_conflict_assignments TEXT;
    v_primary_key_columns TEXT;
    v_primary_key_predicate TEXT;
    v_primary_key_count INTEGER;
    v_single_primary_key TEXT;
    v_unknown_columns TEXT;
    v_affected_rows BIGINT;
    v_upsert_sql TEXT;
BEGIN
    -- Reporting intentionally retains historical rows after source cleanup.
    -- Redpanda Connect filters DELETE events, and direct callers follow the
    -- same contract without recording or applying them.
    IF UPPER(COALESCE(p_operation, '')) = 'DELETE' THEN
        RETURN FALSE;
    END IF;

    INSERT INTO reporting.cdc_event (
        service_id,
        raft_index,
        event_position,
        node_id,
        commit_timestamp_ms,
        operation,
        table_name,
        old_row_id,
        new_row_id,
        before_data,
        after_data,
        error,
        event_data
    ) VALUES (
        p_service_id,
        p_raft_index,
        p_event_position,
        p_node_id,
        p_commit_timestamp_ms,
        p_operation,
        p_table_name,
        p_old_row_id,
        p_new_row_id,
        p_before,
        p_after,
        NULLIF(p_error, ''),
        p_event
    )
    ON CONFLICT (service_id, raft_index, event_position) DO NOTHING;

    IF NOT FOUND THEN
        RETURN FALSE;
    END IF;

    IF p_error IS NOT NULL AND p_error <> '' THEN
        UPDATE reporting.cdc_event
        SET skip_reason = 'source CDC error: ' || p_error
        WHERE service_id = p_service_id
          AND raft_index = p_raft_index
          AND event_position = p_event_position;
        RETURN FALSE;
    END IF;

    IF p_table_name IS NULL OR NOT (p_table_name = ANY (v_allowed_tables)) THEN
        UPDATE reporting.cdc_event
        SET skip_reason = 'table is not part of the reporting projection'
        WHERE service_id = p_service_id
          AND raft_index = p_raft_index
          AND event_position = p_event_position;
        RETURN FALSE;
    END IF;

    IF p_operation IS NULL OR p_operation NOT IN ('INSERT', 'UPDATE') THEN
        UPDATE reporting.cdc_event
        SET skip_reason = 'unsupported CDC operation: ' || COALESCE(p_operation, '<null>')
        WHERE service_id = p_service_id
          AND raft_index = p_raft_index
          AND event_position = p_event_position;
        RETURN FALSE;
    END IF;

    SELECT
        string_agg(format('%I', column_name), ', ' ORDER BY ordinal_position),
        string_agg(format('%1$I = source.%1$I', column_name), ', ' ORDER BY ordinal_position),
        string_agg(format('%1$I = EXCLUDED.%1$I', column_name), ', ' ORDER BY ordinal_position)
    INTO v_columns, v_update_assignments, v_conflict_assignments
    FROM information_schema.columns
    WHERE table_schema = 'reporting'
      AND table_name = p_table_name;

    SELECT
        string_agg(format('%I', kcu.column_name), ', ' ORDER BY kcu.ordinal_position),
        string_agg(format('target.%1$I = old_row.%1$I', kcu.column_name), ' AND ' ORDER BY kcu.ordinal_position),
        count(*),
        min(kcu.column_name)
    INTO v_primary_key_columns, v_primary_key_predicate, v_primary_key_count, v_single_primary_key
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
      ON kcu.constraint_catalog = tc.constraint_catalog
     AND kcu.constraint_schema = tc.constraint_schema
     AND kcu.constraint_name = tc.constraint_name
    WHERE tc.table_schema = 'reporting'
      AND tc.table_name = p_table_name
      AND tc.constraint_type = 'PRIMARY KEY';

    IF v_columns IS NULL OR v_primary_key_columns IS NULL THEN
        RAISE EXCEPTION 'reporting table % is missing or has no primary key', p_table_name;
    END IF;

    SELECT string_agg(source_column, ', ' ORDER BY source_column)
    INTO v_unknown_columns
    FROM (
        SELECT jsonb_object_keys(COALESCE(p_before, '{}'::JSONB)) AS source_column
        UNION
        SELECT jsonb_object_keys(COALESCE(p_after, '{}'::JSONB)) AS source_column
    ) AS source_columns
    WHERE NOT EXISTS (
        SELECT 1
        FROM information_schema.columns AS target_columns
        WHERE target_columns.table_schema = 'reporting'
          AND target_columns.table_name = p_table_name
          AND target_columns.column_name = source_columns.source_column
    );

    IF v_unknown_columns IS NOT NULL THEN
        RAISE EXCEPTION 'CDC row for reporting table % contains unknown column(s): %',
            p_table_name, v_unknown_columns
            USING ERRCODE = 'undefined_column';
    END IF;

    v_before := reporting.normalize_cdc_row(p_table_name, p_before);
    v_after := reporting.normalize_cdc_row(p_table_name, p_after);

    IF v_before IS NULL
       AND p_old_row_id IS NOT NULL
       AND v_primary_key_count = 1
       AND v_single_primary_key = 'key' THEN
        v_before := jsonb_build_object('key', p_old_row_id);
    END IF;

    v_upsert_sql := format(
        'INSERT INTO reporting.%1$I (%2$s) '
        'SELECT %2$s FROM jsonb_populate_record(NULL::reporting.%1$I, $1) AS source '
        'ON CONFLICT (%3$s) DO UPDATE SET %4$s',
        p_table_name,
        v_columns,
        v_primary_key_columns,
        v_conflict_assignments
    );

    CASE p_operation
        WHEN 'INSERT' THEN
            IF v_after IS NULL THEN
                UPDATE reporting.cdc_event
                SET skip_reason = 'INSERT event has no after image'
                WHERE service_id = p_service_id
                  AND raft_index = p_raft_index
                  AND event_position = p_event_position;
                RETURN FALSE;
            END IF;
            EXECUTE v_upsert_sql USING v_after;

        WHEN 'UPDATE' THEN
            IF v_before IS NULL OR v_after IS NULL THEN
                UPDATE reporting.cdc_event
                SET skip_reason = 'UPDATE event requires before and after images'
                WHERE service_id = p_service_id
                  AND raft_index = p_raft_index
                  AND event_position = p_event_position;
                RETURN FALSE;
            END IF;

            EXECUTE format(
                'UPDATE reporting.%1$I AS target SET %2$s '
                'FROM jsonb_populate_record(NULL::reporting.%1$I, $1) AS source, '
                'jsonb_populate_record(NULL::reporting.%1$I, $2) AS old_row '
                'WHERE %3$s',
                p_table_name,
                v_update_assignments,
                v_primary_key_predicate
            ) USING v_after, v_before;

            GET DIAGNOSTICS v_affected_rows = ROW_COUNT;
            IF v_affected_rows = 0 THEN
                EXECUTE v_upsert_sql USING v_after;
            END IF;

    END CASE;

    UPDATE reporting.cdc_event
    SET applied = TRUE,
        applied_at = CURRENT_TIMESTAMP
    WHERE service_id = p_service_id
      AND raft_index = p_raft_index
      AND event_position = p_event_position;

    RETURN TRUE;
END;
$$;

COMMIT;
