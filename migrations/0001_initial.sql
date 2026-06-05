PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;
PRAGMA foreign_keys = ON;

CREATE TABLE schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT INTO schema_meta(key, value) VALUES ('schema_version', '1');
INSERT INTO schema_meta(key, value) VALUES ('time_unit', 'unix_epoch_us');

CREATE TABLE plc (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    host TEXT NOT NULL,
    rack INTEGER NOT NULL DEFAULT 0,
    slot INTEGER NOT NULL DEFAULT 2,
    enabled INTEGER NOT NULL DEFAULT 1,
    last_ok_ts INTEGER,
    last_error TEXT,
    connectivity TEXT NOT NULL DEFAULT 'unknown'
        CHECK (connectivity IN ('online', 'offline', 'unknown'))
);

CREATE TABLE poll_group (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    period_ms INTEGER NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE metric_set (
    id INTEGER PRIMARY KEY,
    plc_id INTEGER NOT NULL REFERENCES plc(id),
    name TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 0,
    description TEXT,
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL,
    UNIQUE(plc_id, name),
    UNIQUE(id, plc_id)
);

CREATE TABLE tag (
    id INTEGER PRIMARY KEY,
    plc_id INTEGER NOT NULL REFERENCES plc(id),
    name TEXT NOT NULL,
    address TEXT NOT NULL,
    db_num INTEGER NOT NULL,
    byte_offset INTEGER NOT NULL,
    bit_offset INTEGER,
    byte_size INTEGER NOT NULL,
    data_type TEXT NOT NULL,
    value_role TEXT NOT NULL DEFAULT 'state',
    role TEXT,
    domain TEXT,
    poll_group_id INTEGER NOT NULL REFERENCES poll_group(id),
    store_mode TEXT NOT NULL DEFAULT 'on_change',
    storage_hook TEXT NOT NULL DEFAULT 'on_change',
    hook_params_json TEXT,
    deadband REAL,
    enabled INTEGER NOT NULL DEFAULT 1,
    bad INTEGER NOT NULL DEFAULT 0,
    bad_class TEXT,
    bad_detail TEXT,
    last_error_ts INTEGER,
    desired_version INTEGER NOT NULL DEFAULT 1,
    applied_version INTEGER NOT NULL DEFAULT 0,
    apply_state TEXT NOT NULL DEFAULT 'pending',
    version INTEGER NOT NULL DEFAULT 1,
    last_value_i INTEGER,
    last_value_f REAL,
    last_ts INTEGER,
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL,
    UNIQUE(plc_id, name),
    UNIQUE(id, plc_id),
    CHECK(data_type IN ('bool', 'i16', 'u16', 'i32', 'u32', 'f32')),
    CHECK(value_role IN ('state', 'counter', 'position', 'speed', 'temperature', 'diagnostic')),
    CHECK(store_mode IN ('on_change', 'deadband', 'periodic', 'rollup')),
    CHECK(storage_hook IN (
        'on_change',
        'deadband',
        'periodic',
        'min_max_rollup',
        'delta_rollup',
        'state_duration',
        'counter_delta'
    )),
    CHECK(bad_class IS NULL OR bad_class IN (
        'address_parse',
        'address_out_of_range',
        'decode_error',
        'timeout',
        'disconnected',
        'protocol_error',
        'plc_unavailable',
        'internal'
    )),
    CHECK(apply_state IN ('pending', 'applied', 'bad'))
);

CREATE TABLE metric_set_tag (
    metric_set_id INTEGER NOT NULL REFERENCES metric_set(id) ON DELETE CASCADE,
    tag_id INTEGER NOT NULL REFERENCES tag(id) ON DELETE CASCADE,
    plc_id INTEGER NOT NULL REFERENCES plc(id),
    created_ts INTEGER NOT NULL,
    PRIMARY KEY(metric_set_id, tag_id),
    FOREIGN KEY(metric_set_id, plc_id) REFERENCES metric_set(id, plc_id) ON DELETE CASCADE,
    FOREIGN KEY(tag_id, plc_id) REFERENCES tag(id, plc_id) ON DELETE CASCADE
);

CREATE INDEX ix_metric_set_plc_enabled ON metric_set(plc_id, enabled);
CREATE INDEX ix_metric_set_tag_tag ON metric_set_tag(tag_id);
CREATE INDEX ix_metric_set_tag_plc ON metric_set_tag(plc_id);

CREATE TABLE dataset (
    id INTEGER PRIMARY KEY,
    plc_id INTEGER NOT NULL REFERENCES plc(id),
    name TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'stopped',
    started_ts INTEGER,
    stopped_ts INTEGER,
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL,
    note TEXT,
    UNIQUE(plc_id, name),
    UNIQUE(id, plc_id),
    CHECK(state IN ('active', 'stopped'))
);

CREATE UNIQUE INDEX ux_dataset_one_active_per_plc
ON dataset(plc_id)
WHERE state = 'active';

CREATE TABLE dataset_metric_set (
    dataset_id INTEGER NOT NULL REFERENCES dataset(id) ON DELETE CASCADE,
    metric_set_id INTEGER NOT NULL REFERENCES metric_set(id),
    plc_id INTEGER NOT NULL REFERENCES plc(id),
    created_ts INTEGER NOT NULL,
    PRIMARY KEY(dataset_id, metric_set_id),
    FOREIGN KEY(dataset_id, plc_id) REFERENCES dataset(id, plc_id) ON DELETE CASCADE,
    FOREIGN KEY(metric_set_id, plc_id) REFERENCES metric_set(id, plc_id)
);

CREATE INDEX ix_dataset_plc_state ON dataset(plc_id, state);
CREATE INDEX ix_dataset_metric_set_metric_set ON dataset_metric_set(metric_set_id);

CREATE TABLE poll_cycle (
    id INTEGER PRIMARY KEY,
    dataset_id INTEGER NOT NULL REFERENCES dataset(id),
    plc_id INTEGER NOT NULL REFERENCES plc(id),
    poll_group_id INTEGER NOT NULL REFERENCES poll_group(id),
    seq INTEGER NOT NULL,
    started_ts INTEGER NOT NULL,
    finished_ts INTEGER NOT NULL,
    target_ms INTEGER NOT NULL,
    actual_ms REAL NOT NULL,
    ok INTEGER NOT NULL,
    error TEXT,
    UNIQUE(dataset_id, poll_group_id, seq)
);

CREATE TABLE read_span (
    id INTEGER PRIMARY KEY,
    plc_id INTEGER NOT NULL REFERENCES plc(id),
    poll_group_id INTEGER NOT NULL REFERENCES poll_group(id),
    db_num INTEGER NOT NULL,
    byte_offset INTEGER NOT NULL,
    byte_len INTEGER NOT NULL,
    active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE sample (
    dataset_id INTEGER NOT NULL REFERENCES dataset(id),
    tag_id INTEGER NOT NULL REFERENCES tag(id),
    cycle_id INTEGER REFERENCES poll_cycle(id),
    span_id INTEGER REFERENCES read_span(id),
    ts INTEGER NOT NULL,
    vi INTEGER,
    vf REAL
);

CREATE INDEX ix_sample_tag_ts ON sample(tag_id, ts);
CREATE INDEX ix_sample_dataset_ts ON sample(dataset_id, ts);
CREATE INDEX ix_sample_cycle ON sample(cycle_id);
CREATE INDEX ix_sample_span ON sample(span_id);
CREATE INDEX ix_tag_plc_enabled_bad ON tag(plc_id, enabled, bad);
CREATE INDEX ix_tag_poll_group ON tag(poll_group_id);
CREATE INDEX ix_tag_role_domain ON tag(role, domain);

CREATE TABLE sample_rollup (
    dataset_id INTEGER NOT NULL REFERENCES dataset(id),
    tag_id INTEGER NOT NULL REFERENCES tag(id),
    plc_id INTEGER NOT NULL REFERENCES plc(id),
    poll_group_id INTEGER NOT NULL REFERENCES poll_group(id),
    window_start_ts INTEGER NOT NULL,
    window_end_ts INTEGER NOT NULL,
    sample_count INTEGER NOT NULL,
    first_vi INTEGER,
    last_vi INTEGER,
    min_vi INTEGER,
    max_vi INTEGER,
    delta_vi INTEGER,
    first_vf REAL,
    last_vf REAL,
    min_vf REAL,
    max_vf REAL,
    delta_vf REAL
);

CREATE INDEX ix_sample_rollup_tag_window ON sample_rollup(tag_id, window_start_ts);
CREATE INDEX ix_sample_rollup_dataset_window ON sample_rollup(dataset_id, window_start_ts);

CREATE TABLE plc_time_sample (
    id INTEGER PRIMARY KEY,
    dataset_id INTEGER REFERENCES dataset(id),
    plc_id INTEGER NOT NULL REFERENCES plc(id),
    cycle_id INTEGER REFERENCES poll_cycle(id),
    host_before_ts INTEGER NOT NULL,
    host_after_ts INTEGER NOT NULL,
    plc_datetime TEXT,
    clock_byte INTEGER,
    ob1_cycle_ms REAL,
    source TEXT NOT NULL
);

CREATE INDEX ix_plc_time_sample_plc_ts ON plc_time_sample(plc_id, host_before_ts);

CREATE VIEW v_tag_status AS
SELECT
    plc.name AS plc,
    tag.name AS tag,
    tag.address,
    tag.data_type,
    poll_group.name AS poll_group,
    tag.enabled,
    tag.bad,
    tag.bad_class,
    tag.bad_detail,
    tag.apply_state,
    tag.desired_version,
    tag.applied_version,
    tag.last_error_ts,
    tag.last_ts,
    tag.last_value_i,
    tag.last_value_f
FROM tag
JOIN plc ON plc.id = tag.plc_id
JOIN poll_group ON poll_group.id = tag.poll_group_id;

CREATE VIEW v_metric_set_member AS
SELECT
    plc.name AS plc,
    metric_set.name AS metric_set,
    metric_set.enabled AS metric_set_enabled,
    tag.id AS tag_id,
    tag.name AS tag,
    tag.address,
    tag.enabled AS tag_enabled,
    tag.bad,
    tag.bad_class,
    tag.bad_detail
FROM metric_set
JOIN plc ON plc.id = metric_set.plc_id
JOIN metric_set_tag ON metric_set_tag.metric_set_id = metric_set.id
JOIN tag ON tag.id = metric_set_tag.tag_id;

CREATE VIEW v_metric_set_status AS
SELECT
    plc.name AS plc,
    metric_set.name AS metric_set,
    metric_set.enabled,
    count(metric_set_tag.tag_id) AS tags_total,
    sum(CASE WHEN tag.enabled = 1 AND tag.bad = 0 THEN 1 ELSE 0 END) AS tags_healthy,
    sum(CASE WHEN tag.bad = 1 THEN 1 ELSE 0 END) AS tags_bad
FROM metric_set
JOIN plc ON plc.id = metric_set.plc_id
LEFT JOIN metric_set_tag ON metric_set_tag.metric_set_id = metric_set.id
LEFT JOIN tag ON tag.id = metric_set_tag.tag_id
GROUP BY plc.name, metric_set.name, metric_set.enabled;

CREATE VIEW v_active_tag AS
SELECT DISTINCT
    tag.*
FROM tag
JOIN metric_set_tag ON metric_set_tag.tag_id = tag.id
JOIN metric_set ON metric_set.id = metric_set_tag.metric_set_id
WHERE tag.enabled = 1
  AND tag.bad = 0
  AND metric_set.enabled = 1;

CREATE VIEW v_latest_sample AS
SELECT
    plc.name AS plc,
    tag.name AS tag,
    tag.address,
    tag.last_ts AS ts,
    tag.last_value_i AS vi,
    tag.last_value_f AS vf
FROM tag
JOIN plc ON plc.id = tag.plc_id;

CREATE VIEW v_transition AS
SELECT
    plc.name AS plc,
    tag.name AS tag,
    tag.address,
    sample.cycle_id,
    sample.span_id,
    sample.ts,
    lag(sample.vi) OVER (PARTITION BY sample.tag_id ORDER BY sample.ts) AS old_vi,
    sample.vi AS new_vi,
    lag(sample.vf) OVER (PARTITION BY sample.tag_id ORDER BY sample.ts) AS old_vf,
    sample.vf AS new_vf
FROM sample
JOIN tag ON tag.id = sample.tag_id
JOIN plc ON plc.id = tag.plc_id;

CREATE TABLE event_marker (
    id INTEGER PRIMARY KEY,
    dataset_id INTEGER REFERENCES dataset(id),
    plc_id INTEGER NOT NULL REFERENCES plc(id),
    cycle_id INTEGER REFERENCES poll_cycle(id),
    ts INTEGER NOT NULL,
    kind TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'info',
    tag_id INTEGER REFERENCES tag(id),
    value_i INTEGER,
    value_f REAL,
    details_json TEXT
);

CREATE INDEX ix_event_marker_kind_ts ON event_marker(kind, ts);
CREATE INDEX ix_event_marker_cycle ON event_marker(cycle_id);

CREATE TABLE raw_probe (
    id INTEGER PRIMARY KEY,
    dataset_id INTEGER REFERENCES dataset(id),
    plc_id INTEGER NOT NULL REFERENCES plc(id),
    db_num INTEGER NOT NULL,
    byte_offset INTEGER NOT NULL,
    byte_len INTEGER NOT NULL,
    ts INTEGER NOT NULL,
    bytes BLOB NOT NULL,
    note TEXT
);

CREATE INDEX ix_raw_probe_plc_db_ts ON raw_probe(plc_id, db_num, ts);

CREATE TABLE command_request (
    id INTEGER PRIMARY KEY,
    created_ts INTEGER NOT NULL,
    claimed_ts INTEGER,
    lease_expires_ts INTEGER,
    heartbeat_ts INTEGER,
    finished_ts INTEGER,
    state TEXT NOT NULL DEFAULT 'pending',
    priority INTEGER NOT NULL DEFAULT 100,
    plc_id INTEGER REFERENCES plc(id),
    kind TEXT NOT NULL,
    request_json TEXT NOT NULL,
    requested_by TEXT,
    idempotency_key TEXT,
    idempotency_expires_ts INTEGER,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    CHECK(state IN ('pending', 'running', 'done', 'bad', 'cancelled')),
    UNIQUE(idempotency_key)
);

CREATE INDEX ix_command_request_state_priority ON command_request(state, priority, created_ts);
CREATE INDEX ix_command_request_plc_state ON command_request(plc_id, state, created_ts);

CREATE TABLE command_result (
    command_id INTEGER PRIMARY KEY REFERENCES command_request(id),
    ok INTEGER NOT NULL,
    result_json TEXT,
    error TEXT
);

CREATE TABLE config_apply (
    id INTEGER PRIMARY KEY,
    created_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL,
    apply_after_ts INTEGER NOT NULL,
    applied_ts INTEGER,
    state TEXT NOT NULL DEFAULT 'pending',
    scope TEXT NOT NULL,
    plc_id INTEGER REFERENCES plc(id),
    poll_group_id INTEGER REFERENCES poll_group(id),
    metric_set_id INTEGER REFERENCES metric_set(id),
    tag_id INTEGER REFERENCES tag(id),
    reason TEXT,
    cooldown_ms INTEGER NOT NULL,
    details_json TEXT,
    CHECK(state IN ('pending', 'applying', 'done', 'bad', 'cancelled'))
);

CREATE INDEX ix_config_apply_due ON config_apply(state, apply_after_ts);
CREATE INDEX ix_config_apply_scope ON config_apply(scope, plc_id, poll_group_id, metric_set_id, tag_id, state);

CREATE TABLE history_cleanup_policy (
    id INTEGER PRIMARY KEY,
    poll_group_id INTEGER REFERENCES poll_group(id),
    cutoff_days INTEGER,
    delete_batch_rows INTEGER NOT NULL DEFAULT 10000,
    enabled INTEGER NOT NULL DEFAULT 1
);

CREATE UNIQUE INDEX ux_history_cleanup_policy_enabled_scope
ON history_cleanup_policy(coalesce(poll_group_id, -1))
WHERE enabled = 1;
