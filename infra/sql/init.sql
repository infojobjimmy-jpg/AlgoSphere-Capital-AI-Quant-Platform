CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS sources (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    source_type TEXT NOT NULL,
    endpoint TEXT,
    metadata JSONB DEFAULT '{}'::jsonb,
    relevance_score DOUBLE PRECISION DEFAULT 0,
    enabled BOOLEAN DEFAULT TRUE,
    discovered_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS observations (
    id BIGSERIAL,
    time TIMESTAMPTZ NOT NULL,
    layer TEXT NOT NULL,
    source_id BIGINT REFERENCES sources (id),
    external_id TEXT,
    payload JSONB NOT NULL,
    PRIMARY KEY (time, id)
);

SELECT create_hypertable('observations', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS observations_layer_time_idx ON observations (layer, time DESC);

CREATE TABLE IF NOT EXISTS alerts (
    id BIGSERIAL PRIMARY KEY,
    severity TEXT NOT NULL,
    title TEXT NOT NULL,
    body TEXT,
    payload JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS discovery_runs (
    id BIGSERIAL PRIMARY KEY,
    query TEXT,
    results JSONB DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS camera_streams (
    id BIGSERIAL PRIMARY KEY,
    slug TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    stream_url TEXT,
    info_url TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    jurisdiction TEXT,
    stream_category TEXT NOT NULL DEFAULT 'unknown',
    health_state TEXT NOT NULL DEFAULT 'unknown',
    last_probe_at TIMESTAMPTZ,
    probe_latency_ms DOUBLE PRECISION,
    http_status INT,
    classification_confidence DOUBLE PRECISION DEFAULT 0,
    metadata JSONB DEFAULT '{}'::jsonb,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS camera_streams_enabled_health_idx
    ON camera_streams (enabled, health_state);

CREATE TABLE IF NOT EXISTS fusion_events (
    id BIGSERIAL,
    time TIMESTAMPTZ NOT NULL,
    h3_cell TEXT NOT NULL,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    summary TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (time, id)
);

SELECT create_hypertable('fusion_events', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS fusion_events_h3_time_idx ON fusion_events (h3_cell, time DESC);

ALTER TABLE sources ADD COLUMN IF NOT EXISTS reliability_score DOUBLE PRECISION DEFAULT 0;
ALTER TABLE sources ADD COLUMN IF NOT EXISTS richness_score DOUBLE PRECISION DEFAULT 0;
ALTER TABLE sources ADD COLUMN IF NOT EXISTS update_frequency_score DOUBLE PRECISION DEFAULT 0;
ALTER TABLE sources ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS decisions (
    id BIGSERIAL,
    time TIMESTAMPTZ NOT NULL,
    event_id TEXT NOT NULL,
    severity TEXT NOT NULL,
    action TEXT NOT NULL,
    confidence DOUBLE PRECISION NOT NULL,
    reasoning JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (time, id)
);

SELECT create_hypertable('decisions', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS decisions_event_time_idx ON decisions (event_id, time DESC);
CREATE INDEX IF NOT EXISTS decisions_severity_time_idx ON decisions (severity, time DESC);

CREATE TABLE IF NOT EXISTS model_performance (
    id BIGSERIAL PRIMARY KEY,
    time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    model_name TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS model_performance_model_time_idx ON model_performance (model_name, time DESC);

CREATE TABLE IF NOT EXISTS goals (
    id BIGSERIAL PRIMARY KEY,
    description TEXT NOT NULL,
    priority DOUBLE PRECISION NOT NULL DEFAULT 5.0,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS goals_status_priority_idx ON goals (status, priority DESC);

CREATE TABLE IF NOT EXISTS plans (
    id BIGSERIAL PRIMARY KEY,
    goal_id BIGINT REFERENCES goals (id),
    title TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',
    steps JSONB NOT NULL DEFAULT '[]'::jsonb,
    context JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS plans_goal_status_idx ON plans (goal_id, status);

CREATE TABLE IF NOT EXISTS plan_executions (
    id BIGSERIAL PRIMARY KEY,
    plan_id BIGINT REFERENCES plans (id) ON DELETE CASCADE,
    step_index INT NOT NULL DEFAULT 0,
    state JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_tick_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS plan_executions_plan_idx ON plan_executions (plan_id);

CREATE TABLE IF NOT EXISTS feedback_records (
    id BIGSERIAL PRIMARY KEY,
    subject_type TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    notes JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS feedback_records_subject_idx ON feedback_records (subject_type, subject_id, created_at DESC);

INSERT INTO goals (description, priority, status, metadata)
SELECT 'Maintain high situational awareness; minimize missed critical air anomalies.', 12.0, 'active', '{"theme":"security"}'::jsonb
WHERE NOT EXISTS (SELECT 1 FROM goals WHERE description LIKE '%missed critical air%');

INSERT INTO goals (description, priority, status, metadata)
SELECT 'Balance alert volume with operator capacity (reduce noise).', 6.0, 'active', '{"theme":"efficiency"}'::jsonb
WHERE NOT EXISTS (SELECT 1 FROM goals WHERE description LIKE '%operator capacity%');

CREATE TABLE IF NOT EXISTS strategy_memory (
    id BIGSERIAL PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    deployment TEXT NOT NULL DEFAULT 'live',
    metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    outcome_score DOUBLE PRECISION,
    plan_id BIGINT,
    guidance_applied JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS strategy_memory_fp_created_idx ON strategy_memory (fingerprint, created_at DESC);
CREATE INDEX IF NOT EXISTS strategy_memory_deploy_idx ON strategy_memory (deployment, created_at DESC);

CREATE TABLE IF NOT EXISTS strategy_rankings (
    fingerprint TEXT PRIMARY KEY,
    aggregate_score DOUBLE PRECISION NOT NULL,
    sample_count INT NOT NULL,
    last_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sandbox_results (
    id BIGSERIAL PRIMARY KEY,
    candidate_fingerprint TEXT NOT NULL,
    baseline_fingerprint TEXT NOT NULL,
    expected_lift DOUBLE PRECISION NOT NULL,
    approved BOOLEAN NOT NULL DEFAULT FALSE,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS sandbox_results_created_idx ON sandbox_results (created_at DESC);
