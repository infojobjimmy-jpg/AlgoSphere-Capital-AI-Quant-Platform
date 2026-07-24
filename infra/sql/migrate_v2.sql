-- Run manually on existing volumes: psql -U gaios -d gaios -f migrate_v2.sql

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
