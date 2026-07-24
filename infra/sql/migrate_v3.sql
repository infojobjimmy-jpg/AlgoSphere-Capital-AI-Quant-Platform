-- Apply on existing DBs: psql -U gaios -d gaios -f migrate_v3.sql

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
