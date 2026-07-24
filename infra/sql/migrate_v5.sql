-- Meta-learning: strategy memory, rankings, sandbox audit trail

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
