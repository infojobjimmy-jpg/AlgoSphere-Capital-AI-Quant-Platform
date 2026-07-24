-- Strategic goals & planning: psql -U gaios -d gaios -f migrate_v4.sql

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
