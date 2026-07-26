-- Canonical DDL for visitor_sessions.
-- IMPORTANT: backend/app/routers/analytics.py/_VISITOR_SESSIONS_DDL must mirror this file.
-- When adding columns here, also add them in analytics.py AND add ALTER TABLE guards below.

CREATE TABLE IF NOT EXISTS visitor_sessions (
    id                  BIGSERIAL PRIMARY KEY,
    visitor_session_id  TEXT        NOT NULL,
    visitor_id          TEXT,
    member_id           TEXT,
    username            TEXT,
    product_id          TEXT,
    plan                TEXT,
    authenticated       BOOLEAN     NOT NULL DEFAULT FALSE,
    first_seen          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    duration_seconds    INTEGER     NOT NULL DEFAULT 0,
    page_views          INTEGER     NOT NULL DEFAULT 1,
    heartbeat_count     INTEGER     NOT NULL DEFAULT 1,
    landing_page        TEXT,
    last_page           TEXT,
    referrer_domain     TEXT,
    utm_source          TEXT,
    utm_campaign        TEXT,
    device_type         TEXT,
    consent             TEXT        NOT NULL DEFAULT 'null',
    checkout_clicked    BOOLEAN     NOT NULL DEFAULT FALSE,
    login_completed     BOOLEAN     NOT NULL DEFAULT FALSE,
    converted_to_member BOOLEAN     NOT NULL DEFAULT FALSE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_vs_vsid       ON visitor_sessions (visitor_session_id);
CREATE INDEX        IF NOT EXISTS idx_vs_first_seen ON visitor_sessions (first_seen DESC);
CREATE INDEX        IF NOT EXISTS idx_vs_last_seen  ON visitor_sessions (last_seen  DESC);
CREATE INDEX        IF NOT EXISTS idx_vs_member_id  ON visitor_sessions (member_id);
CREATE INDEX        IF NOT EXISTS idx_vs_utm_src    ON visitor_sessions (utm_source);

-- Migration guards: safe to run against an existing table (idempotent).
ALTER TABLE visitor_sessions ADD COLUMN IF NOT EXISTS heartbeat_count INTEGER NOT NULL DEFAULT 1;
ALTER TABLE visitor_sessions ADD COLUMN IF NOT EXISTS consent         TEXT    NOT NULL DEFAULT 'null';
