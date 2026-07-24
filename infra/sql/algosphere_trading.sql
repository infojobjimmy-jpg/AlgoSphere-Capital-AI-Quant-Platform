-- Algosphere Capital: trading + self-code audit (applied after base GAIOS init)

CREATE TABLE IF NOT EXISTS market_ticks (
    id BIGSERIAL PRIMARY KEY,
    time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    asset_class TEXT NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS market_ticks_sym_time_idx ON market_ticks (symbol, time DESC);

CREATE TABLE IF NOT EXISTS trading_signals (
    id BIGSERIAL PRIMARY KEY,
    time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    strength DOUBLE PRECISION NOT NULL,
    rationale TEXT,
    meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS trading_signals_time_idx ON trading_signals (time DESC);

CREATE TABLE IF NOT EXISTS paper_orders (
    id BIGSERIAL PRIMARY KEY,
    time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    qty DOUBLE PRECISION NOT NULL,
    entry_price DOUBLE PRECISION NOT NULL,
    stop_price DOUBLE PRECISION NOT NULL,
    notional_usd DOUBLE PRECISION NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS paper_orders_time_idx ON paper_orders (time DESC);

CREATE TABLE IF NOT EXISTS live_orders (
    id BIGSERIAL PRIMARY KEY,
    time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    broker TEXT NOT NULL,
    broker_order_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    qty DOUBLE PRECISION NOT NULL,
    entry_price DOUBLE PRECISION NOT NULL,
    stop_price DOUBLE PRECISION NOT NULL,
    notional_usd DOUBLE PRECISION NOT NULL,
    status TEXT NOT NULL DEFAULT 'submitted',
    meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS live_orders_time_idx ON live_orders (time DESC);

CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    id BIGSERIAL PRIMARY KEY,
    time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    equity_usd DOUBLE PRECISION NOT NULL,
    cash_usd DOUBLE PRECISION NOT NULL,
    realized_pnl_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS self_code_runs (
    id BIGSERIAL PRIMARY KEY,
    time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    proposal_summary TEXT NOT NULL,
    benchmark_delta DOUBLE PRECISION,
    approved BOOLEAN NOT NULL DEFAULT FALSE,
    dry_run BOOLEAN NOT NULL DEFAULT TRUE,
    meta JSONB NOT NULL DEFAULT '{}'::jsonb
);
