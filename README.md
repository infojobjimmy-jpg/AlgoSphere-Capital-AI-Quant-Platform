# Algosphere Capital

**Unified geospatial intelligence platform** — a live Cesium globe for aircraft,
satellites, weather, licensed AIS vessels and approved webcams. The public
deployment is geospatial-only and contains no trading terminal.

## Quick start (local)

```bash
docker compose up --build
```

- **Frontend**: http://localhost:5173  
- **API**: http://localhost:8080  
- **API docs**: http://localhost:8080/docs  
- **Grafana** (optional): http://localhost:3001 (admin/admin)

> First boot provisions TimescaleDB from `infra/sql/init.sql` plus `infra/sql/algosphere_trading.sql`. Use a **fresh** Docker volume if you previously ran another stack on the same DB volume.

## Architecture (summary)

| Layer | Role |
|--------|------|
| **Ingestion** | Kafka `acap.telemetry` — aircraft, satellites, weather, ships, cameras, **market_crypto / market_forex / market_equities** |
| **Bridge + Cortex** | Single `run_kafka_intel_tick` — goals → fusion + agents → plan/execute/critic → memory → **trading hook** |
| **Meta-learning / Evolution** | Offline evolution; Redis guidance `acap:meta:evolution:guidance` |
| **Trading** | Signal derivation from snapshot + **RiskManager** (≤1% equity/trade, stop, drawdown) + **paper orders** + Postgres audit |

## Configuration

Copy `backend/.env.example` → `backend/.env`. Important keys:

- `TWELVE_DATA_API_KEY` / `FINNHUB_API_KEY` — optional; forex/equity ticks are skipped if empty. Crypto uses **CoinGecko public** simple price (rate limits apply).
- `TRADING_MODE` (`paper|live`) and `TRADING_LIVE_BROKER` (`mt5|ctrader`) control execution mode.
- `TRADING_KILL_SWITCH` — runtime toggled via `POST /api/trading/kill-switch?on=true`; mode override via `POST /api/trading/mode?mode=live`.
- `MT5_BRIDGE_URL` / `CTRADER_BASE_URL` (+ tokens) configure live adapters.
- `SELF_CODE_ENABLED` + `SELF_CODE_*` command settings enable diff-only sandbox benchmark/deployment gate.

## API surface (selected)

| Method | Path | Description |
|--------|------|--------------|
| GET | `/health` | Liveness |
| GET | `/layers/snapshot` | Latest fused JSON from Redis |
| WS | `/ws/live` | Live snapshot stream |
| GET | `/trading/portfolio` | Paper + live account snapshot + positions + kill/mode |
| GET | `/trading/signals` | Recent `trading_signals` rows |
| GET | `/trading/orders` | Paper orders |
| POST | `/trading/kill-switch?on=` | Redis-backed kill switch |
| POST | `/trading/mode?mode=paper|live` | Runtime execution mode override |
| GET | `/self-code/status` | Self-code flags |
| POST | `/self-code/run` | Sandbox + benchmark + deploy gate |

Full OpenAPI: `/docs`.

## Production notes

- Keep `PUBLIC_GEOSPATIAL_MODE=true` for the public globe. This disables market
  ingestion, the trading hook, and trading/self-code routes.
- See `docs/PRODUCTION_READINESS.md` for AIS, webcam, HTTPS and deployment setup.
- Live mode runs through explicit broker adapters and stays feature-flagged; keep API tokens in secrets and enable only in approved environments.
- **Secrets**: inject via Docker/Kubernetes secrets, not committed `.env`.
- **Scale**: horizontal Kafka consumers; split `bridge` / `ingestion` replicas; managed Redis/Postgres/Timescale.

## Repository layout

- `backend/` — FastAPI application (`app/`)
- `frontend/` — Vite + React + Cesium
- `infra/` — SQL, Prometheus, K8s samples (`infra/k8s/` from upstream template)

## Legal

Use only data sources and APIs you are licensed and permitted to call. Trading involves risk; this software is not financial advice.
