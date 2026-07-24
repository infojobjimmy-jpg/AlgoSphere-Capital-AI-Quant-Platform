# HTTP API — Algosphere Capital

Base URL: `http://localhost:8080` (compose) or your ingress URL.

## Health

- `GET /health` → `{ "status": "ok", "service": "algosphere-capital-api" }`

## Live state

- `GET /layers/snapshot` — last fused snapshot JSON (same payload as WS, without streaming).
- `WebSocket /ws/live` — binary gzip optional (`hello` + `compress: true`).

## Intelligence

- `GET /intel/timeline?limit=` — Redis timeline frames.
- `GET /goals` — active goals; `GET /goals?status=all` — broader listing.
- `GET /goals/plans/summary` — plan / execution summary.

## Trading

- `GET /trading/portfolio` — mode, paper state, live account, live positions, kill switch, recent order cache.
- `GET /trading/signals` — persisted signal rows.
- `GET /trading/orders` — orders from `paper_orders` or `live_orders` based on current mode.
- `POST /trading/kill-switch?on=true|false` — emergency stop (Redis `acap:trading:kill`).
- `POST /trading/mode?mode=paper|live` — runtime mode override in Redis.

## Self-code pipeline

- `GET /self-code/status`
- `POST /self-code/run` — executes: `sandbox_runner` → `benchmark_engine` → `deployment_gate`.

OpenAPI **/docs** is the authoritative schema.
