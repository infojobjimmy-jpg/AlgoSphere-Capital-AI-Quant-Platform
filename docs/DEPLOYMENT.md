# Deployment guide — Algosphere Capital

## Docker Compose (development / single node)

1. Install Docker Desktop (Windows/macOS) or Docker Engine + Compose (Linux).
2. From the repo root: `docker compose up --build`.
3. Wait for `db` (Postgres/Timescale) to be ready; API listens on **8080**, UI on **5173**, optional **edge** gateway on **9080** (reverse proxy to UI + API).

### Services

| Service | Purpose |
|---------|---------|
| `api` | FastAPI + `/docs` |
| `bridge` | Kafka → Redis snapshot + Cortex loop |
| `ingestion` | Public feeds → Kafka |
| `camera-worker` | Curated camera registry |
| `kafka` | Streaming (KRaft; no Zookeeper) |
| `redis` | Snapshots, paper state, adaptive thresholds |
| `db` | Relational + hypertables (TimescaleDB image) |
| `chromadb` | Episodic memory |
| `frontend` | nginx serving Vite build → port 5173 host map |
| `edge` | Optional public nginx gateway → **9080** (TLS: mount certs; see `infra/nginx/edge.conf`) |

### Remote access (edge + TLS)

- Use `edge` as the public entry: map host **80/443** to the container if desired (edit `docker-compose.yml` ports).
- For Let's Encrypt, serve `/.well-known/acme-challenge` (see `infra/nginx/edge.conf`) with Certbot on the host or a companion container, then add an `ssl`-listening `server` block pointing `ssl_certificate` / `ssl_certificate_key` at the issued PEM files.
- Optional HTTP basic auth: create an `htpasswd` file, mount it at `/etc/nginx/.htpasswd`, and uncomment the `auth_basic` lines in `infra/nginx/edge.conf`.
- JWT validation at the edge is not enabled by default; use an OAuth2 proxy or extend FastAPI auth and `auth_request` as needed.

### Database initialization

- `01-init.sql` — base schema (upstream GAIOS-compatible tables).
- `02-trading.sql` — market + paper/live trading + self-code audit tables.

If Postgres volume already exists **without** trading tables, run manually:

```bash
docker compose exec db psql -U gaios -d gaios -f /docker-entrypoint-initdb.d/02-trading.sql
```

(or copy `infra/sql/algosphere_trading.sql` contents into `psql`).

## Kubernetes (outline)

- Build and push images for `backend` and `frontend` to your registry.
- Replace placeholder image names in `infra/k8s/` manifests.
- Provide `Secret` for `DATABASE_URL`, API keys, and Kafka bootstrap.
- Run **one** `bridge` Deployment per Kafka consumer group; scale `ingestion` with partitions.

## Live trading mode

- Set `TRADING_MODE=live` and `TRADING_LIVE_BROKER=mt5` or `ctrader`.
- MT5 adapter uses `MT5_BRIDGE_URL` + optional `MT5_API_TOKEN`.
- cTrader adapter uses `CTRADER_BASE_URL` + `CTRADER_ACCESS_TOKEN` (+ optional `CTRADER_ACCOUNT_ID`).
- Keep `TRADING_KILL_SWITCH=true` by default during first bring-up, then enable via API when validated.

## Self-coding pipeline

- Enable with `SELF_CODE_ENABLED=true`.
- Pipeline endpoint: `POST /self-code/run` with diff-only proposal text.
- Commands run in isolated sandbox copy of `SELF_CODE_WORKSPACE_ROOT`.
- Deployment gate only applies changes when benchmark improves and `SELF_CODE_DRY_RUN=false`.

## Observability

- Prometheus: host port **9090** (compose).
- Instrumentation: `/metrics` on API (when enabled via instrumentator).
