# Production readiness

## Public product boundary

`PUBLIC_GEOSPATIAL_MODE=true` is the production default. In this mode:

- the frontend contains no trading terminal, portfolio, signal or market-tick UI;
- trading and self-code routes are not registered;
- market ingestion and the Cortex trading hook are disabled;
- unavailable aircraft, satellite, ship and camera sources publish empty layers;
- simulated objects are never substituted for unavailable live data.

## Required external accounts

1. Create an AISStream API key: https://aisstream.io/
2. Create a Windy Webcams API key: https://api.windy.com/webcams
3. Register a domain: https://www.cloudflare.com/products/registrar/
4. Create a Docker-capable server: https://www.digitalocean.com/products/droplets

AISStream is currently beta and has no SLA. For a commercial SLA, replace the
adapter with a licensed maritime provider before promising guaranteed coverage.

## Required secrets

Create `backend/.env` from `.env.example` and set:

```dotenv
PUBLIC_GEOSPATIAL_MODE=true
API_ADMIN_KEY=<at least 32 random bytes>
AISSTREAM_API_KEY=<server-side key>
WINDY_WEBCAMS_API_KEY=<server-side key>
CORS_ALLOWED_ORIGINS=https://your-domain.example
VITE_CESIUM_TOKEN=<public browser-scoped Cesium token>
```

Never commit `backend/.env`.

## HTTPS deployment

Point the domain's DNS A/AAAA records to the server, open inbound ports 80 and
443, then run:

```bash
export ALGOSPHERE_DOMAIN=globe.example.com
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
```

Caddy obtains and renews the public TLS certificate automatically. Restrict host
ports 5173, 8080, 9080, 9090 and database ports with the server firewall; only
80/443 should be publicly reachable.

## Verification

```bash
python -m pytest backend/tests
npm --prefix frontend run build
docker compose config --quiet
curl -fsS https://globe.example.com/api/health
```

Confirm that `/api/trading/*` and `/api/self-code/*` return 404 in public mode.
