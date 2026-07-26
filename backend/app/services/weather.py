from __future__ import annotations

import logging
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

OPEN_METEO = "https://api.open-meteo.com/v1/forecast"
_CACHE_TTL = 900.0  # 15 minutes

_cache_data: list[dict[str, Any]] = []
_cache_ts: float = 0.0

# Canada + NE USA regional grid — Open-Meteo public, no API key, CC-BY 4.0
_GRID: list[tuple[float, float, str]] = [
    # Quebec
    (45.5017, -73.5673, "Montreal"),
    (46.8139, -71.2080, "QuebecCity"),
    (45.4000, -71.9000, "Sherbrooke"),
    (46.3400, -72.5400, "TroisRivieres"),
    (48.4286, -71.0597, "Chicoutimi"),
    (45.4765, -75.7013, "Gatineau"),
    (50.6834, -65.8658, "SeptIles"),
    (48.2500, -79.0000, "RouynNoranda"),
    (48.1000, -77.7800, "ValDor"),
    # Ontario
    (43.7001, -79.4163, "Toronto"),
    (45.4215, -75.6919, "Ottawa"),
    (42.3170, -83.0268, "Windsor"),
    (46.4900, -80.9900, "Sudbury"),
    (48.3826, -89.2477, "ThunderBay"),
    (43.2557, -79.8711, "Hamilton"),
    # Atlantic Canada
    (44.6488, -63.5752, "Halifax"),
    (46.0878, -64.7782, "Moncton"),
    (47.5606, -52.7126, "StJohns"),
    (46.2382, -63.1311, "Charlottetown"),
    (46.1353, -60.1932, "Sydney_NS"),
    # Western Canada
    (49.2827, -123.1207, "Vancouver"),
    (51.0447, -114.0719, "Calgary"),
    (53.5444, -113.4909, "Edmonton"),
    (49.8954, -97.1385, "Winnipeg"),
    (52.1332, -106.6700, "Saskatoon"),
    (50.4501, -104.6178, "Regina"),
    (60.7212, -135.0568, "Whitehorse"),
    (62.4540, -114.3718, "Yellowknife"),
    # NE USA
    (42.3601, -71.0589, "Boston"),
    (40.7128, -74.0060, "NewYork"),
    (42.6526, -73.7562, "Albany"),
    (42.8864, -78.8784, "Buffalo"),
    (44.4759, -73.2121, "Burlington_VT"),
    (43.6591, -70.2568, "Portland_ME"),
    (41.8240, -71.4128, "Providence"),
    (39.9526, -75.1652, "Philadelphia"),
    (38.9072, -77.0369, "Washington"),
    (39.2904, -76.6122, "Baltimore"),
    (40.4406, -79.9959, "Pittsburgh"),
    (43.0481, -76.1474, "Syracuse"),
]

_CURRENT_PARAMS = "temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code,precipitation"
_BATCH_SIZE = 20


async def _fetch_batch(client: httpx.AsyncClient, batch: list[tuple[float, float, str]]) -> list[dict[str, Any]]:
    lats = ",".join(str(p[0]) for p in batch)
    lons = ",".join(str(p[1]) for p in batch)
    url = (
        f"{OPEN_METEO}?latitude={lats}&longitude={lons}"
        f"&current={_CURRENT_PARAMS}&timezone=UTC"
    )
    r = await client.get(url)
    r.raise_for_status()
    payload = r.json()

    # Single location → dict; multiple locations → list
    results = payload if isinstance(payload, list) else [payload]
    out: list[dict[str, Any]] = []
    for i, d in enumerate(results):
        if i >= len(batch):
            break
        lat, lon, label = batch[i]
        c = d.get("current") or {}
        out.append(
            {
                "id": f"wx_{label}",
                "label": label,
                "lat": float(lat),
                "lon": float(lon),
                "temperature_c": c.get("temperature_2m"),
                "humidity_pct": c.get("relative_humidity_2m"),
                "wind_mps": c.get("wind_speed_10m"),
                "precipitation_mm": c.get("precipitation"),
                "code": c.get("weather_code"),
                "ingest_type": "weather",
            }
        )
    return out


async def fetch_global_weather_grid() -> list[dict[str, Any]]:
    global _cache_data, _cache_ts
    if _cache_data and (time.monotonic() - _cache_ts) < _CACHE_TTL:
        return _cache_data

    out: list[dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=30.0) as client:
        for i in range(0, len(_GRID), _BATCH_SIZE):
            batch = _GRID[i : i + _BATCH_SIZE]
            try:
                out.extend(await _fetch_batch(client, batch))
            except Exception:
                logger.exception("weather batch %d failed", i // _BATCH_SIZE)

    if out:
        _cache_data = out
        _cache_ts = time.monotonic()
        logger.info("weather grid: cached %d stations", len(out))
    return out
