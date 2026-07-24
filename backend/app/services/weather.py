from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

OPEN_METEO = "https://api.open-meteo.com/v1/forecast"


async def fetch_global_weather_grid() -> list[dict[str, Any]]:
    """
    Sample a sparse global grid using Open-Meteo (public, no API key).
    """
    points = [
        (51.5, -0.12, "London"),
        (40.7, -74.0, "NYC"),
        (35.7, 139.7, "Tokyo"),
        (-33.9, 151.2, "Sydney"),
        (48.9, 2.3, "Paris"),
        (25.2, 55.3, "Dubai"),
        (-23.5, -46.6, "SaoPaulo"),
        (19.4, -99.1, "MexicoCity"),
        (55.8, 37.6, "Moscow"),
        (28.6, 77.2, "Delhi"),
    ]
    out: list[dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=30.0) as client:
        for lat, lon, label in points:
            url = (
                f"{OPEN_METEO}?latitude={lat}&longitude={lon}"
                "&current=temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code"
                "&timezone=UTC"
            )
            r = await client.get(url)
            r.raise_for_status()
            d = r.json()
            c = d.get("current", {})
            out.append(
                {
                    "id": f"wx_{label}",
                    "label": label,
                    "lat": float(lat),
                    "lon": float(lon),
                    "temperature_c": c.get("temperature_2m"),
                    "humidity_pct": c.get("relative_humidity_2m"),
                    "wind_mps": c.get("wind_speed_10m"),
                    "code": c.get("weather_code"),
                }
            )
    return out
