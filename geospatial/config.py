from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


def _parse_bbox(raw: str) -> tuple[float, float, float, float]:
    """Return (lat_min, lon_min, lat_max, lon_max)."""
    try:
        values = tuple(float(part.strip()) for part in raw.split(","))
        if len(values) != 4:
            raise ValueError
        lat_min, lon_min, lat_max, lon_max = values
        if not (-90 <= lat_min <= lat_max <= 90 and -180 <= lon_min <= lon_max <= 180):
            raise ValueError
        return lat_min, lon_min, lat_max, lon_max
    except ValueError:
        return 15.0, -170.0, 75.0, -50.0


def _parse_ais_boxes(raw: str) -> list[list[list[float]]]:
    try:
        value: Any = json.loads(raw)
        if not isinstance(value, list) or not value:
            raise ValueError
        return value
    except (json.JSONDecodeError, ValueError, TypeError):
        return [[[40.0, -82.0], [63.0, -52.0]]]


@dataclass(frozen=True)
class Settings:
    host: str
    port: int
    public_base_url: str
    opensky_username: str | None
    opensky_password: str | None
    opensky_bbox: tuple[float, float, float, float]
    opensky_cache_seconds: int
    celestrak_groups: tuple[str, ...]
    satellite_cache_seconds: int
    satellite_limit: int
    ais_api_key: str | None
    ais_bounding_boxes: list[list[list[float]]]
    ais_stale_seconds: int
    camera_config_path: Path
    camera_catalog_url: str | None
    camera_refresh_seconds: int
    websocket_interval_seconds: float
    cesium_ion_token: str | None


def load_settings() -> Settings:
    root = Path(__file__).resolve().parent
    groups = tuple(
        item.strip().upper()
        for item in os.getenv(
            "GEOSPHERE_CELESTRAK_GROUPS",
            "STATIONS,BRIGHTEST,GPS-OPS,WEATHER",
        ).split(",")
        if item.strip()
    )
    return Settings(
        host=os.getenv("GEOSPHERE_HOST", "0.0.0.0"),
        port=_env_int("PORT", _env_int("GEOSPHERE_PORT", 8088)),
        public_base_url=os.getenv("GEOSPHERE_PUBLIC_BASE_URL", "").rstrip("/"),
        opensky_username=os.getenv("OPENSKY_USERNAME") or None,
        opensky_password=os.getenv("OPENSKY_PASSWORD") or None,
        opensky_bbox=_parse_bbox(
            os.getenv("GEOSPHERE_OPENSKY_BBOX", "15,-170,75,-50")
        ),
        opensky_cache_seconds=max(10, _env_int("GEOSPHERE_OPENSKY_CACHE_SECONDS", 20)),
        celestrak_groups=groups or ("STATIONS",),
        satellite_cache_seconds=max(
            300, _env_int("GEOSPHERE_SATELLITE_CACHE_SECONDS", 1800)
        ),
        satellite_limit=max(1, _env_int("GEOSPHERE_SATELLITE_LIMIT", 500)),
        ais_api_key=os.getenv("AISSTREAM_API_KEY") or None,
        ais_bounding_boxes=_parse_ais_boxes(
            os.getenv(
                "GEOSPHERE_AIS_BOUNDING_BOXES_JSON",
                '[[[40,-82],[63,-52]]]',
            )
        ),
        ais_stale_seconds=max(60, _env_int("GEOSPHERE_AIS_STALE_SECONDS", 900)),
        camera_config_path=Path(
            os.getenv(
                "GEOSPHERE_CAMERAS_CONFIG",
                str(root / "cameras.json"),
            )
        ),
        camera_catalog_url=os.getenv("GEOSPHERE_CAMERA_CATALOG_URL") or None,
        camera_refresh_seconds=max(
            15, _env_int("GEOSPHERE_CAMERA_REFRESH_SECONDS", 60)
        ),
        websocket_interval_seconds=max(
            2.0, _env_float("GEOSPHERE_WEBSOCKET_INTERVAL_SECONDS", 5.0)
        ),
        cesium_ion_token=os.getenv("CESIUM_ION_TOKEN") or None,
    )


settings = load_settings()
