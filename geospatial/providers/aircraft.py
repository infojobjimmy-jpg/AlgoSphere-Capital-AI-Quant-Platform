from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any

import httpx

from ..config import Settings
from ..models import FeedSnapshot, LiveObject


class AircraftProvider:
    URL = "https://opensky-network.org/api/states/all"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._lock = asyncio.Lock()
        self._cached_at = 0.0
        self._cached = FeedSnapshot("aircraft", "starting", [], source="OpenSky")

    async def snapshot(self) -> FeedSnapshot:
        if time.monotonic() - self._cached_at < self.settings.opensky_cache_seconds:
            return self._cached
        async with self._lock:
            if time.monotonic() - self._cached_at < self.settings.opensky_cache_seconds:
                return self._cached
            lat_min, lon_min, lat_max, lon_max = self.settings.opensky_bbox
            params: dict[str, Any] = {
                "lamin": lat_min,
                "lomin": lon_min,
                "lamax": lat_max,
                "lomax": lon_max,
            }
            auth = None
            if self.settings.opensky_username and self.settings.opensky_password:
                auth = (self.settings.opensky_username, self.settings.opensky_password)
            try:
                async with httpx.AsyncClient(timeout=20.0) as client:
                    response = await client.get(self.URL, params=params, auth=auth)
                    response.raise_for_status()
                    payload = response.json()
                objects: list[LiveObject] = []
                for state in payload.get("states") or []:
                    if len(state) < 14 or state[5] is None or state[6] is None:
                        continue
                    icao24 = str(state[0] or "unknown")
                    callsign = str(state[1] or icao24).strip() or icao24
                    altitude = state[7] if state[7] is not None else state[13]
                    objects.append(
                        LiveObject(
                            id=icao24,
                            kind="aircraft",
                            latitude=float(state[6]),
                            longitude=float(state[5]),
                            altitude_m=float(altitude or 0.0),
                            heading_deg=float(state[10]) if state[10] is not None else None,
                            speed=float(state[9]) if state[9] is not None else None,
                            label=callsign,
                            updated_at=datetime.fromtimestamp(
                                int(state[4] or payload.get("time") or time.time()),
                                tz=timezone.utc,
                            ).isoformat(),
                            metadata={
                                "country": state[2],
                                "on_ground": bool(state[8]),
                                "vertical_rate_mps": state[11],
                            },
                        )
                    )
                self._cached = FeedSnapshot(
                    feed="aircraft",
                    status="live",
                    objects=objects,
                    source="OpenSky Network",
                    message=f"bbox={self.settings.opensky_bbox}",
                )
            except Exception as exc:
                stale = bool(self._cached.objects)
                self._cached = FeedSnapshot(
                    feed="aircraft",
                    status="degraded" if stale else "error",
                    objects=self._cached.objects if stale else [],
                    source="OpenSky Network",
                    message=str(exc),
                    stale=stale,
                )
            self._cached_at = time.monotonic()
            return self._cached
