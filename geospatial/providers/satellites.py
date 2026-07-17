from __future__ import annotations

import asyncio
import math
import time
from datetime import datetime, timezone

import httpx
from sgp4.api import Satrec
from sgp4.functions import jday

from ..config import Settings
from ..models import FeedSnapshot, LiveObject


class SatelliteProvider:
    URL = "https://celestrak.org/NORAD/elements/gp.php"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._lock = asyncio.Lock()
        self._loaded_at = 0.0
        self._satellites: list[tuple[str, str, Satrec]] = []
        self._message = "not loaded"

    async def _refresh(self) -> None:
        if time.monotonic() - self._loaded_at < self.settings.satellite_cache_seconds:
            return
        async with self._lock:
            if time.monotonic() - self._loaded_at < self.settings.satellite_cache_seconds:
                return
            satellites: list[tuple[str, str, Satrec]] = []
            errors: list[str] = []
            async with httpx.AsyncClient(timeout=30.0) as client:
                for group in self.settings.celestrak_groups:
                    try:
                        response = await client.get(self.URL, params={"GROUP": group, "FORMAT": "JSON"})
                        response.raise_for_status()
                        for row in response.json():
                            line1 = row.get("TLE_LINE1")
                            line2 = row.get("TLE_LINE2")
                            if not line1 or not line2:
                                continue
                            sat = Satrec.twoline2rv(line1, line2)
                            satellites.append((str(row.get("NORAD_CAT_ID") or sat.satnum), str(row.get("OBJECT_NAME") or sat.satnum), sat))
                            if len(satellites) >= self.settings.satellite_limit:
                                break
                    except Exception as exc:
                        errors.append(f"{group}: {exc}")
                    if len(satellites) >= self.settings.satellite_limit:
                        break
            if satellites:
                self._satellites = satellites
                self._message = f"loaded={len(satellites)}"
            elif errors:
                self._message = "; ".join(errors)
            self._loaded_at = time.monotonic()

    async def snapshot(self) -> FeedSnapshot:
        try:
            await self._refresh()
            now = datetime.now(timezone.utc)
            jd, fraction = jday(now.year, now.month, now.day, now.hour, now.minute, now.second + now.microsecond / 1e6)
            objects: list[LiveObject] = []
            for catalog_id, name, sat in self._satellites:
                error, position, velocity = sat.sgp4(jd, fraction)
                if error:
                    continue
                latitude, longitude, altitude_km = _to_geodetic(position, now)
                speed = math.sqrt(sum(value * value for value in velocity))
                objects.append(LiveObject(
                    id=catalog_id,
                    kind="satellite",
                    latitude=latitude,
                    longitude=longitude,
                    altitude_m=max(0.0, altitude_km * 1000.0),
                    speed=speed,
                    label=name,
                    metadata={"norad_id": catalog_id, "speed_unit": "km/s"},
                ))
            return FeedSnapshot(
                feed="satellites",
                status="live" if objects else "error",
                objects=objects,
                source="CelesTrak + SGP4",
                message=self._message,
                stale=not bool(objects),
            )
        except Exception as exc:
            return FeedSnapshot("satellites", "error", [], source="CelesTrak + SGP4", message=str(exc))


def _gmst(moment: datetime) -> float:
    year, month = moment.year, moment.month
    day = moment.day + (moment.hour + moment.minute / 60 + moment.second / 3600) / 24
    if month <= 2:
        year -= 1
        month += 12
    a = math.floor(year / 100)
    b = 2 - a + math.floor(a / 4)
    jd = math.floor(365.25 * (year + 4716)) + math.floor(30.6001 * (month + 1)) + day + b - 1524.5
    t = (jd - 2451545.0) / 36525.0
    return math.radians((280.46061837 + 360.98564736629 * (jd - 2451545.0) + 0.000387933 * t * t) % 360)


def _to_geodetic(position: tuple[float, float, float], moment: datetime) -> tuple[float, float, float]:
    x0, y0, z = position
    theta = _gmst(moment)
    x = x0 * math.cos(theta) + y0 * math.sin(theta)
    y = -x0 * math.sin(theta) + y0 * math.cos(theta)
    lon = math.atan2(y, x)
    radius = math.hypot(x, y)
    semi_major = 6378.137
    eccentricity_sq = 6.69437999014e-3
    lat = math.atan2(z, radius * (1 - eccentricity_sq))
    altitude = 0.0
    for _ in range(7):
        sin_lat = math.sin(lat)
        normal = semi_major / math.sqrt(1 - eccentricity_sq * sin_lat * sin_lat)
        altitude = radius / max(math.cos(lat), 1e-12) - normal
        lat = math.atan2(z, radius * (1 - eccentricity_sq * normal / (normal + altitude)))
    return math.degrees(lat), ((math.degrees(lon) + 180) % 360) - 180, altitude
