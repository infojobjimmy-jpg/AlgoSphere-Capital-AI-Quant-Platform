from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class CorrelationHit:
    score: float
    title: str
    details: str
    payload: dict[str, Any]


def correlate_aircraft_weather(
    aircraft: list[dict[str, Any]], weather: list[dict[str, Any]]
) -> list[CorrelationHit]:
    """
    Lightweight spatial join: high wind cells with dense air traffic.
    """
    hits: list[CorrelationHit] = []
    for w in weather:
        wlat = float(w.get("lat", 0.0))
        wlon = float(w.get("lon", 0.0))
        wind = float(w.get("wind_mps") or 0.0)
        if wind < 12:
            continue
        nearby = 0
        for a in aircraft:
            alat = float(a.get("lat", 0.0))
            alon = float(a.get("lon", 0.0))
            if abs(alat - wlat) < 2.0 and abs(alon - wlon) < 2.0:
                nearby += 1
        if nearby >= 40:
            score = min(1.0, (wind / 25.0) * (nearby / 200.0))
            hits.append(
                CorrelationHit(
                    score=float(score),
                    title="Wind–traffic coupling",
                    details=f"{nearby} aircraft within 2° of {w.get('label')} with {wind:.1f} m/s winds.",
                    payload={
                        "weather_id": w.get("id"),
                        "nearby_aircraft": nearby,
                        "wind_mps": wind,
                    },
                )
            )
    hits.sort(key=lambda h: h.score, reverse=True)
    return hits[:12]
