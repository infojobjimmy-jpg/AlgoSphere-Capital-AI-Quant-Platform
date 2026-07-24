from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from app.config import settings
from app.fusion.h3_index import cell, counts_by_cell


@dataclass
class FusionEvent:
    h3_cell: str
    event_type: str
    severity: str
    summary: str
    payload: dict[str, Any]


def _ewma_update(prev: float | None, x: float, alpha: float = 0.15) -> float:
    if prev is None:
        return x
    return alpha * x + (1.0 - alpha) * prev


def detect_events(
    aircraft: list[dict[str, Any]],
    ships: list[dict[str, Any]],
    weather: list[dict[str, Any]],
    ewma_state: dict[str, float],
    res: int | None = None,
    thresholds: dict[str, float] | None = None,
) -> tuple[list[FusionEvent], dict[str, float]]:
    th = thresholds or {}
    air_z = float(th.get("air_density_z", 3.2))
    mar_z = float(th.get("maritime_z", 3.0))
    air_min = int(th.get("air_density_min_count", 35))
    mar_min = int(th.get("maritime_min_count", 60))
    res = res if res is not None else settings.h3_aircraft_resolution
    ac_cells = counts_by_cell(aircraft, res)
    sh_cells = counts_by_cell(ships, max(2, res - 1))
    events: list[FusionEvent] = []
    new_state = dict(ewma_state)

    for h, cnt in ac_cells.items():
        key = f"ac:{h}"
        prev = new_state.get(key)
        sm = _ewma_update(prev, float(cnt))
        new_state[key] = sm
        if prev is None:
            continue
        sigma = max(2.0, math.sqrt(sm + 1.0))
        z = (float(cnt) - prev) / sigma if sigma else 0.0
        if z >= air_z and cnt >= air_min:
            events.append(
                FusionEvent(
                    h3_cell=h,
                    event_type="air_density_spike",
                    severity="high" if z >= air_z + 1.3 else "medium",
                    summary=f"Aircraft density spike in H3 {h} (z={z:.2f}, n={cnt}).",
                    payload={"layer": "aircraft", "count": cnt, "z": z},
                )
            )

    for h, cnt in sh_cells.items():
        key = f"sh:{h}"
        prev = new_state.get(key)
        sm = _ewma_update(prev, float(cnt))
        new_state[key] = sm
        if prev is None:
            continue
        sigma = max(2.0, math.sqrt(sm + 1.0))
        z = (float(cnt) - prev) / sigma if sigma else 0.0
        if z >= mar_z and cnt >= mar_min:
            events.append(
                FusionEvent(
                    h3_cell=h,
                    event_type="maritime_cluster",
                    severity="medium",
                    summary=f"Maritime clustering in H3 {h} (z={z:.2f}, n={cnt}).",
                    payload={"layer": "ships", "count": cnt, "z": z},
                )
            )

    for w in weather:
        wind = float(w.get("wind_mps") or 0.0)
        if wind < 18:
            continue
        lat = float(w.get("lat", 0.0))
        lon = float(w.get("lon", 0.0))
        h = cell(lat, lon, res)
        cnt = ac_cells.get(h, 0)
        if wind >= 20 and cnt >= 45:
            events.append(
                FusionEvent(
                    h3_cell=h,
                    event_type="weather_traffic_coupling",
                    severity="high" if wind >= 24 else "medium",
                    summary=f"High wind ({wind:.1f} m/s) co-located with dense air traffic (n={cnt}).",
                    payload={"wind_mps": wind, "aircraft": cnt, "label": w.get("label")},
                )
            )

    events.sort(key=lambda e: (e.severity == "high", e.payload.get("z", 0)), reverse=True)
    return events[:80], new_state


def events_to_dicts(events: list[FusionEvent]) -> list[dict[str, Any]]:
    ts = datetime.now(timezone.utc)
    ts_iso = ts.isoformat()
    minute_bucket = ts.strftime("%Y%m%d%H%M")
    out: list[dict[str, Any]] = []
    for e in events:
        eid = hashlib.sha256(f"{e.h3_cell}|{e.event_type}|{minute_bucket}".encode()).hexdigest()[:32]
        out.append(
            {
                "time": ts_iso,
                "event_id": eid,
                "h3_cell": e.h3_cell,
                "event_type": e.event_type,
                "severity": e.severity,
                "summary": e.summary,
                "payload": e.payload,
            }
        )
    return out
