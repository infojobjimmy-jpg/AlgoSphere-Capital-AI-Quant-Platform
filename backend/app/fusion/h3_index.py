from __future__ import annotations

from typing import Any

import h3


def cell(lat: float, lon: float, resolution: int) -> str:
    return h3.latlng_to_cell(lat, lon, resolution)


def counts_by_cell(points: list[dict[str, Any]], resolution: int) -> dict[str, int]:
    out: dict[str, int] = {}
    for p in points:
        lat = float(p.get("lat", 0.0))
        lon = float(p.get("lon", 0.0))
        if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
            continue
        c = cell(lat, lon, resolution)
        out[c] = out.get(c, 0) + 1
    return out


def disk_neighbors(center: str, k: int = 1) -> set[str]:
    try:
        return set(h3.grid_disk(center, k))
    except Exception:
        return {center}
