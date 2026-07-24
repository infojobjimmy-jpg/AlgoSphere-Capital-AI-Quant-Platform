from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np


@dataclass
class AnomalySignal:
    severity: str
    title: str
    body: str
    payload: dict[str, Any]


def zscore_tail(values: list[float], x: float) -> float:
    if len(values) < 5:
        return 0.0
    arr = np.array(values[-120:], dtype=float)
    mu = float(arr.mean())
    sigma = float(arr.std(ddof=1)) if arr.size > 1 else 0.0
    if sigma < 1e-6:
        return 0.0
    return (x - mu) / sigma


def detect_count_anomalies(
    layer: str, history: list[float], current: int
) -> list[AnomalySignal]:
    signals: list[AnomalySignal] = []
    z = zscore_tail(history, float(current))
    if z >= 4.0:
        signals.append(
            AnomalySignal(
                severity="critical",
                title=f"{layer} traffic surge",
                body=f"Observed {current} entities (z={z:.2f}) vs recent baseline.",
                payload={"layer": layer, "z": z, "count": current},
            )
        )
    elif z <= -3.5:
        signals.append(
            AnomalySignal(
                severity="warning",
                title=f"{layer} traffic drop",
                body=f"Observed {current} entities (z={z:.2f}) vs recent baseline.",
                payload={"layer": layer, "z": z, "count": current},
            )
        )
    return signals


def cluster_events_by_grid(
    points: list[dict[str, Any]], cell_deg: float = 2.0
) -> list[dict[str, Any]]:
    buckets: dict[tuple[int, int], list[dict[str, Any]]] = {}
    for p in points:
        lat = float(p.get("lat", 0.0))
        lon = float(p.get("lon", 0.0))
        key = (int(lat // cell_deg), int(lon // cell_deg))
        buckets.setdefault(key, []).append(p)
    clusters = []
    for (gy, gx), items in buckets.items():
        if len(items) < 25:
            continue
        lat = (gy + 0.5) * cell_deg
        lon = (gx + 0.5) * cell_deg
        clusters.append(
            {
                "id": f"cl_{gy}_{gx}",
                "lat": lat,
                "lon": lon,
                "count": len(items),
                "intensity": min(1.0, len(items) / 200.0),
            }
        )
    return clusters
