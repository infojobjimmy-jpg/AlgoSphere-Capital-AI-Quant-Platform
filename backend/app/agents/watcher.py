from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass
class WatchFinding:
    severity: str
    title: str
    body: str
    payload: dict[str, Any]


def run_watcher(ctx: dict[str, Any]) -> list[WatchFinding]:
    """
    Global monitoring pass: fast structural checks on fused layers.
    """
    out: list[WatchFinding] = []
    layers = ctx.get("layers") or {}
    cams = layers.get("cameras") or []
    degraded = sum(1 for c in cams if str(c.get("health_state", "")).lower() in ("degraded", "down"))
    if degraded and len(cams) and degraded / max(1, len(cams)) > 0.35:
        out.append(
            WatchFinding(
                severity="warning",
                title="Camera fleet health degraded",
                body=f"{degraded}/{len(cams)} curated cameras are degraded or offline.",
                payload={"degraded": degraded, "total": len(cams)},
            )
        )

    ac = len(layers.get("aircraft") or [])
    if ac > 6000:
        out.append(
            WatchFinding(
                severity="info",
                title="Global air traffic saturation",
                body="Aircraft volume is extremely high; rendering and correlation budgets may throttle.",
                payload={"aircraft": ac},
            )
        )

    events = ctx.get("events") or []
    hi = sum(1 for e in events if str(e.get("severity")) == "high")
    if hi >= 4:
        out.append(
            WatchFinding(
                severity="warning",
                title="Elevated fused-event rate",
                body=f"{hi} high-severity fusion events detected in this cycle.",
                payload={"high_events": hi, "ts": datetime.now(timezone.utc).isoformat()},
            )
        )

    return out
