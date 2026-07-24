from __future__ import annotations

import json
import logging

from app.config import settings

logger = logging.getLogger(__name__)

DEFAULTS: dict[str, float] = {
    "air_density_z": 3.2,
    "maritime_z": 3.0,
    "air_density_min_count": 35.0,
    "maritime_min_count": 60.0,
    "iso_contamination": 0.06,
    "drift_residual_scale": 0.35,
    "iso_warn": 0.25,
    "iso_critical": 0.45,
}


async def load_thresholds(r) -> dict[str, float]:
    try:
        raw = await r.get(settings.redis_learn_thresholds_key())
        if not raw:
            return dict(DEFAULTS)
        data = json.loads(raw)
        out = dict(DEFAULTS)
        for k, v in data.items():
            try:
                out[str(k)] = float(v)
            except (TypeError, ValueError):
                continue
        return out
    except Exception:
        logger.exception("load_thresholds failed")
        return dict(DEFAULTS)


async def save_thresholds(r, th: dict[str, float]) -> None:
    merged = dict(DEFAULTS)
    merged.update(th)
    await r.set(settings.redis_learn_thresholds_key(), json.dumps(merged))


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


async def adapt_from_cycle(r, snap: dict[str, Any], prev_thresholds: dict[str, float]) -> dict[str, float]:
    """
    Reinforcement-style nudge: if the system is firing too many critical decisions,
    slightly relax detection; if almost none, tighten slightly.
    """
    th = dict(prev_thresholds)
    decisions = snap.get("decisions") or []
    events = snap.get("events") or []
    crit = sum(1 for d in decisions if str(d.get("severity")) == "critical")
    high = sum(1 for d in decisions if str(d.get("severity")) == "high")
    n = max(1, len(decisions))

    pressure = (crit * 2 + high) / n
    if pressure > 0.55 and len(decisions) >= 4:
        th["air_density_z"] = clamp(th.get("air_density_z", 3.2) + 0.06, 2.4, 4.5)
        th["iso_contamination"] = clamp(th.get("iso_contamination", 0.06) + 0.01, 0.03, 0.12)
    elif pressure < 0.12 and len(events) >= 6:
        th["air_density_z"] = clamp(th.get("air_density_z", 3.2) - 0.04, 2.4, 4.5)
        th["iso_contamination"] = clamp(th.get("iso_contamination", 0.06) - 0.005, 0.03, 0.12)

    await save_thresholds(r, th)
    return th
