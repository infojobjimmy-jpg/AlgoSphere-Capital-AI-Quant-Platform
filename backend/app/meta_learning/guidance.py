from __future__ import annotations

import json
import logging
from typing import Any

from app.config import settings
from app.learning.adaptive_thresholds import clamp

logger = logging.getLogger(__name__)

# Small deltas only; merged before goal bias in Cortex.
_DELTA_KEYS = (
    "air_density_z",
    "maritime_z",
    "iso_contamination",
    "iso_warn",
    "iso_critical",
    "drift_residual_scale",
)


async def load_guidance(r: Any) -> dict[str, Any]:
    try:
        raw = await r.get(settings.redis_evolution_guidance_key())
        if not raw:
            return {}
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        logger.exception("meta_learning: load_guidance failed")
        return {}


async def save_guidance(r: Any, payload: dict[str, Any]) -> None:
    try:
        await r.set(settings.redis_evolution_guidance_key(), json.dumps(payload, separators=(",", ":")))
    except Exception:
        logger.exception("meta_learning: save_guidance failed")


def merge_evolution_into_thresholds(th: dict[str, float], guidance: dict[str, Any] | None) -> dict[str, float]:
    """Apply evolution threshold_deltas onto adaptive thresholds (same keys as learning loop)."""
    if not guidance:
        return th
    out = dict(th)
    td = guidance.get("threshold_deltas")
    if not isinstance(td, dict):
        return out
    for k in _DELTA_KEYS:
        if k not in td:
            continue
        try:
            dv = float(td[k])
        except (TypeError, ValueError):
            continue
        base = float(out.get(k, 0.0))
        if k == "air_density_z":
            out[k] = clamp(base + dv, 2.3, 4.6)
        elif k == "maritime_z":
            out[k] = clamp(base + dv, 2.2, 4.2)
        elif k == "iso_contamination":
            out[k] = clamp(base + dv, 0.03, 0.12)
        elif k in ("iso_warn", "iso_critical"):
            out[k] = clamp(base + dv, 0.08, 0.65)
        elif k == "drift_residual_scale":
            out[k] = clamp(base + dv, 0.12, 0.75)
    return out


def severity_lift_delta(guidance: dict[str, Any] | None) -> float:
    if not guidance:
        return 0.0
    try:
        return clamp(float(guidance.get("severity_lift_delta", 0.0)), -0.08, 0.12)
    except (TypeError, ValueError):
        return 0.0


def planner_profile_choice(guidance: dict[str, Any] | None) -> str | None:
    if not guidance:
        return None
    v = guidance.get("planner_profile")
    if v in ("template", "openai"):
        return str(v)
    return None


def compact_guidance_payload(g: dict[str, Any] | None) -> dict[str, Any]:
    if not g:
        return {}
    keys = ("threshold_deltas", "severity_lift_delta", "planner_profile", "decision_confidence_boost", "version")
    return {k: g[k] for k in keys if k in g and g[k] is not None}
