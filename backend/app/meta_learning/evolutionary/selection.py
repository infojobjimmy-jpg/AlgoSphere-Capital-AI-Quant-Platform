"""Select or reject evolved strategy variants from offline competition scores."""

from __future__ import annotations

from typing import Any

from app.config import settings


def pick_winner(scored: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not scored:
        return None
    ranked = sorted(scored, key=lambda x: float(x.get("composite", 0.0)))
    best = ranked[-1]
    if float(best.get("composite", 0.0)) < float(settings.evolutionary_min_composite):
        return None
    vid = str(best.get("variant_id", ""))
    if ":baseline" not in vid and float(best.get("lift", 0.0)) < float(settings.evolutionary_promote_min_lift):
        return None
    return best


def reject_reason(scored: list[dict[str, Any]]) -> str | None:
    if not scored:
        return "no_variants"
    w = pick_winner(scored)
    if w is None:
        return "below_threshold"
    return None
