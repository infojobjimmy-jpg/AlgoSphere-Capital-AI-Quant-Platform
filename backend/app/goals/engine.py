from __future__ import annotations

import re
from typing import Any


def compute_goal_bias(goals: list[dict[str, Any]]) -> dict[str, float]:
    """
    Map active goals into numeric threshold deltas (added to base thresholds in Redis).
    Negative air_density_z delta => more sensitive (lower z required).
    """
    bias: dict[str, float] = {
        "air_density_z_delta": 0.0,
        "iso_contamination_delta": 0.0,
        "severity_lift": 0.0,
    }
    if not goals:
        return bias
    max_pri = max(float(g.get("priority", 1.0)) for g in goals)
    scale = min(1.0, max_pri / 25.0)
    for g in goals:
        desc = str(g.get("description", "")).lower()
        pri = float(g.get("priority", 5.0)) / 10.0
        if re.search(r"miss|security|aware|critical|never miss", desc):
            bias["air_density_z_delta"] -= 0.12 * pri * scale
            bias["severity_lift"] += 0.04 * pri * scale
        if re.search(r"noise|cost|capacity|reduce|false positive", desc):
            bias["air_density_z_delta"] += 0.1 * pri * scale
            bias["iso_contamination_delta"] += 0.01 * pri * scale
    bias["air_density_z_delta"] = max(-0.35, min(0.35, bias["air_density_z_delta"]))
    bias["iso_contamination_delta"] = max(-0.03, min(0.03, bias["iso_contamination_delta"]))
    bias["severity_lift"] = max(0.0, min(0.15, bias["severity_lift"]))
    return bias


def apply_goal_bias_to_thresholds(base: dict[str, float], bias: dict[str, float]) -> dict[str, float]:
    th = dict(base)
    th["air_density_z"] = float(th.get("air_density_z", 3.2)) + float(bias.get("air_density_z_delta", 0.0))
    th["maritime_z"] = float(th.get("maritime_z", 3.0)) + 0.5 * float(bias.get("air_density_z_delta", 0.0))
    th["iso_contamination"] = float(th.get("iso_contamination", 0.06)) + float(
        bias.get("iso_contamination_delta", 0.0)
    )
    th["air_density_z"] = max(2.3, min(4.6, th["air_density_z"]))
    th["maritime_z"] = max(2.2, min(4.2, th["maritime_z"]))
    th["iso_contamination"] = max(0.03, min(0.12, th["iso_contamination"]))
    return th


def global_priority_score(goals: list[dict[str, Any]], snap: dict[str, Any]) -> float:
    if not goals:
        return 0.35
    gp = sum(float(g.get("priority", 1.0)) for g in goals)
    ev = len(snap.get("events") or [])
    al = len([a for a in (snap.get("alerts") or []) if str(a.get("severity")) in ("critical", "high")])
    stress = min(1.0, 0.18 * ev + 0.09 * al + gp / 80.0)
    return float(min(1.0, max(0.05, stress)))


def attention_weights(goals: list[dict[str, Any]]) -> dict[str, float]:
    w = {"aircraft": 1.0, "ships": 0.85, "weather": 0.65, "cameras": 0.45, "satellites": 0.5}
    blob = " ".join(str(g.get("description", "")).lower() for g in goals)
    if any(k in blob for k in ("maritime", "ship", "ais", "sea")):
        w["ships"] += 0.35
    if any(k in blob for k in ("weather", "wind", "storm")):
        w["weather"] += 0.3
    if any(k in blob for k in ("camera", "visual", "traffic cam")):
        w["cameras"] += 0.25
    if any(k in blob for k in ("air", "ads-b", "aircraft", "sky")):
        w["aircraft"] += 0.3
    s = sum(w.values()) or 1.0
    return {k: v / s for k, v in w.items()}
