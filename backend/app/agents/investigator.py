from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class Investigation:
    confidence: float
    title: str
    narrative: str
    evidence: dict[str, Any]


def run_investigator(ctx: dict[str, Any]) -> list[Investigation]:
    """
    Deep anomaly analysis: combine fusion events, correlations, and density context.
    """
    inv: list[Investigation] = []
    events = ctx.get("events") or []
    corr = ctx.get("correlations") or []
    alerts = ctx.get("alerts") or []

    for e in events[:6]:
        if str(e.get("severity")) not in ("high", "medium"):
            continue
        et = str(e.get("event_type", ""))
        h3 = str(e.get("h3_cell", ""))
        inv.append(
            Investigation(
                confidence=0.62 if e.get("severity") == "high" else 0.48,
                title=f"Fusion trace: {et}",
                narrative=f"Spatially anchored anomaly at {h3}. Cross-check adjacent layers and replay timeline.",
                evidence={"event": e},
            )
        )

    for c in corr[:3]:
        inv.append(
            Investigation(
                confidence=float(c.get("score", 0.0)),
                title=str(c.get("title", "Correlation")),
                narrative=str(c.get("details", "")),
                evidence={"correlation": c},
            )
        )

    for a in alerts[:2]:
        inv.append(
            Investigation(
                confidence=0.55 if a.get("severity") == "critical" else 0.42,
                title=str(a.get("title", "Alert")),
                narrative=str(a.get("body", "")),
                evidence={"alert": a},
            )
        )

    inv.sort(key=lambda x: x.confidence, reverse=True)
    return inv[:12]
