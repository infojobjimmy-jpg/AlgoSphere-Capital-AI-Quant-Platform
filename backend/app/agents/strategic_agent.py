from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


def _fingerprint(h3: str, event_type: str, minute_bucket: str) -> str:
    raw = f"{h3}|{event_type}|{minute_bucket}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def _norm_severity(s: str) -> str:
    s = (s or "").lower()
    if s in ("critical", "crit"):
        return "critical"
    if s in ("high", "severe"):
        return "high"
    if s in ("medium", "warn", "warning"):
        return "medium"
    return "low"


@dataclass
class StrategicDecision:
    event_id: str
    severity: str
    action: str
    confidence: float
    rationale: str
    sources: list[str]


def _severity_weight(sev: str) -> float:
    s = _norm_severity(sev)
    if s == "critical":
        return 1.0
    if s == "high":
        return 0.7
    if s == "medium":
        return 0.35
    return 0.1


def run_strategic_agent(ctx: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    """
    Operator-grade decisioning over fused context.
    Returns (decisions_as_dicts, reasoning_trace).
    """
    trace: list[str] = []
    trace.append("StrategicAgent: ingest fused events, alerts, investigations, insights.")

    lift = float(ctx.get("goal_severity_lift", 0.0))
    boost = float(ctx.get("decision_confidence_boost", 0.0))
    boost = min(0.08, max(0.0, boost))

    events = ctx.get("events") or []
    alerts = ctx.get("alerts") or []
    investigations = ctx.get("investigations") or []
    insights = ctx.get("insights") or []

    minute_bucket = datetime.now(timezone.utc).strftime("%Y%m%d%H%M")
    decisions: list[StrategicDecision] = []

    for ev in events[:40]:
        h3 = str(ev.get("h3_cell", ""))
        et = str(ev.get("event_type", "unknown"))
        eid = str(ev.get("event_id") or _fingerprint(h3, et, minute_bucket))
        base = _norm_severity(str(ev.get("severity", "medium")))
        if lift >= 0.06 and base == "medium":
            base = "high"
        elif lift >= 0.12 and base == "high":
            base = "critical"
        conf = 0.55
        if base == "high":
            conf = 0.72
        if base == "critical":
            conf = 0.86
        conf = min(0.97, conf + boost)

        if base == "low":
            continue

        if base == "critical":
            action = "ESCALATE_OPERATOR_WEBHOOK_DEEP_TRACE"
        elif base == "high":
            action = "NOTIFY_ACTION_HOOKS_INCREASE_MONITORING"
        elif base == "medium":
            action = "LOG_AND_WATCH_REPLAY_WINDOW"
        else:
            action = "ROUTINE_LOG_ONLY"

        rationale = (
            f"Fusion event {et} at {h3} with fused severity {ev.get('severity')}. "
            f"Recommended posture: {action}."
        )
        decisions.append(
            StrategicDecision(
                event_id=eid,
                severity=base,
                action=action,
                confidence=conf,
                rationale=rationale,
                sources=["fusion_event"],
            )
        )

    for a in alerts[:15]:
        sev = _norm_severity(str(a.get("severity", "warning")))
        if lift >= 0.06 and sev == "medium":
            sev = "high"
        elif lift >= 0.12 and sev == "high":
            sev = "critical"
        if sev == "low":
            continue
        title = str(a.get("title", "alert"))
        digest = hashlib.sha256(title.encode("utf-8")).hexdigest()[:24]
        eid = f"alert:{digest}"
        conf = 0.58 if sev == "medium" else 0.74 if sev == "high" else 0.82
        conf = min(0.97, conf + boost)
        action = (
            "ESCALATE_OPERATOR_WEBHOOK"
            if sev == "critical"
            else "NOTIFY_ACTION_HOOKS"
            if sev == "high"
            else "INVESTIGATOR_FOLLOWUP"
        )
        decisions.append(
            StrategicDecision(
                event_id=eid,
                severity=sev,
                action=action,
                confidence=conf,
                rationale=str(a.get("body", title))[:1200],
                sources=["alert"],
            )
        )

    if investigations:
        top = investigations[0]
        conf = float(top.get("confidence", 0.5))
        conf = min(0.97, conf + boost)
        if conf >= 0.55:
            eid = f"inv:{hashlib.sha256(str(top.get('title','')).encode()).hexdigest()[:20]}"
            decisions.append(
                StrategicDecision(
                    event_id=eid,
                    severity="medium" if conf < 0.7 else "high",
                    action="REQUEST_ANALYST_REVIEW_QUEUE",
                    confidence=min(0.9, conf),
                    rationale=str(top.get("narrative", ""))[:1200],
                    sources=["investigator"],
                )
            )

    if insights:
        trace.append(f"StrategicAgent: incorporated {len(insights)} narrator insight(s) into posture.")

    trace.append(f"StrategicAgent: emitted {len(decisions)} decision(s).")
    out = [
        {
            "event_id": d.event_id,
            "severity": d.severity,
            "action": d.action,
            "confidence": d.confidence,
            "rationale": d.rationale,
            "sources": d.sources,
        }
        for d in decisions[:120]
    ]
    return out, trace
