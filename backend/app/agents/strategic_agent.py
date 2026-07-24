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


def _market_quality(layers: dict[str, Any]) -> tuple[float, list[str]]:
    """
    Market quality score in [0,1] from cross-asset breadth and quote availability.
    """
    crypto = layers.get("market_crypto") or []
    fx = layers.get("market_forex") or []
    eq = layers.get("market_equities") or []
    sources: list[str] = []
    breadth = 0.0
    if crypto:
        breadth += 0.40
        sources.append("market_crypto")
    if fx:
        breadth += 0.30
        sources.append("market_forex")
    if eq:
        breadth += 0.30
        sources.append("market_equities")
    priced = 0
    for row in (crypto[:6] + fx[:4] + eq[:4]):
        try:
            if float(row.get("price", 0.0)) > 0:
                priced += 1
        except (TypeError, ValueError):
            continue
    quality = min(1.0, breadth + min(0.2, 0.02 * priced))
    return quality, sources


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
    correlations = ctx.get("correlations") or []
    layers = ctx.get("layers") or {}

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

    # Trading strategy overlay: combine anomaly load + correlations + market breadth.
    anomaly_load = min(1.0, sum(_severity_weight(str(e.get("severity", "low"))) for e in events[:30]) / 12.0)
    corr_strength = 0.0
    corr_n = 0
    for c in correlations[:8]:
        try:
            corr_strength += max(0.0, min(1.0, float(c.get("score", 0.0))))
            corr_n += 1
        except (TypeError, ValueError):
            continue
    corr_signal = (corr_strength / corr_n) if corr_n else 0.0
    market_quality, market_sources = _market_quality(layers)
    trade_quality = max(0.0, min(1.0, 0.45 * anomaly_load + 0.30 * corr_signal + 0.25 * market_quality + boost))
    trace.append(
        "StrategicAgent: trade_quality="
        f"{trade_quality:.3f} (anomaly={anomaly_load:.3f}, corr={corr_signal:.3f}, market={market_quality:.3f})."
    )
    if trade_quality >= 0.60:
        decisions.append(
            StrategicDecision(
                event_id=f"trade:{minute_bucket}",
                severity="high" if trade_quality >= 0.75 else "medium",
                action="TRADING_SIGNAL_QUALITY_LONG_BIAS",
                confidence=min(0.95, 0.55 + 0.35 * trade_quality),
                rationale=(
                    "Composite strategist score indicates strong cross-source alignment "
                    f"(anomaly {anomaly_load:.2f}, correlation {corr_signal:.2f}, market {market_quality:.2f})."
                ),
                sources=["anomaly", "correlation", *market_sources],
            )
        )
    elif anomaly_load >= 0.7:
        decisions.append(
            StrategicDecision(
                event_id=f"trade:risk:{minute_bucket}",
                severity="high",
                action="TRADING_SIGNAL_DE_RISK",
                confidence=min(0.9, 0.50 + 0.30 * anomaly_load),
                rationale="Anomaly load elevated beyond safe execution envelope; reduce exposure.",
                sources=["anomaly"],
            )
        )

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
