from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
from typing import Any

import numpy as np

from app.agents.anomaly_agent import cluster_events_by_grid, detect_count_anomalies
from app.agents.anomaly_v2 import density_importance, detect_multivariate_anomalies
from app.agents.correlation import correlate_aircraft_weather
from app.agents.investigator import run_investigator
from app.agents.narrator import run_narrator
from app.agents.strategic_agent import run_strategic_agent
from app.agents.watcher import run_watcher
from app.config import settings
from app.fusion.event_engine import detect_events, events_to_dicts
from app.learning.adaptive_thresholds import adapt_from_cycle, load_thresholds
import json as _json


def forecast_counts(history: deque[int]) -> dict[str, float]:
    if len(history) < 3:
        return {"next": float(history[-1]) if history else 0.0, "slope": 0.0}
    xs = list(range(len(history)))
    ys = list(history)
    n = float(len(xs))
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys, strict=False))
    den = sum((x - mean_x) ** 2 for x in xs) or 1.0
    slope = num / den
    next_x = len(xs)
    next_y = mean_y + slope * (next_x - mean_x)
    return {"next": float(max(0.0, next_y)), "slope": float(slope)}


async def update_trails(r, aircraft: list[dict[str, Any]], maxlen: int = 8) -> None:
    import json

    for a in aircraft[:4000]:
        icao = str(a.get("id", ""))
        if not icao:
            continue
        lat = float(a.get("lat", 0.0))
        lon = float(a.get("lon", 0.0))
        alt = float(a.get("alt_m") or 0.0)
        key = settings.redis_trail_key(icao)
        raw = await r.get(key)
        hist = json.loads(raw) if raw else []
        hist.append({"lat": lat, "lon": lon, "alt": alt, "t": datetime.now(timezone.utc).isoformat()})
        hist = hist[-maxlen:]
        await r.set(key, json.dumps(hist))


async def build_snapshot(
    *,
    layers: dict[str, list],
    ac_hist: deque[int],
    sh_hist: deque[int],
    ewma_state: dict[str, float],
    feat_history: list[np.ndarray],
    r,
    working_thresholds: dict[str, float] | None = None,
    goal_severity_lift: float = 0.0,
    evolution_guidance: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, float], list[np.ndarray]]:
    th = working_thresholds if working_thresholds is not None else await load_thresholds(r)

    aircraft = layers.get("aircraft", [])
    wx = layers.get("weather", [])
    ships = layers.get("ships", [])

    events, ewma2 = detect_events(
        aircraft, ships, wx, ewma_state, settings.h3_aircraft_resolution, thresholds=th
    )
    event_dicts = events_to_dicts(events)

    mv_signals, feat_hist2 = detect_multivariate_anomalies(
        aircraft,
        ships,
        wx,
        feat_history,
        settings.h3_aircraft_resolution,
        contamination=float(th.get("iso_contamination", 0.06)),
        drift_scale=float(th.get("drift_residual_scale", 0.35)),
        iso_warn=float(th.get("iso_warn", 0.25)),
        iso_critical=float(th.get("iso_critical", 0.45)),
    )

    alerts: list[dict[str, Any]] = []
    for sig in detect_count_anomalies("aircraft", list(ac_hist), len(aircraft)):
        alerts.append(
            {
                "severity": sig.severity,
                "title": sig.title,
                "body": sig.body,
                "payload": sig.payload,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    for sig in mv_signals:
        alerts.append(
            {
                "severity": sig.severity,
                "title": sig.title,
                "body": sig.body,
                "payload": sig.payload,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    corr = correlate_aircraft_weather(aircraft, wx)
    correlations = [
        {"score": c.score, "title": c.title, "details": c.details, "payload": c.payload}
        for c in corr
    ]

    heatmaps = {
        "aircraft": cluster_events_by_grid(aircraft, cell_deg=3.0),
        "ships": cluster_events_by_grid(ships, cell_deg=2.5),
    }

    imp = density_importance(aircraft, settings.h3_aircraft_resolution)
    enriched_aircraft: list[dict[str, Any]] = []
    for a in aircraft:
        d = dict(a)
        icao = str(d.get("id", ""))
        d["importance"] = float(imp.get(icao, 0.0))
        enriched_aircraft.append(d)

    layers_out = dict(layers)
    layers_out["aircraft"] = enriched_aircraft

    ctx = {
        "meta": {"updated_at": datetime.now(timezone.utc).isoformat()},
        "layers": layers_out,
        "events": event_dicts,
        "correlations": correlations,
        "alerts": alerts,
    }

    watch = run_watcher({**ctx, "layers": layers_out})
    for w in watch:
        alerts.append(
            {
                "severity": w.severity,
                "title": w.title,
                "body": w.body,
                "payload": w.payload,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    investigations = run_investigator({**ctx, "alerts": alerts})
    inv_public = [
        {
            "confidence": i.confidence,
            "title": i.title,
            "narrative": i.narrative,
            "evidence": i.evidence,
        }
        for i in investigations
    ]

    narrator_ctx = {
        **ctx,
        "alerts": alerts,
        "investigations": inv_public,
    }
    insights = await run_narrator(narrator_ctx)

    eg = evolution_guidance or {}
    try:
        dcb = float(min(0.08, max(0.0, float(eg.get("decision_confidence_boost", 0.0)))))
    except (TypeError, ValueError):
        dcb = 0.0
    strategic_ctx = {
        **ctx,
        "alerts": alerts,
        "investigations": inv_public,
        "insights": insights,
        "goal_severity_lift": float(goal_severity_lift),
        "decision_confidence_boost": dcb,
    }
    decisions, reasoning_trace = run_strategic_agent(strategic_ctx)

    focus = None
    for a in alerts:
        if str(a.get("severity")) == "critical":
            pl = a.get("payload") or {}
            lat = pl.get("lat")
            lon = pl.get("lon")
            if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
                focus = {"lat": float(lat), "lon": float(lon), "alt": 1_800_000.0}
                break
    if focus is None and event_dicts:
        try:
            import h3

            h = str(event_dicts[0].get("h3_cell", ""))
            lat, lng = h3.cell_to_latlng(h)
            focus = {"lat": float(lat), "lon": float(lng), "alt": 2_200_000.0}
        except Exception:
            focus = None

    _now = datetime.now(timezone.utc).isoformat()
    _src_raw = None
    try:
        _src_raw = await r.get(f"{settings.app_slug}:sources")
    except Exception:
        pass
    _persisted_src: dict = _json.loads(_src_raw) if _src_raw else {}
    sources = {
        "cameras": _persisted_src.get("cameras") or {
            "status": "live" if layers.get("cameras") else "unavailable",
            "count": len(layers.get("cameras") or []),
            "provider": "MTQ_WFS",
            "error_code": None,
            "updated_at": _now,
        },
        "aircraft": {
            "status": "live" if aircraft else "unavailable",
            "count": len(aircraft),
            "provider": "opensky_network",
            "error_code": None,
            "updated_at": _now,
        },
        "satellites": {
            "status": "live" if layers.get("satellites") else "unavailable",
            "count": len(layers.get("satellites") or []),
            "provider": "celestrak_spacetrack",
            "error_code": None,
            "updated_at": _now,
        },
        "ships": {
            "status": "live" if ships else "unavailable",
            "count": len(ships),
            "provider": "aisstream",
            "error_code": None,
            "updated_at": _now,
        },
        "weather": {
            "status": "live" if wx else "unavailable",
            "count": len(wx),
            "provider": "open_meteo",
            "error_code": None,
            "updated_at": _now,
        },
    }

    snap = {
        "meta": {
            "updated_at": _now,
            "forecast": {
                "aircraft_count": forecast_counts(ac_hist),
                "ships_count": forecast_counts(sh_hist),
            },
            "focus": focus,
            "reasoning_trace": reasoning_trace,
            "learning": {
                "air_density_z": th.get("air_density_z"),
                "iso_contamination": th.get("iso_contamination"),
                "iso_warn": th.get("iso_warn"),
                "iso_critical": th.get("iso_critical"),
            },
            "strategy": {},
        },
        "sources": sources,
        "layers": layers_out,
        "alerts": alerts[:80],
        "correlations": correlations,
        "heatmaps": heatmaps,
        "events": event_dicts,
        "investigations": inv_public,
        "insights": insights,
        "decisions": decisions,
    }

    try:
        await update_trails(r, aircraft)
    except Exception:
        pass

    try:
        th2 = await adapt_from_cycle(r, snap, th)
        snap["meta"]["learning"] = {
            "air_density_z": th2.get("air_density_z"),
            "iso_contamination": th2.get("iso_contamination"),
            "iso_warn": th2.get("iso_warn"),
            "iso_critical": th2.get("iso_critical"),
        }
    except Exception:
        pass

    return snap, ewma2, feat_hist2
