from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _market_rows(snap: dict[str, Any]) -> list[dict[str, Any]]:
    layers = snap.get("layers") or {}
    return [
        *(layers.get("market_crypto") or []),
        *(layers.get("market_forex") or []),
        *(layers.get("market_equities") or []),
    ]


def _best_price_by_symbol(rows: list[dict[str, Any]]) -> dict[str, float]:
    out: dict[str, float] = {}
    for row in rows:
        sym = str(row.get("symbol", "")).upper()
        if not sym:
            continue
        try:
            px = float(row.get("price", 0.0))
        except (TypeError, ValueError):
            continue
        if px > 0:
            out[sym] = px
    return out


def derive_signals(snap: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Deterministic signal hints from fused context + market ticks (no duplicate ML — uses snapshot).
    """
    sigs: list[dict[str, Any]] = []
    rows = _market_rows(snap)
    px_by_sym = _best_price_by_symbol(rows)
    events = snap.get("events") or []
    correlations = snap.get("correlations") or []
    decisions = snap.get("decisions") or []

    if len(events) >= 3:
        sigs.append(
            {
                "symbol": "MACRO:RISK",
                "side": "de_risk",
                "strength": min(1.0, 0.35 + 0.05 * len(events)),
                "rationale": f"Elevated fused event throughput ({len(events)} events).",
                "meta": {"source": "fusion_load"},
            }
        )

    for c in correlations[:2]:
        try:
            sc = float(c.get("score", 0.0))
        except (TypeError, ValueError):
            sc = 0.0
        if sc >= 0.55:
            sigs.append(
                {
                    "symbol": "CORR:WIND_AIR",
                    "side": "watchlist",
                    "strength": min(1.0, sc),
                    "rationale": str(c.get("title", "correlation"))[:500],
                    "meta": {"source": "correlation"},
                }
            )

    for d in decisions[:10]:
        action = str(d.get("action", ""))
        if not action.startswith("TRADING_SIGNAL_"):
            continue
        confidence = float(d.get("confidence", 0.0))
        if confidence < 0.55:
            continue
        if "DE_RISK" in action:
            sigs.append(
                {
                    "symbol": "MACRO:RISK",
                    "side": "de_risk",
                    "strength": min(1.0, 0.55 + 0.35 * confidence),
                    "rationale": str(d.get("rationale", ""))[:600],
                    "meta": {"source": "strategist", "action": action},
                }
            )
            continue
        if "LONG_BIAS" in action:
            for sym, price in list(px_by_sym.items())[:5]:
                sigs.append(
                    {
                        "symbol": sym,
                        "side": "buy_bias",
                        "strength": min(1.0, 0.40 + 0.55 * confidence),
                        "rationale": f"Strategist long-bias confirmation on {sym}.",
                        "meta": {"source": "strategist", "action": action, "price": price},
                    }
                )

    for t in rows[:10]:
        sym = str(t.get("symbol", ""))
        price = t.get("price")
        if not sym or price is None:
            continue
        st = 0.72 if sym.upper().startswith("BITCOIN") else min(0.80, 0.40 + 0.04 * min(8, len(rows)))
        sigs.append(
            {
                "symbol": sym,
                "side": "buy_bias",
                "strength": float(st),
                "rationale": f"Public quote {sym} @ {price} ({t.get('venue')}).",
                "meta": {
                    "source": "market_tick",
                    "venue": t.get("venue"),
                    "asset_class": t.get("asset_class"),
                    "price": float(price),
                },
            }
        )

    ts = datetime.now(timezone.utc).isoformat()
    dedup: dict[tuple[str, str, str], dict[str, Any]] = {}
    for s in sigs:
        s["time"] = ts
        key = (str(s.get("symbol", "")), str(s.get("side", "")), str((s.get("meta") or {}).get("source", "")))
        prev = dedup.get(key)
        if prev is None or float(s.get("strength", 0.0)) > float(prev.get("strength", 0.0)):
            dedup[key] = s
    return sorted(dedup.values(), key=lambda x: -float(x.get("strength", 0.0)))[:24]
