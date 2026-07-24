"""Ultra-quant style advisory heuristics — read-only ranking, no execution."""

from __future__ import annotations

from typing import Any, Literal


def _num(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _sym(row: dict[str, Any]) -> str:
    return str(row.get("symbol") or row.get("pair") or "?").upper()


def _ema_momentum(scores: list[float], span: int = 5) -> float:
    if not scores:
        return 0.0
    k = 2.0 / (span + 1)
    ema = scores[0]
    for s in scores[1:]:
        ema = k * s + (1 - k) * ema
    tail = sum(scores[-span:]) / max(1, min(span, len(scores)))
    head = sum(scores[:span]) / max(1, min(span, len(scores)))
    return max(-1.0, min(1.0, (tail - head) / max(1e-6, abs(head) + 0.05)))


def _vol_score(drawdown_pct: float, signal_n: int, open_legs: int) -> float:
    """0 = calm, 1 = stressed."""
    dd = max(0.0, drawdown_pct) / 40.0
    churn = min(1.0, signal_n / 40.0)
    exposure = min(1.0, open_legs / 12.0)
    return max(0.0, min(1.0, 0.45 * dd + 0.35 * churn + 0.2 * exposure))


def _risk_level(conf: float, vol: float) -> Literal["low", "medium", "high"]:
    if vol > 0.72 or conf < 0.45:
        return "high"
    if vol > 0.45 or conf < 0.62:
        return "medium"
    return "low"


def analyze_trading_advisory(
    positions: list[dict[str, Any]],
    pnl: dict[str, Any],
    signals: list[dict[str, Any]],
    volatility: float,
    regime: str,
    symbol_universe: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Advisory-only synthesis. Does not place orders or alter risk limits.

    Returns:
        opportunities, warnings, summary
    """
    warnings: list[str] = []
    opportunities: list[dict[str, Any]] = []

    dd = _num(pnl.get("drawdown_pct"))
    agg = pnl.get("aggregate_pnl_usd")
    open_legs = len([p for p in positions if isinstance(p, dict)])
    pending_hint = int(pnl.get("pending_orders", 0) or 0)

    if dd > 12:
        warnings.append("Drawdown elevated — prioritize risk reduction over new risk.")
    if pending_hint > 6:
        warnings.append("Many pending orders — avoid overtrading; confirm fills before adding exposure.")
    if volatility > 0.75:
        warnings.append("Volatility / stress score high — tighten size and widen confirmation.")

    # Per-symbol momentum from recent signals (strength series)
    by_sym: dict[str, list[float]] = {}
    for s in signals:
        if not isinstance(s, dict):
            continue
        sym = _sym(s)
        st = _num(s.get("strength"))
        by_sym.setdefault(sym, []).append(st)

    universe_syms = {_sym(u) for u in symbol_universe if isinstance(u, dict)}
    latest_by_sym: dict[str, dict[str, Any]] = {}
    for s in signals:
        if not isinstance(s, dict):
            continue
        sym = _sym(s)
        if sym not in latest_by_sym:
            latest_by_sym[sym] = s

    ranked: list[tuple[str, float, str, float]] = []
    for sym, series in by_sym.items():
        if len(series) < 2:
            continue
        mom = _ema_momentum(series)
        avg_st = sum(series) / len(series)
        latest = latest_by_sym.get(sym) or {}
        latest_side = str(latest.get("side") or ("BUY" if mom >= 0 else "SELL")).upper()
        if latest_side not in {"BUY", "SELL"}:
            latest_side = "BUY" if mom >= 0 else "SELL"
        score = avg_st * (0.55 + 0.45 * abs(mom)) * (1.0 - 0.35 * volatility)
        ranked.append((sym, score, latest_side, mom))

    ranked.sort(key=lambda x: x[1], reverse=True)

    overtrade_guard = min(1.0, pending_hint / 10.0) + min(1.0, len(signals) / 50.0)
    if overtrade_guard > 1.2:
        warnings.append("Signal cadence + queue depth suggest overtrading risk — stand down size.")

    for sym, score, side, mom in ranked[:8]:
        if score < 0.18:
            continue
        conf = max(0.0, min(0.95, score * (1.1 - 0.5 * volatility)))
        if conf < 0.35:
            continue
        rl = _risk_level(conf, volatility)
        reason_parts = [
            f"regime={regime}",
            f"momentum={mom:+.2f}",
            f"avg_strength={score:.2f}",
        ]
        if sym in universe_syms:
            reason_parts.append("universe=active_ticks")
        opportunities.append(
            {
                "symbol": sym,
                "action": side if side in {"BUY", "SELL"} else ("BUY" if mom >= 0 else "SELL"),
                "confidence": round(conf, 2),
                "reason": " · ".join(reason_parts),
                "risk_level": rl,
            }
        )

    if not opportunities and signals:
        # fallback: top single signal as weak opportunity
        top = next((s for s in signals if isinstance(s, dict)), None)
        if top:
            sym = _sym(top)
            st = _num(top.get("strength"))
            side = str(top.get("side") or "BUY").upper()
            if side not in {"BUY", "SELL"}:
                side = "BUY"
            conf = max(0.25, min(0.75, st * (1.0 - 0.4 * volatility)))
            opportunities.append(
                {
                    "symbol": sym,
                    "action": side,
                    "confidence": round(conf, 2),
                    "reason": "single-signal preview (weak evidence)",
                    "risk_level": _risk_level(conf, volatility),
                }
            )

    tone = "stressed" if volatility > 0.55 else "balanced"
    summary = (
        f"{tone.capitalize()} tape · regime={regime} · stress={volatility:.2f} · open_legs={open_legs}"
        + (f" · agg_pnl={agg}" if agg is not None else "")
    )

    return {
        "opportunities": opportunities[:12],
        "warnings": warnings,
        "summary": summary[:500],
    }
