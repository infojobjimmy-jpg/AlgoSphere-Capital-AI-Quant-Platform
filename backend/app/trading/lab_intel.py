"""Advisory-only trading lab intelligence — rule-based, no execution, no extra data sources."""

from __future__ import annotations

from typing import Any

from app.config import settings


def _norm_sym(s: str) -> str:
    u = str(s or "").strip().upper().replace("-", "").replace("/", "")
    if u.endswith("USDT"):
        return u[:-4]
    if u.endswith("USD"):
        return u[:-3]
    return u


def regime_from_signals(signals: list[dict[str, Any]]) -> dict[str, Any]:
    """Heuristic regime from recent signal stream (DB-backed)."""
    if not signals:
        return {"regime": "low_volatility", "confidence": 0.35}
    strengths = [float(s.get("strength") or 0) for s in signals[:24]]
    sides = [str(s.get("side") or "").upper() for s in signals[:24]]
    avg_st = sum(strengths) / max(len(strengths), 1)
    buy_share = sum(1 for x in sides if x.startswith("B")) / max(len(sides), 1)
    skew = abs(buy_share - 0.5)
    vol_proxy = max(0.0, min(1.0, (max(strengths) - min(strengths)) if strengths else 0.0))
    if vol_proxy < 0.08 and avg_st < 0.62:
        return {"regime": "low_volatility", "confidence": round(0.45 + 0.2 * (1 - vol_proxy), 2)}
    if vol_proxy > 0.22:
        return {"regime": "high_volatility", "confidence": round(0.5 + 0.35 * vol_proxy, 2)}
    if skew > 0.28:
        return {"regime": "trend", "confidence": round(0.55 + 0.35 * skew, 2)}
    return {"regime": "range", "confidence": round(0.5 + 0.25 * (1 - skew), 2)}


def risk_intel(
    *,
    equity: float,
    peak: float,
    kill_switch: bool,
    mode: str,
    open_positions: int,
) -> dict[str, Any]:
    peak = max(peak, 1.0)
    dd = 100.0 * (1.0 - float(equity) / peak)
    kill_pct = float(settings.trading_global_drawdown_kill_pct)
    emerg = float(settings.trading_emergency_stop_dd_pct)
    if kill_switch:
        return {
            "risk_state": "halted",
            "recommendation": "kill_switch_engaged",
            "reason": "BLOCKED BY: kill_switch",
        }
    if dd >= emerg:
        return {
            "risk_state": "critical",
            "recommendation": "reduce_size",
            "reason": f"drawdown {dd:.1f}% >= emergency {emerg:.0f}%",
        }
    if dd >= kill_pct:
        return {
            "risk_state": "elevated",
            "recommendation": "reduce_size",
            "reason": f"drawdown {dd:.1f}% >= global gate {kill_pct:.0f}%",
        }
    if dd >= float(settings.trading_global_drawdown_kill_pct) * 0.65:
        return {
            "risk_state": "elevated",
            "recommendation": "reduce_size",
            "reason": f"drawdown {dd:.1f}% approaching limits",
        }
    if open_positions >= 4:
        return {
            "risk_state": "elevated",
            "recommendation": "avoid_low_volatility",
            "reason": "many open positions — watch overtrading",
        }
    return {
        "risk_state": "normal",
        "recommendation": "maintain",
        "reason": f"drawdown {dd:.2f}% within tolerance",
    }


def trade_explanation(last_order: dict[str, Any] | None, last_signal: dict[str, Any] | None) -> str:
    if not last_order and not last_signal:
        return "No recent orders or signals in database window."
    parts: list[str] = []
    if last_signal:
        parts.append(
            f"Latest signal {last_signal.get('side')} {last_signal.get('symbol')} "
            f"strength={float(last_signal.get('strength') or 0):.3f}"
        )
        rat = str(last_signal.get("rationale") or "")[:160]
        if rat:
            parts.append(f"Rationale: {rat}")
    if last_order:
        parts.append(
            f"Latest order {last_order.get('side')} {last_order.get('symbol')} "
            f"status={last_order.get('status')} notional={float(last_order.get('notional_usd') or 0):.2f}"
        )
    return " · ".join(parts)


def performance_hint(signals: list[dict[str, Any]], regime: str) -> str:
    if len(signals) >= 18 and regime == "low_volatility":
        return "Trading too frequently in low volatility — consider smaller size or pausing adds."
    if len(signals) < 3:
        return "Low signal cadence — filters or market conditions may be quiet."
    return "Signal cadence looks moderate vs regime."


def build_ai_payload(
    *,
    message: str,
    signals: list[dict[str, Any]],
    orders: list[dict[str, Any]],
    positions: list[dict[str, Any]],
    risk_row: dict[str, Any],
    kill_switch: bool,
    mode: str,
    extra_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    msg = (message or "").lower()
    regime = regime_from_signals(signals)
    ri = risk_intel(
        equity=float(risk_row.get("equity") or 0),
        peak=float(risk_row.get("peak") or 1),
        kill_switch=kill_switch,
        mode=mode,
        open_positions=len(positions),
    )
    last_sig = signals[0] if signals else None
    last_ord = orders[0] if orders else None
    explain = trade_explanation(last_ord, last_sig)
    perf = performance_hint(signals, str(regime.get("regime")))

    blockers: list[str] = []
    if kill_switch:
        blockers.append("BLOCKED BY: kill_switch")
    if ri["risk_state"] == "critical":
        blockers.append("BLOCKED BY: drawdown / emergency_stop")
    if ri["risk_state"] == "elevated" and float(risk_row.get("drawdown_pct") or 0) >= float(
        settings.trading_global_drawdown_kill_pct
    ):
        blockers.append("BLOCKED BY: drawdown / global_drawdown_gate")
    if not signals:
        blockers.append("No recent DB signals — possible filter quiet or ingestion lag.")

    sym_set = {_norm_sym(str(s.get("symbol"))) for s in signals[:12] if s.get("symbol")}
    mkt = f"Watchlist activity (from signals): {', '.join(sorted(sym_set)) or '—'} · regime={regime['regime']}"

    summary_parts: list[str] = []
    g = (extra_context or {}).get("globe") if isinstance(extra_context, dict) else None
    if isinstance(g, dict):
        summary_parts.append(
            "Globe layer counts: "
            f"aircraft={g.get('aircraft')} satellites={g.get('satellites')} ships={g.get('ships')} "
            f"cameras={g.get('cameras')} weather={g.get('weather')} "
            f"crypto_ticks={g.get('crypto_ticks')} forex_ticks={g.get('forex_ticks')} "
            f"equity_ticks={g.get('equity_ticks')} updated_at={g.get('updated_at')!r}."
        )
    if "why" in msg and ("no trade" in msg or "not trade" in msg):
        summary_parts.append(
            "Likely reasons: " + ("; ".join(blockers) if blockers else "No hard blockers from kill/DD in snapshot.")
        )
    if "risk" in msg:
        summary_parts.append(f"Risk: {ri['risk_state']} — {ri['reason']}")
    if not summary_parts:
        summary_parts.append(explain)

    action = "Suggestion only: "
    if ri["recommendation"] == "reduce_size":
        action += "reduce size; confirm mode; avoid pyramiding in elevated DD."
    elif regime["regime"] == "low_volatility":
        action += "stand down on adds in low-vol unless signals strengthen."
    else:
        action += "maintain discipline; align adds with regime and risk state."

    return {
        "summary": " ".join(summary_parts),
        "risk": f"{ri['risk_state']}: {ri['reason']}",
        "market": mkt,
        "action": action,
        "regime": regime,
        "risk_intel": ri,
        "trade_explanation": explain,
        "performance": perf,
        "blockers": blockers,
    }
