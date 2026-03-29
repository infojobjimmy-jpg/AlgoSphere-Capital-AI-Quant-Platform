"""
Weekend strategy evolution: demo-only research loop.

Generates diversified strategy candidates, replays them on synthetic or CSV OHLC,
mutates top performers, and selects a low-correlation top-N portfolio.

No broker access, no live execution, no capital deployment.
"""

from __future__ import annotations

import csv
import json
import math
import os
import random
import statistics
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from .bot_factory import rank_candidates
from .config import DATA_DIR
from .evolution_engine import evolve_candidates

DEFAULT_OHLC_CSV = DATA_DIR / "ohlc_history.csv"
DEFAULT_PANEL_CSV = DATA_DIR / "ohlc_history_panel.csv"

WeightingMode = Literal["equal", "risk_parity"]


def no_synthetic_history_enforced() -> bool:
    """When true, ``run_weekend_cycle`` must not fall back to ``synthetic_closes``."""
    return os.environ.get("ALGO_SPHERE_NO_SYNTHETIC_HISTORY", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
RankingMode = Literal["legacy", "research"]

DIVERSITY_BUCKETS: tuple[tuple[str, str], ...] = (
    ("mean_reversion", "MEAN_REVERSION"),
    ("ema_cross", "EMA_CROSS"),
    ("trend_following", "TREND_FOLLOWING"),
    ("session_breakout", "SESSION_BREAKOUT"),
    ("opening_range_breakout", "OPENING_RANGE_BREAKOUT"),
    ("momentum", "MOMENTUM"),
    ("volatility_regime", "VOLATILITY_REGIME"),
    ("liquidity_sweep", "LIQUIDITY_SWEEP"),
    ("regime_switching", "REGIME_SWITCHING"),
    ("cross_asset", "CROSS_ASSET_CONFIRM"),
)

# Diversified portfolio: hard cap per family + minimum counts (original five families; tests rely on this).
PORTFOLIO_FAMILY_HARD_CAP = 4
PORTFOLIO_MIN_QUOTAS: dict[str, int] = {
    "EMA_CROSS": 3,
    "SESSION_BREAKOUT": 3,
    "MOMENTUM": 3,
    "MEAN_REVERSION": 3,
    "VOLATILITY_REGIME": 2,
}

# Client / presentation books: cap dominance, optional correlation-first selection.
PRESENTATION_FAMILY_HARD_CAP = 2
PRESENTATION_PORTFOLIO_N_DEFAULT = 8
PRESENTATION_MAX_CORRELATION_DEFAULT = 0.62


def _pick_risk_profile(expected_drawdown: float) -> str:
    if expected_drawdown >= 320:
        return "HIGH"
    if expected_drawdown >= 180:
        return "MEDIUM"
    return "LOW"


def _pick_status(fitness_score: float, expected_drawdown: float) -> str:
    if fitness_score >= 80 and expected_drawdown <= 180:
        return "APPROVED_FOR_REVIEW"
    if fitness_score >= 65:
        return "CANDIDATE"
    if fitness_score >= 45:
        return "TESTING"
    return "REJECTED"


def _fitness_from_metrics(win_rate: float, drawdown: float, risk_multiplier: float) -> float:
    base = (win_rate * 100) - (drawdown / 10)
    adjusted = base + (10 * (1.2 - abs(1.0 - risk_multiplier)))
    return round(max(0.0, min(100.0, adjusted)), 2)


def _build_parameters(rng: random.Random, family: str) -> dict[str, Any]:
    params: dict[str, Any] = {
        "risk_multiplier": round(rng.uniform(0.5, 1.5), 2),
        "tp_sl_ratio": round(rng.uniform(1.1, 3.0), 2),
        "session_filter": rng.choice(["ASIA", "LONDON", "NY", "ALL"]),
        "volatility_filter": rng.choice(["LOW", "MEDIUM", "HIGH"]),
    }
    if family == "EMA_CROSS":
        fast = rng.randint(5, 40)
        slow = rng.randint(fast + 5, fast + 80)
        params["ema_fast"] = fast
        params["ema_slow"] = slow
    elif family == "TREND_FOLLOWING":
        params["donchian"] = rng.randint(15, 55)
    elif family == "MOMENTUM":
        params["lookback"] = rng.randint(5, 60)
        params["threshold"] = round(rng.uniform(0.2, 2.0), 2)
    elif family == "MEAN_REVERSION":
        params["zscore_window"] = rng.randint(10, 80)
        params["reversion_band"] = round(rng.uniform(0.8, 3.0), 2)
    elif family == "SESSION_BREAKOUT":
        params["breakout_window"] = rng.randint(5, 30)
        params["confirmation_bars"] = rng.randint(1, 5)
    elif family == "OPENING_RANGE_BREAKOUT":
        params["orb_window"] = rng.randint(15, 45)
        params["orb_open_bars"] = rng.randint(3, 8)
    elif family == "VOLATILITY_REGIME":
        params["vol_lookback"] = rng.randint(10, 60)
        params["atr_window"] = rng.randint(5, 30)
        params["regime_threshold"] = round(rng.uniform(0.15, 1.2), 2)
    elif family == "LIQUIDITY_SWEEP":
        params["sweep_window"] = rng.randint(6, 25)
        params["reclaim_bps"] = round(rng.uniform(3.0, 25.0), 2)
    elif family == "REGIME_SWITCHING":
        params["vol_lookback"] = rng.randint(12, 45)
        params["high_vol_factor"] = round(rng.uniform(1.05, 1.45), 3)
        params["zscore_window"] = rng.randint(10, 50)
        params["reversion_band"] = round(rng.uniform(0.9, 2.8), 2)
        params["trend_fast"] = rng.randint(5, 18)
        params["trend_slow"] = rng.randint(19, 45)
    elif family == "CROSS_ASSET_CONFIRM":
        params["lookback"] = rng.randint(8, 40)
        params["cross_threshold"] = round(rng.uniform(1.5, 8.0), 2)
    return params


def generate_diverse_candidates(
    min_total: int = 500,
    *,
    seed: int | None = None,
) -> list[dict[str, Any]]:
    """At least ``min_total`` strategies, spread evenly across diversity buckets."""
    rng = random.Random(seed)
    n = max(5, min_total)
    base, rem = divmod(n, len(DIVERSITY_BUCKETS))
    counts = [base + (1 if i < rem else 0) for i in range(len(DIVERSITY_BUCKETS))]
    created_at = datetime.now(timezone.utc).isoformat()
    out: list[dict[str, Any]] = []
    for (_, family), count in zip(DIVERSITY_BUCKETS, counts):
        for _ in range(count):
            parameters = _build_parameters(rng, family)
            expected_drawdown = round(rng.uniform(60, 420), 2)
            expected_win_rate = round(rng.uniform(0.35, 0.75), 3)
            fitness_score = _fitness_from_metrics(
                win_rate=expected_win_rate,
                drawdown=expected_drawdown,
                risk_multiplier=float(parameters["risk_multiplier"]),
            )
            status = _pick_status(fitness_score, expected_drawdown)
            out.append(
                {
                    "strategy_id": uuid4().hex,
                    "family": family,
                    "parameters": parameters,
                    "fitness_score": fitness_score,
                    "expected_drawdown": expected_drawdown,
                    "expected_win_rate": expected_win_rate,
                    "risk_profile": _pick_risk_profile(expected_drawdown),
                    "status": status,
                    "created_at": created_at,
                    "parent_strategy_id": None,
                    "generation": 0,
                    "mutation_note": "weekend diverse seed",
                    "origin_type": "WEEKEND_GENERATED",
                }
            )
    return out


def load_closes_from_csv(path: Path) -> list[float] | None:
    """Expects a header row with ``close`` or a single numeric column."""
    if not path.is_file():
        return None
    closes: list[float] = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)
    if not rows:
        return None
    header = [h.strip().lower() for h in rows[0]]
    if "close" in header:
        ci = header.index("close")
        for row in rows[1:]:
            if len(row) > ci:
                try:
                    closes.append(float(row[ci]))
                except ValueError:
                    continue
    else:
        for row in rows:
            if not row:
                continue
            try:
                closes.append(float(row[0]))
            except ValueError:
                continue
    return closes if len(closes) >= 32 else None


def load_panel_from_csv(path: Path) -> dict[str, list[float]] | None:
    """
    Panel CSV: header row with ``time`` plus one column per symbol (close prices).
    All columns must align row-wise (same length after parsing).
    """
    if not path.is_file():
        return None
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return None
        fields = [h.strip() for h in reader.fieldnames if h]
        key_map = {h.strip().lower(): h for h in fields}
        if "time" not in key_map:
            return None
        series: dict[str, list[float]] = {}
        for h in fields:
            hl = h.strip().lower()
            if hl == "time":
                continue
            series[h.strip()] = []
        for row in reader:
            for name in series:
                raw = row.get(name, "").strip()
                try:
                    series[name].append(float(raw))
                except (ValueError, TypeError):
                    series[name].append(float("nan"))
        cleaned: dict[str, list[float]] = {}
        for name, vals in series.items():
            if any(math.isnan(x) for x in vals):
                continue
            if len(vals) >= 32:
                cleaned[name] = vals
        return cleaned if len(cleaned) >= 2 else None


def synthetic_closes(*, bars: int, seed: int) -> list[float]:
    rng = random.Random(seed)
    price = 100.0
    out = [price]
    for _ in range(max(31, bars) - 1):
        price *= 1.0 + rng.gauss(0.00015, 0.009)
        price = max(1e-6, price)
        out.append(price)
    return out


def _returns_from_closes(closes: list[float]) -> list[float]:
    r: list[float] = []
    for i in range(1, len(closes)):
        prev = closes[i - 1]
        if prev == 0:
            r.append(0.0)
        else:
            r.append((closes[i] - prev) / prev)
    return r


def _signal_position(
    hist: list[float],
    strategy: dict[str, Any],
    panel_slice: dict[str, list[float]] | None = None,
) -> float:
    """
    Position in [-1, 1] from price history. Optional ``panel_slice`` maps symbol
    name → closes up to the same bar index as ``hist`` (research / cross-asset).
    """
    family = str(strategy.get("family", ""))
    p = strategy.get("parameters") or {}
    if len(hist) < 3:
        return 0.0

    if family == "EMA_CROSS":
        ef = int(p.get("ema_fast", 12))
        es = int(p.get("ema_slow", 26))
        if len(hist) < max(ef, es) + 1:
            return 0.0
        sf = sum(hist[-ef:]) / ef
        ss = sum(hist[-es:]) / es
        if sf > ss:
            return 1.0
        if sf < ss:
            return -1.0
        return 0.0

    if family == "TREND_FOLLOWING":
        n = int(p.get("donchian", 20))
        if len(hist) < n + 1:
            return 0.0
        window = hist[-(n + 1) : -1]
        hi, lo = max(window), min(window)
        c = hist[-1]
        if c > hi:
            return 1.0
        if c < lo:
            return -1.0
        return 0.0

    if family == "MEAN_REVERSION":
        w = int(p.get("zscore_window", 20))
        band = float(p.get("reversion_band", 1.5))
        if len(hist) < w:
            return 0.0
        window = hist[-w:]
        mu = sum(window) / w
        sig = statistics.pstdev(window) or 1e-9
        z = (hist[-1] - mu) / sig
        if z > band:
            return -1.0
        if z < -band:
            return 1.0
        return 0.0

    if family == "MOMENTUM":
        lb = int(p.get("lookback", 10))
        th = float(p.get("threshold", 0.5)) / 100.0
        if len(hist) < lb + 1:
            return 0.0
        base = hist[-lb - 1]
        mom = (hist[-1] - base) / (abs(base) or 1e-9)
        if mom > th:
            return 1.0
        if mom < -th:
            return -1.0
        return 0.0

    if family == "SESSION_BREAKOUT":
        bw = int(p.get("breakout_window", 10))
        if len(hist) < bw + 2:
            return 0.0
        segment = hist[-bw - 1 : -1]
        hh, ll = max(segment), min(segment)
        c = hist[-1]
        if c > hh:
            return 1.0
        if c < ll:
            return -1.0
        return 0.0

    if family == "OPENING_RANGE_BREAKOUT":
        w = int(p.get("orb_window", 20))
        f = int(p.get("orb_open_bars", 5))
        if len(hist) < w + 1 or f < 2:
            return 0.0
        seg = hist[-(w + 1) : -1]
        if len(seg) < f + 1:
            return 0.0
        open_rng = seg[:f]
        hi, lo = max(open_rng), min(open_rng)
        c = hist[-1]
        if c > hi:
            return 1.0
        if c < lo:
            return -1.0
        return 0.0

    if family == "VOLATILITY_REGIME":
        vw = int(p.get("vol_lookback", 20))
        th = float(p.get("regime_threshold", 0.5)) / 100.0
        rm = float(p.get("risk_multiplier", 1.0))
        if len(hist) < vw + 2:
            return 0.0
        rets = []
        for j in range(-vw, 0):
            a, b = hist[j - 1], hist[j]
            rets.append((b - a) / (abs(a) or 1e-9))
        vol = statistics.pstdev(rets) if len(rets) > 1 else 0.01
        base = hist[-vw - 1]
        mom = (hist[-1] - base) / (abs(base) or 1e-9)
        if vol < 0.012 and abs(mom) > th:
            pos = 1.0 if mom > 0 else -1.0
        elif vol < 0.02:
            pos = 0.5 if mom > 0 else (-0.5 if mom < 0 else 0.0)
        else:
            pos = 0.0
        return max(-1.0, min(1.0, pos * min(rm, 1.2)))

    if family == "LIQUIDITY_SWEEP":
        w = int(p.get("sweep_window", 10))
        bps = float(p.get("reclaim_bps", 5.0))
        eps = bps / 10000.0
        if len(hist) < w + 3:
            return 0.0
        prior = hist[-(w + 3) : -2]
        if not prior:
            return 0.0
        swing_low = min(prior)
        swing_high = max(prior)
        if hist[-2] <= swing_low and hist[-1] > swing_low * (1.0 + eps):
            return 1.0
        if hist[-2] >= swing_high and hist[-1] < swing_high * (1.0 - eps):
            return -1.0
        return 0.0

    if family == "REGIME_SWITCHING":
        vw = int(p.get("vol_lookback", 20))
        hf = float(p.get("high_vol_factor", 1.15))
        L = len(hist)
        if L < max(vw + 3, 35):
            return 0.0
        rets_now = []
        for j in range(L - vw, L):
            a, b = hist[j - 1], hist[j]
            rets_now.append((b - a) / (abs(a) or 1e-9))
        vol_now = statistics.pstdev(rets_now) if len(rets_now) > 1 else 0.01
        long_span = min(vw * 3, L - 1)
        long_rets = []
        for j in range(L - long_span, L):
            a, b = hist[j - 1], hist[j]
            long_rets.append((b - a) / (abs(a) or 1e-9))
        vol_ref = statistics.pstdev(long_rets) if len(long_rets) > 1 else vol_now
        vol_ref = max(vol_ref, 1e-6)
        high_vol = vol_now > hf * vol_ref
        if high_vol:
            w = int(p.get("zscore_window", 20))
            band = float(p.get("reversion_band", 1.5))
            if len(hist) < w:
                return 0.0
            window = hist[-w:]
            mu = sum(window) / w
            sig = statistics.pstdev(window) or 1e-9
            z = (hist[-1] - mu) / sig
            if z > band:
                return -1.0
            if z < -band:
                return 1.0
            return 0.0
        ef = int(p.get("trend_fast", 8))
        es = int(p.get("trend_slow", 21))
        if len(hist) < max(ef, es) + 1:
            return 0.0
        sf = sum(hist[-ef:]) / ef
        ss = sum(hist[-es:]) / es
        if sf > ss:
            return 1.0
        if sf < ss:
            return -1.0
        return 0.0

    if family == "CROSS_ASSET_CONFIRM":
        lb = int(p.get("lookback", 15))
        th = float(p.get("cross_threshold", 3.0)) / 100.0
        if panel_slice and len(panel_slice) >= 2:
            keys = sorted(panel_slice.keys())
            a, b = keys[0], keys[1]
            sa, sb = panel_slice[a], panel_slice[b]
            if len(sa) < lb + 1 or len(sb) < lb + 1:
                return 0.0

            def mom(series: list[float]) -> float:
                return (series[-1] - series[-lb - 1]) / (abs(series[-lb - 1]) or 1e-9)

            m1, m2 = mom(sa), mom(sb)
            if m1 > th and m2 > th:
                return 1.0
            if m1 < -th and m2 < -th:
                return -1.0
            return 0.0
        if len(hist) < lb + 2:
            return 0.0
        ma = sum(hist[-lb:]) / lb
        ma_prev = sum(hist[-lb - 1 : -1]) / lb
        dev = hist[-1] - ma
        dev_prev = hist[-2] - ma_prev
        tight = float(p.get("risk_multiplier", 1.0)) * 0.001
        if dev > tight and dev_prev > tight:
            return 1.0
        if dev < -tight and dev_prev < -tight:
            return -1.0
        return 0.0

    return 0.0


def historical_replay_backtest(
    strategy: dict[str, Any],
    closes: list[float],
    *,
    n_segments: int = 12,
    panel: dict[str, list[float]] | None = None,
) -> dict[str, Any]:
    """
    PnL = signal_t * next_return_t on the provided close series (historical or synthetic).
    Optional ``panel`` provides aligned per-symbol closes for cross-asset style rules.
    """
    returns = _returns_from_closes(closes)
    if len(returns) < 10:
        z = {
            "win_rate": 0.5,
            "max_drawdown_pct": 50.0,
            "stability": 0.0,
            "segment_pnls": [0.0] * n_segments,
            "composite": 0.0,
            "research_composite": 0.0,
            "total_return": 0.0,
            "bars": len(returns),
            "robustness": 0.0,
            "regime_adaptability": 0.0,
        }
        return z

    pnls: list[float] = []
    for k in range(len(returns)):
        hist = closes[: k + 1]
        panel_k: dict[str, list[float]] | None = None
        if panel:
            panel_k = {sym: series[: k + 1] for sym, series in panel.items()}
        pos = _signal_position(hist, strategy, panel_k)
        pnls.append(pos * returns[k])

    wins = sum(1 for x in pnls if x > 0)
    active = sum(1 for x in pnls if x != 0)
    win_rate = (wins / active) if active else 0.5

    eq = 1.0
    peak = 1.0
    max_dd = 0.0
    for p in pnls:
        eq *= 1.0 + p
        peak = max(peak, eq)
        max_dd = max(max_dd, (peak - eq) / (peak or 1e-9))

    if len(pnls) > 1:
        st = statistics.pstdev(pnls)
        stability = 1.0 / (1.0 + 50.0 * st)
    else:
        stability = 0.5

    seg_n = min(n_segments, len(pnls))
    chunk = max(1, len(pnls) // seg_n)
    segment_pnls: list[float] = []
    for i in range(seg_n):
        start = i * chunk
        end = (i + 1) * chunk if i < seg_n - 1 else len(pnls)
        segment_pnls.append(sum(pnls[start:end]))

    total_return = eq - 1.0
    composite = round(
        max(
            0.0,
            min(
                100.0,
                100.0 * win_rate - max_dd * 45.0 + 25.0 * stability + 15.0 * math.tanh(total_return * 20),
            ),
        ),
        2,
    )

    seg_mean = sum(segment_pnls) / len(segment_pnls) if segment_pnls else 0.0
    seg_std = statistics.pstdev(segment_pnls) if len(segment_pnls) > 1 else 0.0
    robustness = seg_mean / (seg_std + 1e-9)
    abs_sum = sum(abs(x) for x in segment_pnls)
    spread = max(segment_pnls) - min(segment_pnls) if segment_pnls else 0.0
    regime_adaptability = 1.0 - min(1.0, spread / (abs_sum + 1e-9))

    research_composite = round(
        max(
            0.0,
            min(
                100.0,
                22.0 * win_rate
                + 30.0 * (1.0 - min(1.0, max_dd * 1.25))
                + 20.0 * stability
                + 10.0 * (0.5 + 0.5 * math.tanh(robustness * 1.8))
                + 10.0 * regime_adaptability
                + 8.0 * math.tanh(total_return * 10.0),
            ),
        ),
        2,
    )

    return {
        "win_rate": round(win_rate, 4),
        "max_drawdown_pct": round(max_dd * 100.0, 2),
        "stability": round(stability, 4),
        "segment_pnls": segment_pnls,
        "composite": composite,
        "research_composite": research_composite,
        "total_return": round(total_return, 4),
        "bars": len(pnls),
        "robustness": round(robustness, 6),
        "regime_adaptability": round(regime_adaptability, 4),
    }


def compute_diversification_score(
    *,
    quotas_met: bool,
    min_quotas: dict[str, int],
    fam_count: Counter[str],
    avg_pairwise_abs_corr: float | None,
) -> float:
    """Higher is better: quota fulfillment, coverage of required families, low correlation."""
    n_req = len(min_quotas)
    if n_req == 0:
        return round(50.0 * (1.0 - min(1.0, avg_pairwise_abs_corr or 0.0)), 2)
    satisfied = sum(1 for f, need in min_quotas.items() if fam_count.get(f, 0) >= need)
    coverage = satisfied / n_req
    ac = min(1.0, avg_pairwise_abs_corr or 0.0)
    quota_factor = 1.0 if quotas_met else 0.6
    raw = quota_factor * (28.0 + 32.0 * coverage + 40.0 * (1.0 - ac))
    return round(min(100.0, raw), 2)


def pearson_correlation(a: list[float], b: list[float]) -> float:
    n = min(len(a), len(b))
    if n < 2:
        return 0.0
    a, b = a[:n], b[:n]
    ma, mb = sum(a) / n, sum(b) / n
    va = sum((x - ma) ** 2 for x in a)
    vb = sum((y - mb) ** 2 for y in b)
    if va < 1e-12 or vb < 1e-12:
        return 0.0
    cov = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
    return max(-1.0, min(1.0, cov / math.sqrt(va * vb)))


def select_diversified_portfolio(
    scored: list[dict[str, Any]],
    *,
    top_n: int = 20,
    max_correlation: float = 0.78,
    family_hard_cap: int = PORTFOLIO_FAMILY_HARD_CAP,
    family_cap: int | None = None,
    min_family_quotas: dict[str, int] | None = None,
    enforce_min_quotas: bool = True,
    rank_key: str = "_weekend_composite",
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Build a top-N book: optional minimum per-family counts, hard cap per family,
    then fill by score with a segment-PnL correlation limit (threshold relaxes if needed).

    ``rank_key`` selects which field on each strategy row is used for ordering (e.g.
    ``_growth_score`` for presentation growth books).

    Returns ``(portfolio, meta)`` where ``meta`` holds quota status and warnings.
    """
    ranked = sorted(
        scored,
        key=lambda x: float(x.get(rank_key, x.get("_weekend_composite", 0.0))),
        reverse=True,
    )
    cap = family_hard_cap if family_cap is None else max(1, min(family_hard_cap, family_cap))
    mins = dict(PORTFOLIO_MIN_QUOTAS) if min_family_quotas is None else dict(min_family_quotas)
    meta: dict[str, Any] = {
        "family_hard_cap": cap,
        "min_quotas_requested": dict(mins),
        "quotas_met": True,
        "warnings": [],
        "rank_key": rank_key,
    }

    chosen: list[dict[str, Any]] = []
    chosen_ids: set[str] = set()
    fam_count: Counter[str] = Counter()

    def corr_ok(cand: dict[str, Any], thresh: float) -> bool:
        vec = cand.get("_segment_pnls") or []
        for other in chosen:
            ov = other.get("_segment_pnls") or []
            if pearson_correlation(vec, ov) > thresh:
                return False
        return True

    def add_cand(cand: dict[str, Any]) -> None:
        chosen.append(cand)
        chosen_ids.add(str(cand["strategy_id"]))
        fam_count[str(cand.get("family", ""))] += 1

    required_total = sum(mins.values()) if enforce_min_quotas else 0
    if enforce_min_quotas and mins and top_n < required_total:
        meta["warnings"].append(
            f"top_n={top_n} < sum(min_quotas)={required_total}; cannot satisfy all minimums"
        )
        meta["quotas_met"] = False
        enforce_min_quotas = False

    if enforce_min_quotas and mins:
        avail = Counter(str(s.get("family", "")) for s in scored)
        for fam in sorted(mins.keys(), key=lambda f: (avail.get(f, 0), mins[f])):
            need = mins[fam]
            if need > cap:
                meta["warnings"].append(
                    f"min quota {need} for {fam} exceeds family hard cap {cap}"
                )
                meta["quotas_met"] = False
                continue
            while fam_count[fam] < need and len(chosen) < top_n:
                picked: dict[str, Any] | None = None
                for cand in ranked:
                    sid = str(cand.get("strategy_id", ""))
                    if sid in chosen_ids or str(cand.get("family", "")) != fam:
                        continue
                    if fam_count[fam] >= cap:
                        break
                    for step in range(28):
                        thresh = min(0.995, max_correlation + step * 0.018)
                        if corr_ok(cand, thresh):
                            picked = cand
                            break
                    if picked:
                        break
                if picked:
                    add_cand(picked)
                else:
                    meta["warnings"].append(
                        f"Could not fill minimum quota for {fam} (have {fam_count[fam]}, need {need})"
                    )
                    meta["quotas_met"] = False
                    break

    thresh = max_correlation
    attempts = 0
    while len(chosen) < top_n and attempts < 22:
        progressed = False
        for cand in ranked:
            if len(chosen) >= top_n:
                break
            sid = str(cand.get("strategy_id", ""))
            if sid in chosen_ids:
                continue
            fam = str(cand.get("family", ""))
            if fam_count[fam] >= cap:
                continue
            if corr_ok(cand, thresh):
                add_cand(cand)
                progressed = True
        if len(chosen) >= top_n:
            break
        if not progressed:
            thresh = min(0.995, thresh + 0.025)
        attempts += 1

    if enforce_min_quotas and mins:
        for fam, need in mins.items():
            if fam_count[fam] < need:
                meta["quotas_met"] = False

    meta["family_distribution"] = dict(fam_count)
    return chosen, meta


def _apply_backtest_to_strategy(
    strategy: dict[str, Any],
    bt: dict[str, Any],
    *,
    use_research_composite: bool = False,
) -> None:
    # Map equity max drawdown (0–100%) to same rough scale as factory expected_drawdown (≈40–500).
    dd_pct = float(bt["max_drawdown_pct"])
    dd_store = round(max(40.0, min(500.0, dd_pct * 4.0)), 2)
    wr = float(bt["win_rate"])
    comp = float(bt.get("research_composite", bt["composite"])) if use_research_composite else float(bt["composite"])
    strategy["fitness_score"] = comp
    strategy["expected_win_rate"] = round(wr, 4)
    strategy["expected_drawdown"] = dd_store
    strategy["risk_profile"] = _pick_risk_profile(dd_store)
    strategy["status"] = _pick_status(comp, dd_store)


def attach_presentation_rank_scores(scored: list[dict[str, Any]]) -> None:
    """Growth vs demo-safe ranking helpers (research presentation only)."""
    for s in scored:
        bt = s.get("_weekend_backtest") or {}
        rc = float(bt.get("research_composite", bt.get("composite", 0.0)))
        tr = float(bt.get("total_return", 0.0))
        dd = float(bt.get("max_drawdown_pct", 50.0))
        reg = float(bt.get("regime_adaptability", 0.0))
        edd = float(s.get("expected_drawdown", 200.0))
        s["_growth_score"] = round(rc + 14.0 * math.tanh(tr * 16.0), 4)
        s["_safe_score"] = round(rc + 22.0 * reg - 0.42 * dd - 0.12 * edd, 4)


def load_history_meta_json(csv_path: Path | None) -> dict[str, Any] | None:
    """Read ``*.meta.json`` next to the basket CSV when present."""
    base = csv_path if csv_path is not None else DEFAULT_OHLC_CSV
    meta_p = base.with_suffix(".meta.json")
    if not meta_p.is_file():
        return None
    try:
        return json.loads(meta_p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _strip_internal_fields(s: dict[str, Any]) -> dict[str, Any]:
    out = {k: v for k, v in s.items() if not k.startswith("_")}
    return out


def build_risk_profile(portfolio: list[dict[str, Any]]) -> dict[str, Any]:
    if not portfolio:
        return {
            "demo_only": True,
            "no_execution": True,
            "aggregate_expected_drawdown_mean": 0.0,
            "family_mix": {},
            "avg_pairwise_correlation": None,
        }
    dds = [float(p.get("expected_drawdown", 0.0)) for p in portfolio]
    families = Counter(str(p.get("family", "")) for p in portfolio)
    corrs: list[float] = []
    for i, a in enumerate(portfolio):
        va = a.get("_segment_pnls") or []
        for b in portfolio[i + 1 :]:
            vb = b.get("_segment_pnls") or []
            corrs.append(abs(pearson_correlation(va, vb)))
    avg_corr = round(sum(corrs) / len(corrs), 4) if corrs else None
    return {
        "demo_only": True,
        "no_execution": True,
        "aggregate_expected_drawdown_mean": round(sum(dds) / len(dds), 2),
        "family_mix": dict(families),
        "avg_pairwise_abs_correlation": avg_corr,
        "disclaimer": "Research / replay only. Not investment advice. No live trading.",
    }


def equal_weights(n: int) -> list[float]:
    if n <= 0:
        return []
    w = 1.0 / n
    return [round(w, 6)] * n


def risk_parity_weights(strategies: list[dict[str, Any]]) -> list[float]:
    """Inverse volatility on segment PnLs (demo proxy); weights sum to 1."""
    if not strategies:
        return []
    vols: list[float] = []
    for s in strategies:
        seg = s.get("_segment_pnls") or [0.0]
        if len(seg) < 2:
            v = abs(float(seg[0])) + 1e-9
        else:
            v = statistics.pstdev([float(x) for x in seg]) + 1e-9
        # Floor avoids one near-flat book absorbing 100% weight in presentation portfolios.
        vols.append(max(1e-6, v))
    inv = [1.0 / v for v in vols]
    total = sum(inv)
    raw = [x / total for x in inv]
    rounded = [round(x, 6) for x in raw]
    drift = 1.0 - sum(rounded)
    if rounded:
        rounded[-1] = round(rounded[-1] + drift, 6)
    return rounded


def portfolio_weights(strategies: list[dict[str, Any]], mode: WeightingMode) -> list[float]:
    if mode == "equal":
        return equal_weights(len(strategies))
    return risk_parity_weights(strategies)


def resolve_history_closes(
    explicit_csv: Path | None,
    *,
    prefer_synthetic: bool,
) -> tuple[list[float] | None, str]:
    """
    Load OHLC closes once: explicit path first if provided, else optional
    ``data/ohlc_history.csv`` when ``prefer_synthetic`` is False.
    When ``ALGO_SPHERE_LIVE_TESTING`` is enabled, appends closes from
    ``data/ohlc_live_extension.csv`` for the primary live symbol (see live_data_engine).
    """
    from .live_data_engine import (
        live_ohlc_extension_path,
        live_testing_enabled,
        load_extension_closes_for_primary,
    )

    base: list[float] | None = None
    base_src = ""

    if explicit_csv is not None:
        if explicit_csv.is_file():
            loaded = load_closes_from_csv(explicit_csv)
            if loaded:
                base, base_src = loaded, str(explicit_csv.resolve())
        if base is None:
            return None, f"synthetic (explicit csv missing or too short: {explicit_csv})"
    elif prefer_synthetic:
        base = None
        base_src = "synthetic"
    elif DEFAULT_OHLC_CSV.is_file():
        loaded = load_closes_from_csv(DEFAULT_OHLC_CSV)
        if loaded:
            base, base_src = loaded, str(DEFAULT_OHLC_CSV.resolve())

    if live_testing_enabled() and not prefer_synthetic:
        ext = load_extension_closes_for_primary(live_ohlc_extension_path())
        if ext:
            if base:
                merged = list(base) + ext
                if len(merged) >= 32:
                    return merged, f"{base_src}+live_extension({len(ext)} bars)"
            elif len(ext) >= 32:
                return ext, f"live_extension_only({len(ext)} bars)"

    if base:
        return base, base_src
    if no_synthetic_history_enforced():
        return None, "real_data_required_no_csv_or_short_file"
    return None, "synthetic"


def _resolve_aligned_panel(
    closes: list[float],
    *,
    prefer_synthetic_history: bool,
    panel_csv: Path | None,
) -> dict[str, list[float]] | None:
    if prefer_synthetic_history:
        return None
    path = panel_csv if panel_csv is not None else DEFAULT_PANEL_CSV
    panel = load_panel_from_csv(path)
    if not panel:
        return None
    if not all(len(v) == len(closes) for v in panel.values()):
        return None
    return panel


def compute_client_demo_verdict(
    *,
    safe_portfolio_size: int,
    diversification_score: float | None,
    avg_pairwise_abs_correlation: float | None,
    extra_limitations: list[str],
) -> dict[str, Any]:
    """
    Heuristic GO / NO-GO for **demo / paper** follow-up (not a trading recommendation).
    """
    reasons: list[str] = []
    if safe_portfolio_size < 6:
        reasons.append("safe_book_has_fewer_than_6_strategies")
    if (diversification_score or 0) < 40:
        reasons.append("diversification_score_below_40")
    if avg_pairwise_abs_correlation is not None and avg_pairwise_abs_correlation > 0.58:
        reasons.append("average_pairwise_correlation_above_0_58")
    go = len(reasons) == 0
    return {
        "demo_paper_go_no_go": "GO" if go else "NO-GO",
        "reasons": reasons,
        "limitations": list(extra_limitations),
    }


def run_weekend_cycle(
    existing_factory: list[dict[str, Any]],
    *,
    min_generate: int = 1000,
    seed: int | None = None,
    historical_csv: Path | None = None,
    prefer_synthetic_history: bool = False,
    synthetic_bars: int = 504,
    top_portfolio_n: int = 20,
    max_correlation: float = 0.78,
    family_cap: int | None = None,
    enforce_portfolio_quotas: bool = True,
    portfolio_weighting: WeightingMode = "risk_parity",
    cycle_index: int | None = None,
    evolve_top_n: int = 18,
    children_per_parent: int = 4,
    ranking_mode: RankingMode = "legacy",
    panel_csv: Path | None = None,
    include_presentation_portfolios: bool = False,
    presentation_top_n: int = PRESENTATION_PORTFOLIO_N_DEFAULT,
    presentation_max_correlation: float = PRESENTATION_MAX_CORRELATION_DEFAULT,
) -> dict[str, Any]:
    """
    One evolution + backtest + selection pass. Persists nothing (caller inserts).
    If ``historical_csv`` is None and ``prefer_synthetic_history`` is False, loads
    ``data/ohlc_history.csv`` when that file exists and is valid.

    ``ranking_mode="research"`` uses the research composite (robustness / drawdown /
    stability weighted) for leaderboard fitness and ``_weekend_composite``.

    When ``include_presentation_portfolios`` is True, also builds **growth** and
    **demo-safe** books (6–10 names, low per-family cap, no min quotas).
    """
    rng = random.Random(seed)
    cycle_seed = rng.randint(1, 2**30 - 1)
    closes, source = resolve_history_closes(
        historical_csv, prefer_synthetic=prefer_synthetic_history
    )
    if closes is None:
        if no_synthetic_history_enforced():
            raise ValueError(
                "ALGO_SPHERE_NO_SYNTHETIC_HISTORY is enabled but no usable real OHLC history was found. "
                "Run scripts/fetch_ohlc_history.py (or set ALGO_SPHERE_DUKASCOPY_EXPORT_DIR) and ensure "
                f"{DEFAULT_OHLC_CSV} exists with >=32 closes."
            )
        closes = synthetic_closes(bars=synthetic_bars, seed=cycle_seed)

    panel = _resolve_aligned_panel(
        closes,
        prefer_synthetic_history=prefer_synthetic_history,
        panel_csv=panel_csv,
    )

    csv_for_meta: Path | None = historical_csv
    if csv_for_meta is None and not prefer_synthetic_history and DEFAULT_OHLC_CSV.is_file():
        csv_for_meta = DEFAULT_OHLC_CSV
    history_meta = load_history_meta_json(csv_for_meta)

    use_research = ranking_mode == "research"

    generated = generate_diverse_candidates(min_generate, seed=cycle_seed)
    evolved: list[dict[str, Any]] = []
    if existing_factory:
        parents = rank_candidates(existing_factory, limit=max(30, evolve_top_n))
        evolved = evolve_candidates(
            parents[:evolve_top_n],
            top_n=min(evolve_top_n, len(parents)),
            children_per_parent=children_per_parent,
            crossover_rate=0.28,
            seed=cycle_seed + 1,
        )
        for e in evolved:
            e["origin_type"] = str(e.get("origin_type", "MUTATED")) + "_WEEKEND"

    candidates = generated + evolved
    scored: list[dict[str, Any]] = []
    for s in candidates:
        bt = historical_replay_backtest(s, closes, panel=panel)
        comp_main = float(bt["research_composite"] if use_research else bt["composite"])
        s["_weekend_composite"] = comp_main
        s["_segment_pnls"] = bt["segment_pnls"]
        s["_weekend_backtest"] = {k: v for k, v in bt.items() if k != "segment_pnls"}
        _apply_backtest_to_strategy(s, bt, use_research_composite=use_research)
        scored.append(s)

    portfolio, sel_meta = select_diversified_portfolio(
        scored,
        top_n=top_portfolio_n,
        max_correlation=max_correlation,
        family_hard_cap=PORTFOLIO_FAMILY_HARD_CAP,
        family_cap=family_cap,
        enforce_min_quotas=enforce_portfolio_quotas,
    )
    weights = portfolio_weights(portfolio, portfolio_weighting)
    top_strategies = sorted(
        scored,
        key=lambda x: float(x.get("_weekend_composite", 0.0)),
        reverse=True,
    )[:top_portfolio_n]

    ts = datetime.now(timezone.utc).isoformat()
    risk = build_risk_profile(portfolio)
    fam_ct = Counter(str(p.get("family", "")) for p in portfolio)
    mins_used = sel_meta.get("min_quotas_requested") or {}
    if enforce_portfolio_quotas and mins_used:
        risk["diversification_score"] = compute_diversification_score(
            quotas_met=bool(sel_meta.get("quotas_met")),
            min_quotas=mins_used,
            fam_count=fam_ct,
            avg_pairwise_abs_corr=risk.get("avg_pairwise_abs_correlation"),
        )
    else:
        ac = risk.get("avg_pairwise_abs_correlation") or 0.0
        risk["diversification_score"] = round(min(100.0, 55.0 + 45.0 * (1.0 - min(1.0, ac))), 2)
    risk["quotas_met"] = bool(sel_meta.get("quotas_met"))
    if sel_meta.get("warnings"):
        risk["portfolio_warnings"] = list(sel_meta["warnings"])

    limitations: list[str] = []
    if "synthetic" in source:
        limitations.append("History is synthetic (no usable CSV path); not live market data.")
    if panel is None:
        limitations.append(
            "Cross-asset confirmation falls back to single-series logic unless "
            "ohlc_history_panel.csv exists and row count matches the basket series."
        )
    limitations.append(
        "Liquidity sweep and opening-range rules are close-only proxies on daily data "
        "(no real wicks or session clocks)."
    )
    if history_meta:
        limitations.extend(history_meta.get("limitations") or [])
    else:
        limitations.append("No ohlc_history.meta.json next to basket CSV (fetch script writes this).")

    out: dict[str, Any] = {
        "timestamp_utc": ts,
        "cycle_index": cycle_index,
        "demo_only": True,
        "no_live_trading": True,
        "ranking_mode": ranking_mode,
        "history_source": source,
        "history_meta": history_meta,
        "panel_symbols": sorted(panel.keys()) if panel else [],
        "panel_csv_used": str((panel_csv or DEFAULT_PANEL_CSV).resolve()) if panel else None,
        "portfolio_weighting": portfolio_weighting,
        "bars_used": len(closes),
        "generated_count": len(generated),
        "evolved_count": len(evolved),
        "evaluated_count": len(scored),
        "top_strategies_by_score": [
            {
                "strategy_id": x["strategy_id"],
                "family": x["family"],
                "fitness_score": x["fitness_score"],
                "expected_win_rate": x["expected_win_rate"],
                "expected_drawdown": x["expected_drawdown"],
                "origin_type": x.get("origin_type"),
                "backtest": x.get("_weekend_backtest"),
            }
            for x in top_strategies
        ],
        "diversified_portfolio": [
            {
                "strategy_id": x["strategy_id"],
                "family": x["family"],
                "fitness_score": x["fitness_score"],
                "weight": w,
                "backtest": x.get("_weekend_backtest"),
            }
            for x, w in zip(portfolio, weights)
        ],
        "risk_profile": risk,
        "portfolio_selection": {
            "family_hard_cap": sel_meta.get("family_hard_cap"),
            "min_quotas_requested": sel_meta.get("min_quotas_requested"),
            "quotas_met": sel_meta.get("quotas_met"),
            "warnings": list(sel_meta.get("warnings") or []),
            "family_distribution": sel_meta.get("family_distribution"),
            "rank_key": sel_meta.get("rank_key"),
        },
        "research_limitations": limitations,
        "ready_for_demo_review": True,
        "strategies_for_db": [_strip_internal_fields(s) for s in scored],
    }

    if include_presentation_portfolios:
        attach_presentation_rank_scores(scored)
        pres_n = max(6, min(10, presentation_top_n))
        growth_port, growth_meta = select_diversified_portfolio(
            scored,
            top_n=pres_n,
            max_correlation=presentation_max_correlation,
            family_hard_cap=PRESENTATION_FAMILY_HARD_CAP,
            family_cap=PRESENTATION_FAMILY_HARD_CAP,
            enforce_min_quotas=False,
            rank_key="_growth_score",
        )
        safe_port, safe_meta = select_diversified_portfolio(
            scored,
            top_n=pres_n,
            max_correlation=presentation_max_correlation,
            family_hard_cap=PRESENTATION_FAMILY_HARD_CAP,
            family_cap=PRESENTATION_FAMILY_HARD_CAP,
            enforce_min_quotas=False,
            rank_key="_safe_score",
        )
        wg = portfolio_weights(growth_port, portfolio_weighting)
        ws = portfolio_weights(safe_port, portfolio_weighting)
        risk_g = build_risk_profile(growth_port)
        risk_s = build_risk_profile(safe_port)
        ac_s = risk_s.get("avg_pairwise_abs_correlation")
        div_s = round(
            min(
                100.0,
                50.0 + 50.0 * (1.0 - min(1.0, ac_s or 0.0)),
            ),
            2,
        )
        verdict = compute_client_demo_verdict(
            safe_portfolio_size=len(safe_port),
            diversification_score=div_s,
            avg_pairwise_abs_correlation=ac_s,
            extra_limitations=limitations,
        )

        def _weighted_return(strats: list[dict[str, Any]], wts: list[float]) -> float:
            t = 0.0
            for s, w in zip(strats, wts):
                bt = s.get("_weekend_backtest") or {}
                t += w * float(bt.get("total_return", 0.0))
            return round(t, 4)

        def _top_k_safe(scored_list: list[dict[str, Any]], k: int) -> list[dict[str, Any]]:
            ranked_s = sorted(
                scored_list,
                key=lambda z: (float(z.get("expected_drawdown", 999)), -float(z.get("_weekend_composite", 0))),
            )
            out_rows: list[dict[str, Any]] = []
            for z in ranked_s:
                if len(out_rows) >= k:
                    break
                out_rows.append(
                    {
                        "strategy_id": z["strategy_id"],
                        "family": z["family"],
                        "fitness_score": z["fitness_score"],
                        "expected_drawdown": z.get("expected_drawdown"),
                        "research_composite": (z.get("_weekend_backtest") or {}).get("research_composite"),
                        "max_drawdown_pct": (z.get("_weekend_backtest") or {}).get("max_drawdown_pct"),
                    }
                )
            return out_rows

        def _top_k_growth(scored_list: list[dict[str, Any]], k: int) -> list[dict[str, Any]]:
            ranked_g = sorted(
                scored_list,
                key=lambda z: (
                    -float((z.get("_weekend_backtest") or {}).get("total_return", 0)),
                    -float(z.get("_weekend_composite", 0)),
                ),
            )
            out_g: list[dict[str, Any]] = []
            for z in ranked_g:
                if len(out_g) >= k:
                    break
                out_g.append(
                    {
                        "strategy_id": z["strategy_id"],
                        "family": z["family"],
                        "fitness_score": z["fitness_score"],
                        "total_return": (z.get("_weekend_backtest") or {}).get("total_return"),
                        "research_composite": (z.get("_weekend_backtest") or {}).get("research_composite"),
                        "max_drawdown_pct": (z.get("_weekend_backtest") or {}).get("max_drawdown_pct"),
                    }
                )
            return out_g

        fam_all = Counter(str(s.get("family", "")) for s in scored)
        out["presentation"] = {
            "growth_portfolio": [
                {
                    "strategy_id": x["strategy_id"],
                    "family": x["family"],
                    "fitness_score": x["fitness_score"],
                    "weight": w,
                    "backtest": x.get("_weekend_backtest"),
                }
                for x, w in zip(growth_port, wg)
            ],
            "growth_portfolio_meta": growth_meta,
            "growth_risk_profile": risk_g,
            "growth_weighted_return_proxy": _weighted_return(growth_port, wg),
            "demo_safe_portfolio": [
                {
                    "strategy_id": x["strategy_id"],
                    "family": x["family"],
                    "fitness_score": x["fitness_score"],
                    "weight": w,
                    "backtest": x.get("_weekend_backtest"),
                }
                for x, w in zip(safe_port, ws)
            ],
            "demo_safe_portfolio_meta": safe_meta,
            "demo_safe_risk_profile": risk_s,
            "demo_safe_weighted_return_proxy": _weighted_return(safe_port, ws),
            "demo_safe_diversification_score": div_s,
            "client_demo_verdict": verdict,
            "top_5_safest_candidates": _top_k_safe(scored, 5),
            "top_5_strongest_growth_candidates": _top_k_growth(scored, 5),
            "family_mix_evaluated": dict(fam_all),
        }

    return out


def save_report(report: dict[str, Any], directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    ts = report.get("timestamp_utc", datetime.now(timezone.utc).isoformat())
    safe = ts.replace(":", "").replace("+", "_")
    path = directory / f"weekend_report_{safe}.json"
    payload = {k: v for k, v in report.items() if k != "strategies_for_db"}
    payload["evaluated_strategy_count"] = len(report.get("strategies_for_db", []))
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    return path
