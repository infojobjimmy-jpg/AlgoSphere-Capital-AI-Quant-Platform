from __future__ import annotations

from collections import Counter
from typing import Any


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


# --- Portfolio Brain: stronger correlation model (decision layer only) ---
CORRELATION_PENALTY = 0.72  # was 0.50 in (1 - k * avg_corr)
SAME_FAMILY_CORR = 0.92
BASE_CORR = 0.16
LINEAGE_BONUS = 0.14
FAMILY_WEIGHT_SOFT_CAP = 0.36
RISK_PAUSE_THRESHOLD = 0.78
DELTA_STRONG = 0.045
DELTA_MILD = 0.022


def _pair_correlation(a: dict[str, Any], b: dict[str, Any]) -> float:
    """Heuristic correlation proxy; same family and shared lineage increase overlap."""
    if a.get("strategy_id") and a.get("strategy_id") == b.get("strategy_id"):
        return 1.0
    corr = BASE_CORR
    if a.get("family") == b.get("family"):
        corr = SAME_FAMILY_CORR
    pa, pb = a.get("parent_strategy_id"), b.get("parent_strategy_id")
    if pa and pb and pa == pb:
        corr = min(1.0, corr + LINEAGE_BONUS)
    if pa and pb and (pa == b.get("strategy_id") or pb == a.get("strategy_id")):
        corr = min(1.0, corr + LINEAGE_BONUS * 0.75)
    return corr


def _avg_correlation(i: int, items: list[dict[str, Any]]) -> float:
    if len(items) <= 1:
        return 0.0
    me = items[i]
    vals = [_pair_correlation(me, other) for j, other in enumerate(items) if j != i]
    return sum(vals) / len(vals)


def _apply_family_soft_cap(raw_scores: list[float], families: list[str], cap: float) -> list[float]:
    """Iteratively shrink overweight families so each family's raw-mass share stays near `cap`."""
    out = [max(0.0, float(x)) for x in raw_scores]
    n_fam = max(1, len(set(families)))
    # Cannot require every family below `cap` if cap < 1/n (e.g. two families cannot both be ≤0.3).
    effective_cap = max(cap, (1.0 / n_fam) + 0.02)
    for _ in range(48):
        total = sum(out)
        if total <= 0:
            return [1.0 / len(out)] * len(out) if out else out
        sum_by_f: dict[str, float] = {}
        for i, fam in enumerate(families):
            sum_by_f[fam] = sum_by_f.get(fam, 0.0) + out[i]
        w_share = {f: s / total for f, s in sum_by_f.items()}
        over = [f for f, sh in w_share.items() if sh > effective_cap + 1e-9]
        if not over:
            break
        f = max(over, key=lambda fam: w_share.get(fam, 0.0))
        sum_a = sum_by_f[f]
        sum_b = total - sum_a
        if sum_a <= 0 or (1.0 - effective_cap) <= 1e-12:
            break
        # k * sum_a / (k * sum_a + sum_b) == effective_cap
        k = (effective_cap * sum_b) / (sum_a * (1.0 - effective_cap))
        k = _clamp(k, 0.0, 1.0)
        for i, fam in enumerate(families):
            if fam == f:
                out[i] *= k
    return out


def _cap_and_redistribute(weights: list[float], cap: float) -> list[float]:
    n = len(weights)
    out = list(weights)
    if n == 0:
        return out
    remaining_indices = set(range(n))
    total = sum(out)
    if total <= 0:
        return [0.0] * n
    out = [w / total for w in out]

    changed = True
    while changed:
        changed = False
        excess = 0.0
        for i in list(remaining_indices):
            if out[i] > cap:
                excess += out[i] - cap
                out[i] = cap
                remaining_indices.remove(i)
                changed = True
        if excess > 0 and remaining_indices:
            rem_total = sum(out[i] for i in remaining_indices)
            if rem_total <= 0:
                add_each = excess / len(remaining_indices)
                for i in remaining_indices:
                    out[i] += add_each
            else:
                for i in remaining_indices:
                    out[i] += excess * (out[i] / rem_total)
    return out


def _fund_weights_by_id(fund_portfolio_strategies: list[dict[str, Any]] | None) -> dict[str, float]:
    if not fund_portfolio_strategies:
        return {}
    out: dict[str, float] = {}
    for row in fund_portfolio_strategies:
        sid = str(row.get("strategy_id", "") or "")
        if not sid:
            continue
        out[sid] = float(row.get("allocation_percent", 0.0) or 0.0)
    return out


def _classify_brain_action(target_weight: float, fund_weight: float, risk_score: float) -> tuple[str, str]:
    tw, cw, risk = float(target_weight), float(fund_weight), float(risk_score)
    if risk >= RISK_PAUSE_THRESHOLD:
        return (
            "PAUSE_ALLOCATION",
            "Risk score above pause threshold; avoid increasing simulated allocation.",
        )
    if cw < 0.008 and tw >= 0.03:
        return (
            "ROTATE_IN",
            "Brain target meaningful vs minimal fund-engine slot; candidate to rotate in.",
        )
    if cw >= 0.04 and tw < 0.012:
        return (
            "ROTATE_OUT",
            "Fund-engine weight remains while brain target is minimal; candidate to rotate out.",
        )
    if tw - cw >= DELTA_STRONG:
        return (
            "INCREASE_ALLOCATION",
            f"Increase simulated allocation toward target (Δ={tw - cw:+.1%} of portfolio weight).",
        )
    if cw - tw >= DELTA_STRONG:
        return (
            "REDUCE_ALLOCATION",
            f"Reduce simulated allocation toward target (Δ={tw - cw:+.1%} of portfolio weight).",
        )
    if tw - cw >= DELTA_MILD:
        return (
            "INCREASE_ALLOCATION",
            f"Mild increase toward target (Δ={tw - cw:+.1%}).",
        )
    if cw - tw >= DELTA_MILD:
        return (
            "REDUCE_ALLOCATION",
            f"Mild reduction toward target (Δ={tw - cw:+.1%}).",
        )
    return "HOLD", "Near fund-engine mix; no material shift recommended."


def _build_capital_shift_recommendations(
    allocations: list[dict[str, Any]],
    fund_by_id: dict[str, float],
) -> list[dict[str, Any]]:
    def tw(a: dict[str, Any]) -> float:
        return float(a.get("weight", 0.0))

    def cw(sid: str) -> float:
        return float(fund_by_id.get(str(sid), 0.0))

    donors = sorted(
        [a for a in allocations if a.get("brain_action") in {"REDUCE_ALLOCATION", "ROTATE_OUT"}],
        key=lambda a: cw(str(a["strategy_id"])) - tw(a),
        reverse=True,
    )
    receivers = sorted(
        [a for a in allocations if a.get("brain_action") in {"INCREASE_ALLOCATION", "ROTATE_IN"}],
        key=lambda a: tw(a) - cw(str(a["strategy_id"])),
        reverse=True,
    )
    out: list[dict[str, Any]] = []
    n = min(4, len(donors), len(receivers))
    for i in range(n):
        fr, to = donors[i], receivers[i]
        sid_f, sid_t = str(fr["strategy_id"]), str(to["strategy_id"])
        gap = min(abs(tw(to) - cw(sid_t)), abs(cw(sid_f) - tw(fr)))
        out.append(
            {
                "from_strategy_id": sid_f,
                "to_strategy_id": sid_t,
                "suggested_shift_weight": round(_clamp(gap, 0.0, 0.15), 4),
                "reason": (
                    f"Shift simulated weight from {fr.get('brain_action')} "
                    f"toward {to.get('brain_action')} (decision layer only)."
                ),
            }
        )
    return out


def build_portfolio_allocation(
    live_safe_candidates: list[dict[str, Any]],
    max_percent_per_strategy: float = 20.0,
    *,
    fund_portfolio_strategies: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Decision-layer allocation + Portfolio Brain extensions (rotation, family balance,
    correlation-aware scoring, explainable actions). No trading or broker execution.
    """
    if not live_safe_candidates:
        return {
            "count": 0,
            "allocations": [],
            "total_allocated_percent": 0.0,
            "brain": _empty_brain(),
            "decision_layer_only": True,
        }

    family_counts = Counter(str(c.get("family", "UNKNOWN")) for c in live_safe_candidates)
    families_list = [str(c.get("family", "UNKNOWN")) for c in live_safe_candidates]

    raw_scores: list[float] = []
    risk_scores: list[float] = []
    correlations: list[float] = []

    for i, c in enumerate(live_safe_candidates):
        drawdown = float(c.get("paper_drawdown", 250.0))
        win_rate = float(c.get("paper_win_rate", 0.5))
        promo = float(c.get("promotion_score", 50.0))
        family = str(c.get("family", "UNKNOWN"))

        risk_score = _clamp((drawdown / 350.0) * 0.6 + (1.0 - win_rate) * 0.4, 0.0, 1.0)
        avg_corr = _avg_correlation(i, live_safe_candidates)
        diversification_factor = 1.0 / max(1, family_counts[family])

        quality = _clamp(
            (promo / 100.0) * 0.55 + win_rate * 0.30 + (1 - drawdown / 500.0) * 0.15,
            0.0,
            1.0,
        )
        # Stronger correlation penalty + family diversification
        raw = (
            quality
            * (1.0 - risk_score)
            * (1.0 - CORRELATION_PENALTY * avg_corr)
            * (0.80 + 0.20 * diversification_factor)
        )

        risk_scores.append(round(risk_score, 3))
        correlations.append(round(avg_corr, 3))
        raw_scores.append(max(0.0, raw))

    raw_scores = _apply_family_soft_cap(raw_scores, families_list, FAMILY_WEIGHT_SOFT_CAP)

    total_raw = sum(raw_scores)
    if total_raw <= 0:
        base = [1.0 / len(raw_scores)] * len(raw_scores)
    else:
        base = [x / total_raw for x in raw_scores]

    cap = _clamp(max_percent_per_strategy / 100.0, 0.01, 1.0)
    capped_weights = _cap_and_redistribute(base, cap=cap)
    allocable_percent = min(100.0, len(live_safe_candidates) * max_percent_per_strategy)

    fund_by_id = _fund_weights_by_id(fund_portfolio_strategies)

    allocations: list[dict[str, Any]] = []
    for i, c in enumerate(live_safe_candidates):
        sid = str(c.get("strategy_id", ""))
        w = capped_weights[i]
        capital_percent = round(w * allocable_percent, 2)
        avg_c = correlations[i]
        priority_score = round(
            w * (1.0 - risk_scores[i]) * (1.0 - 0.45 * avg_c),
            6,
        )
        action, reason = _classify_brain_action(w, fund_by_id.get(sid, 0.0), risk_scores[i])
        allocations.append(
            {
                "strategy_id": c.get("strategy_id"),
                "family": c.get("family"),
                "weight": round(w, 6),
                "capital_percent": capital_percent,
                "risk_score": risk_scores[i],
                "avg_correlation": avg_c,
                "priority_score": priority_score,
                "brain_action": action,
                "brain_reason": reason,
                "allocation_reason": (
                    f"family={c.get('family')}, corr={avg_c}, "
                    f"promo={round(float(c.get('promotion_score', 0.0)), 2)}, "
                    f"target={c.get('target_status')}, brain={action}"
                ),
            }
        )

    allocations.sort(key=lambda x: float(x.get("priority_score", 0.0)), reverse=True)
    for rank, row in enumerate(allocations, start=1):
        row["priority_rank"] = rank

    allocations_by_capital = sorted(allocations, key=lambda x: x["capital_percent"], reverse=True)
    total_alloc = round(sum(a["capital_percent"] for a in allocations_by_capital), 2)

    target_ids = {str(a["strategy_id"]) for a in allocations}
    rotate_out: list[dict[str, Any]] = []
    for sid, cw in fund_by_id.items():
        if cw > 0.025 and sid not in target_ids:
            rotate_out.append(
                {
                    "strategy_id": sid,
                    "brain_action": "ROTATE_OUT",
                    "brain_reason": "Present in fund-engine simulation but absent from live-safe brain universe.",
                    "fund_weight": round(cw, 4),
                }
            )

    rotate_in = [a for a in allocations if a.get("brain_action") == "ROTATE_IN"]
    top_priorities = allocations[:10]

    fam_w: dict[str, float] = {}
    for a in allocations:
        fam = str(a.get("family", "UNKNOWN"))
        fam_w[fam] = fam_w.get(fam, 0.0) + float(a.get("weight", 0.0))

    family_concentration: list[dict[str, Any]] = []
    for fam, share in sorted(fam_w.items(), key=lambda x: -x[1]):
        family_concentration.append(
            {
                "family": fam,
                "weight_share": round(share, 4),
                "capital_percent_sum": round(
                    share * allocable_percent,
                    2,
                ),
                "within_target": share <= FAMILY_WEIGHT_SOFT_CAP + 0.06,
            }
        )

    shifts = _build_capital_shift_recommendations(allocations, fund_by_id)

    brain: dict[str, Any] = {
        "top_priorities": [
            {
                "strategy_id": x.get("strategy_id"),
                "family": x.get("family"),
                "priority_rank": x.get("priority_rank"),
                "priority_score": x.get("priority_score"),
                "capital_percent": x.get("capital_percent"),
                "brain_action": x.get("brain_action"),
                "brain_reason": x.get("brain_reason"),
            }
            for x in top_priorities
        ],
        "rotate_in": [
            {
                "strategy_id": x.get("strategy_id"),
                "family": x.get("family"),
                "weight": x.get("weight"),
                "brain_reason": x.get("brain_reason"),
            }
            for x in rotate_in
        ],
        "rotate_out": rotate_out,
        "family_concentration": family_concentration,
        "capital_shift_recommendations": shifts,
        "parameters": {
            "correlation_penalty": CORRELATION_PENALTY,
            "same_family_corr_proxy": SAME_FAMILY_CORR,
            "family_soft_cap": FAMILY_WEIGHT_SOFT_CAP,
        },
        "decision_layer_only": True,
    }

    return {
        "count": len(allocations_by_capital),
        "allocations": allocations_by_capital,
        "total_allocated_percent": total_alloc,
        "brain": brain,
        "decision_layer_only": True,
    }


def _empty_brain() -> dict[str, Any]:
    return {
        "top_priorities": [],
        "rotate_in": [],
        "rotate_out": [],
        "family_concentration": [],
        "capital_shift_recommendations": [],
        "parameters": {},
        "decision_layer_only": True,
    }
