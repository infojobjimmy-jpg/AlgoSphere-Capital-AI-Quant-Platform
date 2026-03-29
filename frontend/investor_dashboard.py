"""
Investor-facing presentation layer: read-only, institutional tone, no execution controls.

Tab favicon/title: set at app entry in frontend/dashboard.py (global for all views).
"""

from __future__ import annotations

import html
from typing import Any, Callable

import pandas as pd
import streamlit as st

from frontend.brand_theme import (
    BG,
    BLUE,
    BRAND_SUBTITLE,
    CARD_BORDER,
    GOLD,
    LOGO_PATH,
    MUTED,
    TEXT,
    app_shell_css,
    brand_header_streamlit,
    footer_html,
)

FetchJson = Callable[[str], tuple[dict[str, Any] | None, str | None]]

_INVESTOR_CSS = f"""
<style>
    .inv-hero-slim {{
        background: linear-gradient(160deg, {BG} 0%, #12151c 100%);
        border: 1px solid {CARD_BORDER};
        border-radius: 12px;
        padding: 1.1rem 1.25rem;
        margin-bottom: 1.5rem;
    }}
    .inv-hero-slim .sub {{
        color: {MUTED};
        font-size: 0.9rem;
        line-height: 1.5;
        max-width: 52rem;
        margin: 0;
        font-family: 'Inter', system-ui, sans-serif;
    }}
    .inv-kicker {{
        text-transform: uppercase;
        letter-spacing: 0.14em;
        font-size: 0.62rem;
        color: {BLUE};
        font-weight: 600;
        margin-bottom: 0.45rem;
        font-family: 'Inter', system-ui, sans-serif;
    }}
    .inv-section {{
        border-left: 2px solid {GOLD};
        padding-left: 1rem;
        margin: 1.75rem 0 1rem 0;
    }}
    .inv-section h2 {{
        color: {TEXT};
        font-weight: 700;
        font-size: 0.82rem;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        margin: 0 0 0.5rem 0;
        font-family: 'Inter', system-ui, sans-serif;
    }}
    .inv-muted {{ color: {MUTED}; font-size: 0.88rem; line-height: 1.55; }}
    div[data-testid="stMetric"] {{
        background-color: #12151c !important;
        border: 1px solid {CARD_BORDER} !important;
        border-radius: 8px !important;
        padding: 0.65rem 0.5rem !important;
    }}
    div[data-testid="stMetric"] label {{ color: {MUTED} !important; }}
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {{ color: {TEXT} !important; }}
</style>
"""


def _fmt_num(x: Any, digits: int = 2, default: str = "—") -> str:
    try:
        if x is None:
            return default
        return str(round(float(x), digits))
    except (TypeError, ValueError):
        return default


def _render_partner_analytics(fetch_json: FetchJson) -> None:
    """Extra read-only context for Partner (private) — no debug routes or controls."""
    st.markdown(
        '<div class="inv-section"><h2>Partner analytics (read-only)</h2></div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<p class="inv-muted">Operational and book-level context for diligence. '
        "No execution, promotion, or engineering surfaces.</p>",
        unsafe_allow_html=True,
    )

    regime, re = fetch_json("/regime/status")
    meta, me = fetch_json("/meta/status")
    ctrl, ce = fetch_json("/control/signals")
    paper, pe = fetch_json("/paper/status")
    lsv, le = fetch_json("/live_safe/status")
    perf_sys, pse = fetch_json("/performance/system")
    perf_top, pte = fetch_json("/performance/top")
    factory, fe = fetch_json("/factory/top?limit=15")
    alerts, ae = fetch_json("/alerts/summary")

    if regime and not re:
        st.markdown("**Market regime**")
        c1, c2 = st.columns(2)
        c1.metric("current_regime", str(regime.get("current_regime", "—")))
        c2.metric("confidence", f"{float(regime.get('confidence_score', 0.0)):.3f}")
    elif re:
        st.caption(f"Regime: {re}")

    if meta and not me:
        st.markdown("**System / meta posture**")
        m1, m2, m3 = st.columns(3)
        m1.metric("system_posture", str(meta.get("system_posture", "—")))
        m2.metric("system_health", str(meta.get("system_health", "—")))
        m3.metric("risk_mode", str(meta.get("risk_mode", "—")))

    if ctrl and not ce:
        st.markdown("**Control signals (snapshot)**")
        signals = ctrl.get("signals") or []
        by_state: dict[str, int] = {}
        for s in signals:
            cs = str(s.get("control_state") or "")
            if cs:
                by_state[cs] = by_state.get(cs, 0) + 1
        k1, k2 = st.columns(2)
        k1.metric("signals_tracked", int(ctrl.get("count", len(signals)) or 0))
        k2.metric("active_controls", len([s for s in signals if bool(s.get("control_active"))]))
        if by_state:
            st.dataframe(
                pd.DataFrame([{"control_state": k, "count": v} for k, v in sorted(by_state.items())]),
                use_container_width=True,
                hide_index=True,
            )
        rows = []
        for s in signals[:40]:
            rs = s.get("reasoning") or []
            rows.append(
                {
                    "name": s.get("name"),
                    "control_state": s.get("control_state"),
                    "active": s.get("control_active"),
                    "recommended_action": s.get("recommended_action"),
                    "reasoning_preview": " | ".join(str(x) for x in rs[:2]),
                }
            )
        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    if paper and not pe:
        st.markdown("**Paper / simulation layer**")
        st.json({k: paper.get(k) for k in list(paper.keys())[:14]})

    if lsv and not le:
        st.markdown("**Live-safe review posture**")
        st.json({k: lsv.get(k) for k in ("state_counts", "summary", "notes") if k in lsv} or lsv)

    if perf_sys and not pse:
        st.markdown("**Performance system (aggregate)**")
        p1, p2, p3 = st.columns(3)
        p1.metric("runner_success_rate", f"{float(perf_sys.get('runner_success_rate', 0.0)):.2%}")
        p2.metric("runner_fail_rate", f"{float(perf_sys.get('runner_fail_rate', 0.0)):.2%}")
        p3.metric("pipeline_throughput", f"{float(perf_sys.get('pipeline_throughput', 0.0)):.2f}")

    if perf_top and not pte:
        items = perf_top.get("strategies") or []
        if items:
            st.markdown("**Strategy performance (top sample)**")
            st.dataframe(pd.DataFrame(items[:30]), use_container_width=True, hide_index=True)

    if factory and not fe:
        top_items = factory.get("top", []) or []
        if top_items:
            st.markdown("**Factory book (top candidates)**")
            st.dataframe(pd.DataFrame(top_items), use_container_width=True, hide_index=True)

    if alerts and not ae:
        st.markdown("**Alerts summary**")
        st.json(alerts if isinstance(alerts, dict) else {"alerts": alerts})


def render_investor_dashboard(
    fetch_json: FetchJson,
    *,
    access_label: str | None = None,
    partner_extensions: bool = False,
) -> None:
    st.markdown(app_shell_css(), unsafe_allow_html=True)
    st.markdown(_INVESTOR_CSS, unsafe_allow_html=True)
    brand_header_streamlit(LOGO_PATH, partner_mode=partner_extensions)

    kicker = "Institutional presentation"
    if access_label:
        kicker = f"{access_label} · read-only"

    pipe, pipe_err = fetch_json("/data/pipeline/validation")
    live, live_err = fetch_json("/live/status")
    report, _ = fetch_json("/report/summary")
    alloc, alloc_err = fetch_json("/portfolio/allocation")
    risk, risk_err = fetch_json("/risk/status")
    demo, _ = fetch_json("/demo/status")
    snap, _ = fetch_json("/research/investor_snapshot")

    st.markdown(
        f'<div class="inv-hero-slim"><div class="inv-kicker">{html.escape(kicker)}</div>'
        f'<p class="sub"><b>{html.escape(BRAND_SUBTITLE)}</b> — Multi-family systematic research and risk-managed '
        "portfolio evaluation. Decision-support and transparency for allocator due diligence — not a retail product.</p>"
        "</div>",
        unsafe_allow_html=True,
    )

    # --- One-screen executive summary ---
    st.markdown('<div class="inv-section"><h2>Executive summary</h2></div>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    meta = (pipe or {}).get("historical_meta") or {} if pipe else {}
    syms = meta.get("symbols_included") or []
    years = meta.get("years_approx")
    eval_ct = (snap or {}).get("evaluated_count") if snap and snap.get("available") else (report or {}).get(
        "total_strategies"
    )
    no_synth = (pipe or {}).get("no_synthetic_history_enforced") if pipe else None
    live_test = (live or {}).get("live_testing_mode") if live else None

    mode_parts = []
    mode_parts.append("Research / evaluation platform")
    if live_test:
        mode_parts.append("live data merge enabled for research")
    mode_parts.append("not a live brokerage execution stack in this view")
    mode_line = " · ".join(mode_parts)

    c1.metric("Data span (approx.)", f"{years} y" if years is not None else "—")
    c2.metric("Symbols in history", len(syms) if syms else "—")
    c3.metric("Strategies in registry (report)", report.get("total_strategies", "—") if report else "—")
    c4.metric("Research run evaluated", eval_ct if eval_ct is not None else "—")

    st.markdown(
        f'<p class="inv-muted"><b>Operating posture:</b> {mode_line}. '
        f"<b>Real-data-only flag:</b> {'on' if no_synth else 'off' if no_synth is not None else 'unknown'} "
        "(API host configuration).</p>",
        unsafe_allow_html=True,
    )

    if snap and snap.get("available"):
        hm = snap.get("history_meta") or {}
        ds, de = hm.get("date_start"), hm.get("date_end")
        st.markdown(
            '<p class="inv-muted"><b>Latest research snapshot</b> (read-only file on server): '
            f"mode <code>{snap.get('ranking_mode', '—')}</code> · "
            f"bars <code>{snap.get('bars_used', '—')}</code> · "
            f"evaluated <code>{snap.get('evaluated_count', '—')}</code> · "
            f"demo flag <code>{snap.get('demo_only')}</code> · "
            f"no live trading flag <code>{snap.get('no_live_trading')}</code>"
            + (f" · data window <code>{ds}</code> → <code>{de}</code>" if ds and de else "")
            + "</p>",
            unsafe_allow_html=True,
        )

    if pipe_err:
        st.warning(f"Pipeline API: {pipe_err}")
    if live_err:
        st.caption(f"Live status: {live_err}")

    st.markdown("**Why this is credible**")
    st.markdown(
        "- Transparent **historical replay** metrics with explicit **limitations** (no implied forward guarantee).\n"
        "- **Diversification**, **drawdown-aware** scoring, and **correlation** discipline in the research book.\n"
        "- **Paper / demo** safeguards called out; **no exaggerated performance** language in this view.\n"
        "- **Read-only** investor mode: no execution, promotion, or engineering controls exposed."
    )

    # --- B. Portfolio overview (allocation brain + snapshot book) ---
    st.markdown('<div class="inv-section"><h2>Portfolio overview</h2></div>', unsafe_allow_html=True)
    if alloc_err or not alloc:
        st.info("Allocation intelligence unavailable from API. Check connectivity.")
    else:
        count = alloc.get("count", 0)
        st.markdown(
            f'<p class="inv-muted">Decision-layer allocation view over **{count}** live-safe candidates '
            "(simulation / review states — not deployed capital).</p>",
            unsafe_allow_html=True,
        )
        brain = alloc.get("brain") or {}
        fam = brain.get("family_concentration") or []
        if fam:
            st.markdown("**Family distribution (decision layer)**")
            st.dataframe(pd.DataFrame(fam), use_container_width=True, hide_index=True)
        tops = brain.get("top_priorities") or []
        if tops:
            st.markdown("**Current priority sleeves (top of book)**")
            st.dataframe(pd.DataFrame(tops), use_container_width=True, hide_index=True)

    div_pf = (snap or {}).get("diversified_portfolio") if snap and snap.get("available") else None
    if div_pf and isinstance(div_pf, list):
        rows = []
        for row in div_pf:
            bt = row.get("backtest") or {}
            rows.append(
                {
                    "strategy_id": (row.get("strategy_id") or "")[:12] + "…",
                    "family": row.get("family"),
                    "weight": row.get("weight"),
                    "research_composite": row.get("fitness_score"),
                    "replay_total_return": bt.get("total_return"),
                    "max_dd_pct": bt.get("max_drawdown_pct"),
                    "stability": bt.get("stability"),
                }
            )
        st.markdown("**Diversified systematic portfolio (last research book)**")
        st.caption("Weights from internal risk-parity-style construction — research artifact, not live allocation.")
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    rp_snap = (snap or {}).get("risk_profile") or {} if snap and snap.get("available") else {}
    if rp_snap:
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Diversification score", _fmt_num(rp_snap.get("diversification_score"), 2))
        m2.metric("Avg |pairwise corr.|", _fmt_num(rp_snap.get("avg_pairwise_abs_correlation"), 4))
        m3.metric("Expected DD (mean, model scale)", _fmt_num(rp_snap.get("aggregate_expected_drawdown_mean"), 2))
        qm = rp_snap.get("quotas_met")
        m4.metric("Quotas met", "Yes" if qm else "No" if qm is not None else "—")

    pres = (snap or {}).get("presentation") or {} if snap and snap.get("available") else {}
    if pres.get("growth_weighted_return_proxy") is not None or pres.get("demo_safe_weighted_return_proxy") is not None:
        st.markdown("**Replay return proxies (research book — not forecasts)**")
        g1, g2 = st.columns(2)
        g1.metric("Growth book (weighted replay proxy)", _fmt_num(pres.get("growth_weighted_return_proxy"), 4))
        g2.metric("Demo-safe book (weighted replay proxy)", _fmt_num(pres.get("demo_safe_weighted_return_proxy"), 4))

    if risk and not risk_err:
        st.markdown("**Global risk posture (system)**")
        preferred = (
            "global_risk_score",
            "risk_level",
            "decision_layer_only",
            "demo_simulation_only",
            "recommendations",
            "components",
        )
        slim = {k: risk.get(k) for k in preferred if k in risk}
        st.json(slim if slim else dict(list(risk.items())[:10]))

    # --- C. Research credibility ---
    st.markdown('<div class="inv-section"><h2>Research credibility</h2></div>', unsafe_allow_html=True)
    st.markdown(
        '<p class="inv-muted">Methodology emphasizes <b>diversification</b>, <b>drawdown control</b>, '
        "<b>stability</b>, and <b>regime adaptability</b> in the research composite — not raw return maximization.</p>",
        unsafe_allow_html=True,
    )
    if pipe:
        st.markdown(
            f"- **Historical bars (basket):** {(pipe.get('total_bars_historical_basket'))}\n"
            f"- **Merged panel rows:** {pipe.get('total_bars_merged_panel')}\n"
            f"- **Merged dataset ready:** {'yes' if pipe.get('merged_dataset_ready') else 'no'}\n"
            f"- **Live extension rows:** {pipe.get('total_bars_live_extension')}\n"
        )
        yps = pipe.get("years_available_per_symbol") or {}
        if yps:
            st.markdown("**Years available per symbol (approx.)**")
            st.dataframe(
                pd.DataFrame([{"symbol": k, "years_approx": v} for k, v in yps.items()]),
                use_container_width=True,
                hide_index=True,
            )
        src = pipe.get("data_sources_used")
        if isinstance(src, dict):
            st.markdown("**Data sources (historical)**")
            st.dataframe(
                pd.DataFrame([{"symbol": k, "source": v} for k, v in src.items()]),
                use_container_width=True,
                hide_index=True,
            )

    mix = pres.get("family_mix_evaluated") if pres else None
    if mix:
        st.markdown(f"**Strategy families in last research run:** {len(mix)} buckets (balanced generation).")
        st.dataframe(
            pd.DataFrame([{"family": k, "candidates": v} for k, v in mix.items()]),
            use_container_width=True,
            hide_index=True,
        )

    if not snap or not snap.get("available"):
        st.caption(snap.get("message", "Run `scripts/client_research_report.py` to populate deep research tables."))

    # --- D. Risk controls ---
    st.markdown('<div class="inv-section"><h2>Risk controls</h2></div>', unsafe_allow_html=True)
    st.markdown(
        "- **No live trading** from this dashboard; investor mode is **read-only**.\n"
        "- **Paper / demo** workflows are the primary validation path unless your deployment explicitly enables otherwise.\n"
        "- **Real-data-only** can be enforced server-side (`ALGO_SPHERE_NO_SYNTHETIC_HISTORY`); status shown in pipeline API.\n"
        "- **Family caps / quotas** and **correlation** limits apply in diversified portfolio construction (see research report when present).\n"
    )
    if demo:
        st.markdown("**Demo layer (API)**")
        st.json({k: demo.get(k) for k in ("status", "queue_depth", "paused") if k in demo} or demo)

    ps = (snap or {}).get("portfolio_selection") if snap and snap.get("available") else None
    if ps:
        st.markdown("**Last research portfolio selection (quotas / caps)**")
        st.json(ps)

    # --- E. Strategy insights ---
    st.markdown('<div class="inv-section"><h2>Strategy insights</h2></div>', unsafe_allow_html=True)
    if pres:
        safe5 = pres.get("top_5_safest_candidates") or []
        gr5 = pres.get("top_5_strongest_growth_candidates") or []
        if safe5:
            st.markdown("**Top 5 lowest drawdown proxy candidates (research)**")
            st.dataframe(pd.DataFrame(safe5), use_container_width=True, hide_index=True)
        if gr5:
            st.markdown("**Top 5 replay return leaders (research — higher risk)**")
            st.dataframe(pd.DataFrame(gr5), use_container_width=True, hide_index=True)
        dsafe = pres.get("demo_safe_portfolio") or []
        gpf = pres.get("growth_portfolio") or []
        if dsafe:
            st.markdown("**Presentation demo-safe book**")
            st.dataframe(pd.DataFrame(dsafe), use_container_width=True, hide_index=True)
        if gpf:
            st.markdown("**Presentation growth-oriented book**")
            st.dataframe(pd.DataFrame(gpf), use_container_width=True, hide_index=True)
        verdict = (pres.get("client_demo_verdict") or {})
        if verdict:
            st.markdown(
                f"**Demo readiness heuristic:** `{verdict.get('demo_paper_go_no_go', '—')}` "
                f"(internal screen, not an investment recommendation)."
            )
    if report and report.get("top_5_strategies"):
        st.markdown("**Factory top strategies (registry snapshot)**")
        st.dataframe(pd.DataFrame(report["top_5_strategies"]), use_container_width=True, hide_index=True)

    # --- F. Limitations ---
    st.markdown('<div class="inv-section"><h2>Limitations & transparency</h2></div>', unsafe_allow_html=True)
    st.markdown(
        "- **Research and historical replay are not guarantees** of future results.\n"
        "- **Proxy instruments** (e.g. vendor feeds vs. your LP) may diverge from executable markets.\n"
        "- **Daily** history does not capture full **intraday** microstructure.\n"
        "- Metrics are **model constructs** for discipline and comparison, not audited performance claims.\n"
    )
    lim = (snap or {}).get("research_limitations") or [] if snap else []
    if lim:
        st.markdown("**Deployment-reported limitations**")
        for line in lim:
            st.markdown(f"- {line}")
    meta_lim = meta.get("limitations") if meta else None
    if meta_lim:
        st.markdown("**Historical data limitations (meta)**")
        for line in meta_lim:
            st.markdown(f"- {line}")

    if partner_extensions:
        _render_partner_analytics(fetch_json)

    st.divider()
    st.markdown(footer_html(), unsafe_allow_html=True)
