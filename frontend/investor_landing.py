"""
Institutional investor landing — read-only presentation. No trading or admin controls.

Browser tab title/icon: configured once in frontend/dashboard.py (st.set_page_config).
"""

from __future__ import annotations

import html
from typing import Any, Callable

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
    footer_html,
)

FetchJson = Callable[[str], tuple[dict[str, Any] | None, str | None]]

_CSS = f"""
<style>
    .il-wrap {{ max-width: 920px; margin: 0 auto; padding-bottom: 2rem; }}
    .il-hero {{
        background: linear-gradient(165deg, {BG} 0%, #12151c 50%, #0a0c10 100%);
        border: 1px solid {CARD_BORDER};
        border-radius: 14px;
        padding: 1.75rem 1.5rem 1.75rem 1.5rem;
        margin-bottom: 2rem;
        box-shadow: 0 16px 48px rgba(0,0,0,0.45);
    }}
    .il-hero-row {{ display: flex; align-items: flex-start; gap: 1.25rem; flex-wrap: wrap; }}
    .il-hero-right {{ flex: 1; min-width: 200px; }}
    .il-kicker {{
        text-transform: uppercase;
        letter-spacing: 0.16em;
        font-size: 0.62rem;
        color: {BLUE};
        font-weight: 600;
        margin-bottom: 0.5rem;
        font-family: 'Inter', system-ui, sans-serif;
    }}
    .il-brand-line {{
        font-family: 'Inter', system-ui, sans-serif;
        font-weight: 700;
        font-size: 0.95rem;
        color: {GOLD};
        letter-spacing: 0.12em;
        text-transform: uppercase;
        margin-bottom: 0.35rem;
    }}
    .il-title {{
        font-family: 'Inter', system-ui, sans-serif;
        font-weight: 600;
        font-size: 1.15rem;
        letter-spacing: -0.02em;
        color: {TEXT};
        line-height: 1.35;
        margin: 0 0 0.65rem 0;
    }}
    .il-sub {{
        font-size: 0.92rem;
        color: {MUTED};
        line-height: 1.55;
        max-width: 40rem;
        margin-bottom: 1rem;
    }}
    .il-statement {{
        font-size: 0.88rem;
        color: {TEXT};
        line-height: 1.6;
        border-left: 2px solid {GOLD};
        padding-left: 1rem;
        margin: 0.75rem 0 0 0;
        opacity: 0.95;
    }}
    .il-section {{
        border-left: 2px solid {GOLD};
        padding-left: 1rem;
        margin: 2.25rem 0 1rem 0;
        padding-top: 0.15rem;
    }}
    .il-section h2 {{
        color: {TEXT};
        font-weight: 700;
        font-size: 0.82rem;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        margin: 0 0 0.65rem 0;
        font-family: 'Inter', system-ui, sans-serif;
    }}
    .il-muted {{ color: {MUTED}; font-size: 0.88rem; line-height: 1.55; }}
    .il-step {{
        background: #12151c;
        border: 1px solid {CARD_BORDER};
        border-radius: 8px;
        padding: 0.85rem 1rem;
        margin-bottom: 0.5rem;
        color: {TEXT};
        font-size: 0.88rem;
        font-family: 'Inter', system-ui, sans-serif;
    }}
    .il-step-num {{ color: {GOLD}; font-weight: 700; margin-right: 0.5rem; }}
    div[data-testid="stMetric"] {{
        background-color: #12151c !important;
        border: 1px solid {CARD_BORDER} !important;
        border-radius: 8px !important;
    }}
    div[data-testid="stMetric"] label {{ color: {MUTED} !important; }}
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {{ color: {TEXT} !important; }}
</style>
"""


def _go_experience(page: str) -> None:
    st.session_state.experience_page = page
    st.rerun()


def render_investor_landing(fetch_json: FetchJson) -> None:
    st.markdown(app_shell_css(), unsafe_allow_html=True)
    st.markdown(_CSS, unsafe_allow_html=True)

    pipe, _ = fetch_json("/data/pipeline/validation")
    snap, _ = fetch_json("/research/investor_snapshot")
    report, _ = fetch_json("/report/summary")
    demo_api, _ = fetch_json("/demo/status")

    meta = (pipe or {}).get("historical_meta") or {} if pipe else {}
    syms = meta.get("symbols_included") or []
    years = meta.get("years_approx")
    no_synth = (pipe or {}).get("no_synthetic_history_enforced") if pipe else None
    pres = (snap or {}).get("presentation") or {} if snap and snap.get("available") else {}
    mix = pres.get("family_mix_evaluated") or {}
    n_families = len(mix) if isinstance(mix, dict) and mix else None
    eval_ct = (snap or {}).get("evaluated_count") if snap and snap.get("available") else None
    if eval_ct is None and report:
        eval_ct = report.get("total_strategies")
    demo_metric = "—"
    if snap and snap.get("available") and snap.get("demo_only") is not None:
        demo_metric = (
            "Research: demo-oriented" if snap.get("demo_only") else "Research: not demo-only"
        )
    elif demo_api:
        demo_metric = f"Layer: {demo_api.get('status', '—')}"

    st.markdown('<div class="il-wrap">', unsafe_allow_html=True)

    # --- A. Hero: official logo (full wordmark) + tagline only ---
    st.markdown('<p class="il-kicker">Institutional overview</p>', unsafe_allow_html=True)
    if LOGO_PATH.is_file():
        st.image(str(LOGO_PATH), width=320)
    st.markdown(
        f'<p class="il-title" style="margin-top:0.5rem;">{html.escape(BRAND_SUBTITLE)}</p>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="il-hero">'
        '<p class="il-sub">A research-first systematic stack for multi-asset evaluation, portfolio discipline, '
        "and allocator-grade transparency — not a retail trading product.</p>"
        "<p class=\"il-statement\">Built for <b>serious capital allocators</b>: measured language, explicit data and "
        "methodology context, and clear separation between <b>research replay</b>, <b>demo / paper validation</b>, "
        "and any live policy your deployment chooses to enable.</p>"
        "</div>",
        unsafe_allow_html=True,
    )

    c1, c2, c3 = st.columns(3)
    if c1.button("View Investor Dashboard", use_container_width=True, key="il_cta_public_inv"):
        _go_experience("Investor Dashboard")
    if c2.button("Private Investor Access", use_container_width=True, type="primary", key="il_cta_priv_inv"):
        _go_experience("Investor (private)")
    if c3.button("Partner Access", use_container_width=True, key="il_cta_partner"):
        _go_experience("Partner (private)")
    st.caption("Public dashboard is unauthenticated. Private and Partner views require passwords configured on the server.")

    # --- B. Executive snapshot ---
    st.markdown('<div class="il-section"><h2>Executive snapshot</h2></div>', unsafe_allow_html=True)
    st.markdown(
        '<p class="il-muted">Live figures from the API when available; otherwise shown as unavailable.</p>',
        unsafe_allow_html=True,
    )
    m1, m2, m3 = st.columns(3)
    m4, m5, m6 = st.columns(3)
    m1.metric("Years of history (approx.)", f"{years}" if years is not None else "—")
    m2.metric("Symbols in panel", str(len(syms)) if syms else "—")
    m3.metric("Strategy families (last research)", str(n_families) if n_families else "—")
    m4.metric("Strategies evaluated / registry", str(eval_ct) if eval_ct is not None else "—")
    m5.metric("Real-data-only (API flag)", "On" if no_synth else "Off" if no_synth is not None else "—")
    m6.metric("Demo / paper emphasis", demo_metric)

    # Why credible + different (combined institutional blocks)
    st.markdown('<div class="il-section"><h2>Why this is credible</h2></div>', unsafe_allow_html=True)
    st.markdown(
        "- **Research, not promises:** metrics are framed as **historical replay** and model constructs — not forward guarantees.\n"
        "- **Transparent limitations:** proxy instruments, daily bars, and basket construction are acknowledged where applicable.\n"
        "- **Risk-aware book design:** diversification, correlation, and drawdown language are central — not raw return hype.\n"
        "- **Access hygiene:** public vs private vs internal roles are separated; no execution from allocator-facing pages.\n"
    )

    st.markdown('<div class="il-section"><h2>Why AlgoSphere is different</h2></div>', unsafe_allow_html=True)
    cols = st.columns(2)
    with cols[0]:
        st.markdown(
            "**Multi-family strategy research** — systematic coverage across strategy families, not a single-signal toy.\n\n"
            "**Diversification-first construction** — portfolio and quota logic emphasize balance over concentration.\n\n"
            "**Risk-managed evaluation** — drawdown, stability, and correlation enter the research composite."
        )
    with cols[1]:
        st.markdown(
            "**Real historical data + live extension** — long history for replay; optional live merge for research context (deployment-dependent).\n\n"
            "**Transparent limitations** — data source and replay constraints are stated, not hidden.\n\n"
            "**Institutional presentation layer** — allocator-ready narrative; not a crypto-style dashboard."
        )

    # --- D. Research process ---
    st.markdown('<div class="il-section"><h2>Research process</h2></div>', unsafe_allow_html=True)
    steps = [
        "Real data collection — multi-symbol daily history and merged panels (vendor-dependent).",
        "Strategy generation — broad candidate sets across families for robust comparison.",
        "Multi-family evaluation — scoring that rewards discipline, not single-metric chasing.",
        "Drawdown / stability / correlation filters — screens aligned with risk-aware allocation.",
        "Portfolio construction — diversified books with caps and correlation awareness.",
        "Demo / paper validation — primary path for operational proof before any live policy.",
    ]
    for i, text in enumerate(steps, 1):
        st.markdown(
            f'<div class="il-step"><span class="il-step-num">{i}.</span>{html.escape(text)}</div>',
            unsafe_allow_html=True,
        )

    # --- E. Philosophy ---
    st.markdown('<div class="il-section"><h2>Portfolio &amp; risk philosophy</h2></div>', unsafe_allow_html=True)
    st.markdown(
        "- **Diversification** across families and symbols to reduce single-factor dominance.\n"
        "- **Family caps / quotas** to avoid over-concentration in one style bucket.\n"
        "- **Correlation control** in research construction — similar strategies are not free substitutes.\n"
        "- **Drawdown awareness** — expected drawdown proxies and stability enter evaluation, not tail-risk denial.\n"
        "- **Capital preservation mindset** — defensive posture is a first-class research conversation.\n"
        "- **Demo-first approach** — paper and demo workflows are the default validation story unless you explicitly enable otherwise.\n"
    )

    # --- F. Transparency ---
    st.markdown('<div class="il-section"><h2>Transparency &amp; limitations</h2></div>', unsafe_allow_html=True)
    st.markdown(
        '<p class="il-muted"><b>Research, not promises</b> — historical replay and backtest-style metrics do not guarantee future results.</p>',
        unsafe_allow_html=True,
    )
    st.markdown(
        "- Where applicable, the platform is **research- and demo-oriented**; **live trading** is **not** implied by allocator pages.\n"
        "- **Demo / paper** validation is the primary narrative unless your deployment explicitly documents otherwise.\n"
        "- **Proxy data** (e.g. vendor symbols vs. broker feeds) and **basket / equal-weight** constructs may diverge from executable portfolios.\n"
        "- **No misleading promises** — no guaranteed returns; market risk is material.\n"
    )

    # --- G. Access structure ---
    st.markdown('<div class="il-section"><h2>Access structure</h2></div>', unsafe_allow_html=True)
    ac1, ac2, ac3 = st.columns(3)
    with ac1:
        with st.container(border=True):
            st.markdown("**Investor**")
            st.caption("Public + password-gated private")
            st.markdown(
                "For allocators, LPs, and prospects. **Read-only** institutional summary: credibility, portfolio context, risk controls, limitations. **Investor (private)** adds a server password."
            )
    with ac2:
        with st.container(border=True):
            st.markdown("**Partner**")
            st.caption("Private · deeper diligence")
            st.markdown(
                "Same institutional base **plus** deeper **read-only** analytics: regime, meta, control signals snapshot, paper layer, performance samples — HTTP GET only, no execution."
            )
    with ac3:
        with st.container(border=True):
            st.markdown("**Admin**")
            st.caption("Internal only")
            st.markdown(
                "Operator cockpit and engineering surfaces. **Not** for external marketing; keep credentials separate from allocator access."
            )

    # --- H. Vision ---
    st.markdown('<div class="il-section"><h2>Future vision</h2></div>', unsafe_allow_html=True)
    st.markdown(
        "AlgoSphere is designed to scale as **research infrastructure**: AI-assisted allocation workflows, "
        "expanded multi-asset coverage where data quality supports it, and a **presentation layer** that stays "
        "credible under institutional scrutiny. The aim is a **long-term capital platform** posture — disciplined "
        "iteration, transparent limitations, and governance-friendly design — without overclaiming near-term outcomes."
    )

    st.markdown(footer_html(), unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)
