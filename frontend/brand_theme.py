"""
AlgoSphere Capital — Concept 1 Institutional Minimal (UI constants only).
"""

from __future__ import annotations

import html
from pathlib import Path

_FRONTEND = Path(__file__).resolve().parent
ASSETS_DIR = _FRONTEND / "assets"
LOGO_PATH = ASSETS_DIR / "logo.png"
# Primary tab icon (see scripts/build_favicon_assets.py)
FAVICON_32_PATH = ASSETS_DIR / "favicon-32.png"
FAVICON_PATH = FAVICON_32_PATH

BRAND_NAME = "AlgoSphere Capital"
BRAND_SUBTITLE = "AI-Driven Quantitative Investment Platform"
PAGE_TITLE = "AlgoSphere Capital"

# Palette
BG = "#0D0F12"
GOLD = "#C9A227"
BLUE = "#2F5BFF"
TEXT = "#E6E8EB"
MUTED = "#8A8F98"
CARD_BORDER = "#2A303C"
SECTION_GOLD = "rgba(201, 162, 39, 0.35)"

FOOTER_LINES = (
    "AlgoSphere Capital",
    "Institutional AI Research Platform",
    "Confidential – For Investor Presentation Only",
)


def footer_html() -> str:
    lines = [
        f'<div style="color:{MUTED};font-size:0.78rem;line-height:1.55;">{html.escape(l)}</div>'
        for l in FOOTER_LINES
    ]
    return (
        f'<div style="margin-top:2.5rem;padding-top:1.25rem;border-top:1px solid {CARD_BORDER};text-align:center;">'
        + "".join(lines)
        + "</div>"
    )


def app_shell_css() -> str:
    """Full-bleed dark shell for allocator pages."""
    return f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    html, body, [data-testid="stAppViewContainer"] {{
        background-color: {BG} !important;
    }}
    [data-testid="stHeader"] {{
        background-color: {BG} !important;
        border-bottom: 1px solid {CARD_BORDER};
    }}
    [data-testid="stSidebar"] {{
        background-color: #0a0c0f !important;
    }}
    .stApp {{
        font-family: 'Inter', system-ui, -apple-system, 'Segoe UI', sans-serif;
        color: {TEXT};
    }}
    #MainMenu {{visibility: hidden;}}
    footer {{visibility: hidden;}}
</style>
"""


def brand_header_streamlit(logo_path: Path, *, partner_mode: bool = False) -> None:
    """Official wordmark logo (includes ALGOSPHERE CAPITAL) + subtitle only — no duplicate name."""
    import streamlit as st

    sub = BRAND_SUBTITLE
    if partner_mode:
        sub = f"{BRAND_SUBTITLE} · Partner view (read-only)"
    if logo_path.is_file():
        st.image(str(logo_path), width=300)
    else:
        st.caption("◉")
    st.markdown(
        f'<p style="margin:0.35rem 0 0 0;padding:0;font-size:0.92rem;color:{TEXT};font-weight:500;'
        f"font-family:'Inter',system-ui,sans-serif;">{html.escape(sub)}</p>",
        unsafe_allow_html=True,
    )
