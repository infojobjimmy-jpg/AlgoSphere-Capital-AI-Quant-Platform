"""
Streamlit-only deploy flags (e.g. Render public allocator surface). No trading impact.
"""

from __future__ import annotations

import os

# Pages allowed when ALGOSPHERE_PUBLIC_SURFACE is enabled (no Admin / Client / retail landing).
PUBLIC_ALLOCATOR_EXPERIENCE_OPTIONS: tuple[str, ...] = (
    "Investor Landing",
    "Investor Dashboard",
    "Investor (private)",
    "Partner (private)",
)


def is_public_allocator_deploy() -> bool:
    v = os.getenv("ALGOSPHERE_PUBLIC_SURFACE", "").strip().lower()
    return v in ("investor", "allocator", "1", "true", "yes")
