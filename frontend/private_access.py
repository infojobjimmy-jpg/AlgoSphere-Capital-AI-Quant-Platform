"""
Streamlit-only private access gate (investor / partner passwords from environment).
Does not modify trading or API execution behavior.
"""

from __future__ import annotations

import os
import secrets
from typing import Literal

import streamlit as st

PrivateRole = Literal["investor", "partner"]

_SESSION_KEYS: dict[PrivateRole, str] = {
    "investor": "algosphere_private_investor_ok",
    "partner": "algosphere_private_partner_ok",
}

_ENV_KEYS: dict[PrivateRole, str] = {
    "investor": "ALGOSPHERE_PRIVATE_INVESTOR_PASSWORD",
    "partner": "ALGOSPHERE_PRIVATE_PARTNER_PASSWORD",
}


def _expected_password(role: PrivateRole) -> str | None:
    raw = os.getenv(_ENV_KEYS[role], "").strip()
    if raw:
        return raw
    # Local/demo fallback — set env in any shared or production deployment.
    return "demo-investor" if role == "investor" else "demo-partner"


def using_demo_password(role: PrivateRole) -> bool:
    return not os.getenv(_ENV_KEYS[role], "").strip()


def ensure_private_access(role: PrivateRole) -> bool:
    """
    If session is authorized, render sidebar sign-out and return True.
    Otherwise render login and stop the app run (no content below).
    """
    sk = _SESSION_KEYS[role]
    if st.session_state.get(sk):
        label = "Investor" if role == "investor" else "Partner"
        st.sidebar.caption(f"Signed in · {label} (read-only)")
        if using_demo_password(role):
            st.sidebar.warning(
                f"Using built-in demo password. Set {_ENV_KEYS[role]} in `.env` for production.",
            )
        if st.sidebar.button("Sign out private session", key=f"private_sign_out_{role}"):
            st.session_state[sk] = False
            st.rerun()
        return True

    expected = _expected_password(role)

    st.markdown(f"### Private access — {'Investor' if role == 'investor' else 'Partner'}")
    st.caption("Read-only institutional view. No admin or execution controls.")
    pw = st.text_input("Password", type="password", key=f"private_pw_{role}")
    if st.button("Sign in", key=f"private_sign_in_{role}"):
        if secrets.compare_digest(pw.encode("utf-8"), expected.encode("utf-8")):
            st.session_state[sk] = True
            st.rerun()
        else:
            st.error("Invalid password.")

    if using_demo_password(role):
        st.info(
            f"**Demo mode:** default password is `{'demo-investor' if role == 'investor' else 'demo-partner'}`. "
            f"Override with `{_ENV_KEYS[role]}` in `.env`."
        )

    st.stop()
    return False
