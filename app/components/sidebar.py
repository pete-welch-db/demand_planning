from __future__ import annotations

from dataclasses import dataclass

import streamlit as st

from config import AppConfig


@dataclass(frozen=True)
class SidebarState:
    view: str
    use_mock: bool


NAV_ITEMS = [
    ("🏠 Overview", "landing"),
    ("📊 Control Tower", "dashboard"),
    ("🧪 What‑If Scenarios", "scenarios"),
    ("🤖 AI Assistant", "assistant"),
]


def render_sidebar(cfg: AppConfig) -> SidebarState:
    with st.sidebar:
        st.markdown("### 🧭 Demand Planning")
        st.caption("Supply chain visibility + forecasting (synthetic demo)")

        labels = [l for l, _ in NAV_ITEMS]
        default_label = st.session_state.get("nav_label", "🏠 Overview")
        idx = labels.index(default_label) if default_label in labels else 0

        label = st.radio(
            "Nav",
            labels,
            index=idx,
            label_visibility="collapsed",
        )
        st.session_state["nav_label"] = label
        view = dict(NAV_ITEMS)[label]

        with st.expander("⚙️ Settings", expanded=False):
            use_mock = st.toggle(
                "Use mock data",
                value=st.session_state.get("use_mock", cfg.default_use_mock),
                help="When off, the app tries Databricks SQL. Any failure falls back to mock data.",
            )
            st.session_state["use_mock"] = use_mock
    use_mock = st.session_state.get("use_mock", cfg.default_use_mock)

    return SidebarState(view=view, use_mock=use_mock)

