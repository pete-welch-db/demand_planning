from __future__ import annotations

from dataclasses import dataclass

import streamlit as st

from config import AppConfig


@dataclass(frozen=True)
class SidebarState:
    view: str
    use_mock: bool


VIEWS = {
    "Landing": "landing",
    "Dashboard": "dashboard",
    "Scenarios": "scenarios",
    "Assistant": "assistant",
}


def render_sidebar(cfg: AppConfig) -> SidebarState:
    with st.sidebar:
        st.title("Control Tower")
        st.caption("Pipe / stormwater manufacturer demo (synthetic data)")

        use_mock = st.toggle(
            "Use mock data (fallback-safe)",
            value=st.session_state.get("use_mock", cfg.default_use_mock),
            help="When off, the app tries Databricks SQL. Any failure falls back to mock data.",
        )
        st.session_state["use_mock"] = use_mock

        label = st.radio("Navigation", list(VIEWS.keys()), index=1)
        view = VIEWS[label]

        st.divider()
        st.markdown("**Target schema**")
        st.code(f"{cfg.databricks_catalog}.{cfg.databricks_schema}", language="text")

    return SidebarState(view=view, use_mock=use_mock)

