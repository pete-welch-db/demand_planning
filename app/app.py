"""
Routing only (Scrap Intelligence-style).

All view logic lives in app/views/.
All env reads happen ONLY in config.py.
"""

from __future__ import annotations

import os
import sys

# Make `app/` importable as a flat module path when running:
#   streamlit run app/app.py
APP_DIR = os.path.dirname(__file__)
REPO_ROOT = os.path.abspath(os.path.join(APP_DIR, ".."))
for p in [APP_DIR, REPO_ROOT]:
    if p not in sys.path:
        sys.path.insert(0, p)

import streamlit as st  # noqa: E402

from components.styles import apply_theme  # noqa: E402
from components.sidebar import render_sidebar  # noqa: E402
from components.header import render_header  # noqa: E402
from config import get_config  # noqa: E402

from views import landing, dashboard, scenarios, assistant  # noqa: E402


def main() -> None:
    apply_theme()
    cfg = get_config()
    state = render_sidebar(cfg)

    render_header(
        app_name="Demand Planning Control Tower",
        subtitle="Supply chain visibility + forecasting (synthetic demo)",
        right_pill=f"Data: {'Mock' if state.use_mock else 'Databricks SQL (fallback)'}",
    )

    # Routing only
    if state.view == "landing":
        landing.render(cfg, state.use_mock)
    elif state.view == "dashboard":
        dashboard.render(cfg, state.use_mock)
    elif state.view == "scenarios":
        scenarios.render(cfg, state.use_mock)
    elif state.view == "assistant":
        assistant.render(cfg, state.use_mock)
    else:
        st.error("Unknown view")


if __name__ == "__main__":
    main()

