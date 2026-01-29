from __future__ import annotations

import streamlit as st

from components.narrative import render_tab_intro
from config import AppConfig


def render(cfg: AppConfig, use_mock: bool) -> None:
    render_tab_intro(
        persona="Persona: Executive sponsor + supply chain planning leadership",
        business_question="How do we unify supply chain data and scale forecasting to 25k SKUs to improve service, cost, and sustainability?",
        context="This landing page frames the story; use the Dashboard tab for KPIs and drill-downs, Scenarios for value translation, and Assistant for Q&A.",
    )

    # --- Hero ---
    st.markdown(
        """
<div class="hero">
  <div class="hero-title">Demand planning, with end-to-end visibility</div>
  <p class="hero-narrative">
    For supply chain demand planners at a regional, freight-sensitive manufacturer.<br/>
    Unify ERP/WMS/TMS + production signals, forecast demand, and turn insights into action across OTIF, inventory, freight, and sustainability.
  </p>
</div>
        """,
        unsafe_allow_html=True,
    )

    # --- Value cards ---
    st.markdown('<div class="section-title">Why this demo</div>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)

    with c1:
        st.markdown(
            """
<div class="value-card">
  <div class="value-card-title">Problem</div>
  <div class="value-card-body">
    Volatile, project-driven demand + regional plants + high freight sensitivity makes planning and service levels hard to balance.
  </div>
</div>
            """,
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            """
<div class="value-card">
  <div class="value-card-title">Data</div>
  <div class="value-card-body">
    ERP orders, inventory, TMS shipments, telematics-like signals, production output, and external drivers—unified in Delta.
  </div>
</div>
            """,
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(
            """
<div class="value-card">
  <div class="value-card-title">KPIs</div>
  <div class="value-card-body">
    Forecast accuracy (MAPE), OTIF, days of supply, inventory turns, freight $/ton, premium freight %, CO₂/ton, and energy intensity.
  </div>
</div>
            """,
            unsafe_allow_html=True,
        )
    with c4:
        st.markdown(
            """
<div class="value-card">
  <div class="value-card-title">Impact</div>
  <div class="value-card-body">
    A small team standardizes pipelines + forecasting patterns to scale toward 25k SKUs, improving accuracy, reducing buffers, and cutting expedite.
  </div>
</div>
            """,
            unsafe_allow_html=True,
        )

    # --- How it works ---
    st.markdown('<div class="section-title">How it works</div>', unsafe_allow_html=True)
    h1, h2, h3 = st.columns(3)
    with h1:
        st.markdown(
            """
<div class="how-step">
  <div class="how-step-title">1) Ingest & unify</div>
  <div class="how-step-body">
    Land ERP/WMS/TMS/production/external signals as Delta tables and publish curated gold views for planners and leadership.
  </div>
</div>
            """,
            unsafe_allow_html=True,
        )
    with h2:
        st.markdown(
            """
<div class="how-step">
  <div class="how-step-title">2) ML / Genie / AI</div>
  <div class="how-step-body">
    Forecast demand (hierarchical → part-level at scale), track MAPE/bias, and enable Q&A-style analysis via Genie (optional).
  </div>
</div>
            """,
            unsafe_allow_html=True,
        )
    with h3:
        st.markdown(
            """
<div class="how-step">
  <div class="how-step-title">3) Decision & action</div>
  <div class="how-step-body">
    Prioritize hotspots, tune buffers, rebalance inventory, and reduce premium freight while maintaining OTIF and sustainability goals.
  </div>
</div>
            """,
            unsafe_allow_html=True,
        )

    # --- Setup/status ---
    st.markdown('<div class="section-title">Connection status</div>', unsafe_allow_html=True)
    s1, s2, s3 = st.columns([1, 1, 2])
    with s1:
        st.markdown("**Data mode**")
        st.write("Mock" if use_mock else "Databricks SQL (fallback)")
    with s2:
        st.markdown("**Target schema**")
        st.code(f"{cfg.databricks_catalog}.{cfg.databricks_schema}", language="text")
    with s3:
        st.markdown("**Next steps**")
        st.write(
            "Run `notebooks/01_uc_setup` → `02_generate_bronze`, then run the DLT pipeline, then "
            "`03_forecast_weekly_mlflow` → `04_post_forecast_kpis` → `05_ml_late_risk`. "
            "After that, toggle off Mock mode."
        )

    if not cfg.databricks_host or not cfg.databricks_http_path:
        st.info("Real data mode needs `DATABRICKS_HOST` and `DATABRICKS_HTTP_PATH`. Mock mode always works.")

    with st.expander("For technical folks: what’s powering this app (real backend)"):
        st.markdown(
            """
**Lakeflow SDP/DLT (Bronze→Silver→Gold)**
- Pipeline: `pipelines/dlt_supply_chain_medallion.py`
- Gold contract: `control_tower_weekly`, `weekly_demand_actual`, `order_late_risk_scored`, KPI tables

**MLflow (real ML in the loop)**
- Train/register: `notebooks/05_ml_late_risk.py`
- Output: `order_late_risk_scored` (used directly in Dashboard + Scenarios)

**Genie (optional, same Gold data)**
- Configure: `GENIE_SPACE_ID`

**Asset Bundles (deployable by default)**
- Bundle entrypoint: `databricks.yml`
            """
        )

