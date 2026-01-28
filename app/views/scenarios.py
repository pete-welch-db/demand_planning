from __future__ import annotations

import streamlit as st

from components.narrative import render_action_hint, render_chart_annotation, render_tab_intro
from config import AppConfig
from data.service import get_mape_by_family_region, get_order_late_risk


def render(cfg: AppConfig, use_mock: bool) -> None:
    st.title("Scenarios (What-if)")
    render_tab_intro(
        persona="Persona: Demand planning lead / IBP owner",
        business_question="If forecast accuracy improves, how does that translate into lower buffers and less premium freight?",
        context="This is an illustrative planning narrative—not a full optimization model. It’s designed to connect accuracy to operational levers in a live demo.",
    )

    res = get_mape_by_family_region(cfg, use_mock)
    risk = get_order_late_risk(cfg, use_mock)
    if res.warning:
        st.warning(res.warning)
    if risk.warning:
        st.warning(risk.warning)

    df = res.df.copy()
    if len(df) == 0 or "avg_mape" not in df.columns:
        st.info("No MAPE data available yet.")
        return

    baseline_mape = float(df["avg_mape"].mean())

    st.subheader("Improve forecast accuracy → reduce inventory + expedite")
    render_chart_annotation(
        title="What to notice",
        body="Treat MAPE as a leading indicator: as error drops, planners can safely reduce safety stock and avoid reactive carrier moves.",
    )
    improvement = st.slider("MAPE improvement (relative)", min_value=0, max_value=40, value=15, step=5)
    improved_mape = baseline_mape * (1 - improvement / 100.0)

    # Illustrative translation (NOT a real inventory model)
    # Assume safety stock scales ~ with forecast error; expedite/premium freight scales with service misses.
    est_safety_stock_reduction = 0.6 * (improvement / 100.0)
    est_premium_freight_reduction = 0.8 * (improvement / 100.0)

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Baseline avg MAPE", f"{baseline_mape*100:.1f}%")
    with c2:
        st.metric("Improved avg MAPE", f"{improved_mape*100:.1f}%")
    with c3:
        st.metric("Estimated safety stock reduction", f"{est_safety_stock_reduction*100:.0f}%")

    st.markdown(
        """
**How to narrate this in the demo**
- Start at the hierarchy (family × region) to show quick wins with a small team.
- Then scale the pattern to 25k SKU series using standardized features + MLflow + automated backtesting.
- As accuracy improves, planners can safely lower buffers and reduce backorders/premium freight.
        """
    )

    st.subheader("Where to focus first (highest MAPE)")
    st.dataframe(df.head(15))

    st.subheader("ML-in-loop: reduce late-risk with proactive actions")
    render_chart_annotation(
        title="What to notice",
        body="Late-risk predictions identify where to intervene (capacity reservation, allocation, lane planning) before OTIF misses occur.",
    )
    df_risk = risk.df.copy()
    if len(df_risk) and "late_risk_prob" in df_risk.columns:
        baseline_risk = float(df_risk["late_risk_prob"].mean())
        mitigation = st.slider("Risk mitigation from proactive actions (relative)", min_value=0, max_value=40, value=20, step=5)
        mitigated_risk = baseline_risk * (1 - mitigation / 100.0)
        st.metric("Avg late-risk (baseline)", f"{baseline_risk*100:.1f}%")
        st.metric("Avg late-risk (mitigated)", f"{mitigated_risk*100:.1f}%")
    else:
        st.info("No late-risk scoring data available yet.")

    render_action_hint(
        title="Action this enables",
        body="Combine accuracy + risk: use MAPE to choose where to improve forecasts, and use late-risk to decide which lanes/orders to protect this week (allocation + fleet capacity) to prevent OTIF misses and premium freight.",
    )

