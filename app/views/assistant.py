from __future__ import annotations

import streamlit as st

from components.narrative import render_action_hint, render_chart_annotation, render_tab_intro
from config import AppConfig
from data.genie_client import GenieClient
from data.service import get_control_tower_weekly


def render(cfg: AppConfig, use_mock: bool) -> None:
    st.title("Assistant (optional)")
    render_tab_intro(
        persona="Persona: Supply chain manager / planner",
        business_question="Can I ask a business question in plain English and get a governed, explainable answer?",
        context="Genie is optional. If it’s not configured, this tab demonstrates a safe local fallback so the demo never breaks.",
    )

    st.subheader("KPI-driven starter questions")
    starter_questions = [
        "Using control_tower_weekly, which region is trending down in OTIF over the last 13 weeks? Show the trend and the latest value.",
        "In the last 13 weeks, which plant→DC lanes have the highest freight_cost_per_ton AND below-average OTIF? Return top 10 lanes.",
        "Which sku_family × region has the worst forecast accuracy (MAPE) over the last 8–12 weeks? What changed vs the prior period?",
        "Where is premium_freight_pct highest, and is it correlated with low OTIF or higher late_risk_prob?",
        "Using order_late_risk_scored_ml, which region × sku_family has the highest average late_risk_prob in the last 4 weeks?",
        "If we protected the top 5% highest-risk orders (late_risk_prob), how many OTIF misses could we prevent (proxy using actual_late)?",
    ]

    if "genie_question" not in st.session_state:
        st.session_state["genie_question"] = starter_questions[0]

    chosen = st.selectbox("Pick a starter question", starter_questions, index=0)
    if st.button("Use this question", type="secondary"):
        st.session_state["genie_question"] = chosen

    st.subheader("Ask Genie")
    question = st.text_area(
        "Question",
        value=st.session_state["genie_question"],
        height=90,
    )

    render_chart_annotation(
        title="What to notice",
        body="The intent is to shorten time-to-insight for planners and leaders while keeping data access governed. Even if the assistant is unavailable, the workflow still produces an actionable answer.",
    )

    c1, c2 = st.columns([1, 1])
    with c1:
        ask_clicked = st.button("Ask Genie", type="primary")
    with c2:
        if cfg.genie_space_id and cfg.databricks_host:
            # UI link; exact path can vary by workspace release, so keep it best-effort.
            st.link_button("Open Genie space", f"{cfg.databricks_host.rstrip('/')}/genie/spaces/{cfg.genie_space_id}")

    st.subheader("Query phrasing tips")
    st.markdown(
        """
- **Be explicit about time window**: “last 13 weeks”, “last 90 days”, “week starting Monday”.
- **Name the table(s)**: `control_tower_weekly`, `weekly_demand_actual`, `order_late_risk_scored_ml`.
- **Specify grain + grouping**: “by region”, “by sku_family × region”, “by plant→DC lane”.
- **Ask for the output shape**: “top 10”, “include trend line and latest value”, “return a table + a short summary”.
- **Define the KPI if ambiguous**: “OTIF = on-time AND in-full”.
        """
    )

    if ask_clicked:
        genie = GenieClient(cfg)
        ans = genie.ask(question)
        if ans.ok:
            st.success("Genie answer")
            st.write(ans.text)
            render_action_hint(
                title="Action this enables",
                body="Use the answer to prioritize investigations (inventory, allocation, capacity, lanes) and assign owners—then track impact in the Dashboard tab.",
            )
            return

        st.info(ans.text)

        # Fallback: simple local insight from control tower data
        ctl = get_control_tower_weekly(cfg, use_mock=True)  # force mock-safe for assistant demo
        df = ctl.df.copy()
        if "week" in df.columns and "otif_rate" in df.columns and "region" in df.columns:
            df["week"] = df["week"].astype("datetime64[ns]")
            recent = df.sort_values("week").groupby(["week", "region"], as_index=False)["otif_rate"].mean()
            tail_weeks = sorted(recent["week"].unique())[-13:]
            recent = recent[recent["week"].isin(tail_weeks)]
            # trend proxy: last - first
            trend = (
                recent.sort_values("week").groupby("region")["otif_rate"]
                .apply(lambda s: float(s.iloc[-1] - s.iloc[0]))
                .reset_index(name="otif_trend_13w")
                .sort_values("otif_trend_13w")
            )
            st.subheader("Fallback insight (mock-safe)")
            st.dataframe(trend)
            render_action_hint(
                title="Action this enables",
                body="Identify the worst-trending region and trigger a rapid review: confirm root cause (late deliveries vs in-full misses), then adjust buffer, allocation, and transportation plan for the next two weeks.",
            )
        else:
            st.write("No local insight available yet.")

