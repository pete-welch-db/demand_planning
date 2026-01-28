from __future__ import annotations

import pandas as pd
import streamlit as st

from components.metrics import Kpi, line_chart, render_kpi_row
from components.narrative import render_action_hint, render_chart_annotation, render_tab_intro
from config import AppConfig
from data.service import get_control_tower_weekly, get_demand_vs_forecast, get_mape_by_family_region, get_order_late_risk


def _fmt_pct(x: float) -> str:
    return f"{x*100:.1f}%"


def render(cfg: AppConfig, use_mock: bool) -> None:
    st.title("Dashboard")

    render_tab_intro(
        persona="Persona: Demand planner + logistics manager",
        business_question="Where are service (OTIF) and demand accuracy trending off-plan, and what should we act on this week?",
        context="Use this view to spot regional/family hotspots, then drill into demand vs forecast and accuracy to prioritize interventions.",
    )

    # --- load data (graceful fallback inside service) ---
    ctl = get_control_tower_weekly(cfg, use_mock)
    dem = get_demand_vs_forecast(cfg, use_mock)
    mape = get_mape_by_family_region(cfg, use_mock)
    risk = get_order_late_risk(cfg, use_mock)

    if ctl.warning:
        st.warning(ctl.warning)
    if dem.warning:
        st.warning(dem.warning)
    if mape.warning:
        st.warning(mape.warning)
    if risk.warning:
        st.warning(risk.warning)

    st.caption(f"Data source: **{ctl.source}** (controls KPI tables; demand/forecast source may differ)")

    df_ctl = ctl.df.copy()
    if "week" in df_ctl.columns:
        df_ctl["week"] = pd.to_datetime(df_ctl["week"])

    # --- KPI snapshot (latest week) ---
    latest_week = df_ctl["week"].max() if len(df_ctl) else None
    latest = df_ctl[df_ctl["week"] == latest_week] if latest_week is not None else df_ctl

    # Take mean across records for a simple executive snapshot
    otif = float(latest["otif_rate"].mean()) if "otif_rate" in latest.columns and len(latest) else float("nan")
    freight = float(latest["freight_cost_per_ton"].mean()) if "freight_cost_per_ton" in latest.columns and len(latest) else float("nan")
    premium = float(latest["premium_freight_pct"].mean()) if "premium_freight_pct" in latest.columns and len(latest) else float("nan")
    co2 = float(latest["co2_kg_per_ton"].mean()) if "co2_kg_per_ton" in latest.columns and len(latest) else float("nan")
    energy = float(latest["energy_kwh_per_unit"].mean()) if "energy_kwh_per_unit" in latest.columns and len(latest) else float("nan")

    render_kpi_row(
        [
            Kpi("OTIF", _fmt_pct(otif) if otif == otif else "—", help="On-time AND in-full (proxy)"),
            Kpi("Freight $/ton", f"${freight:,.0f}" if freight == freight else "—"),
            Kpi("Premium freight %", _fmt_pct(premium) if premium == premium else "—", help="Carrier-mode freight share (proxy)"),
            Kpi("CO₂ kg/ton", f"{co2:,.1f}" if co2 == co2 else "—"),
            Kpi("Energy kWh/unit", f"{energy:,.2f}" if energy == energy else "—"),
        ]
    )

    st.divider()

    # --- OTIF trend ---
    st.subheader("OTIF trend")
    render_chart_annotation(
        title="What to notice",
        body="Look for regions with sustained OTIF decline over multiple weeks (not one-off noise). Pair with freight and premium freight to see if service is being protected via expensive expedites.",
    )
    if len(df_ctl) and "otif_rate" in df_ctl.columns:
        otif_trend = (
            df_ctl.groupby(["week", "region"], as_index=False)["otif_rate"].mean()
            if "region" in df_ctl.columns
            else df_ctl.groupby(["week"], as_index=False)["otif_rate"].mean()
        )
        line_chart(
            otif_trend,
            x="week",
            y="otif_rate",
            color="region" if "region" in otif_trend.columns else None,
            title="OTIF by region (weekly)",
            y_format="percent",
        )
    else:
        st.info("No OTIF data available yet.")

    # --- Demand vs forecast ---
    st.subheader("Demand vs forecast (weekly)")
    render_chart_annotation(
        title="What to notice",
        body="Compare actual vs forecast in the most recent weeks. Systematic under-forecasting typically shows up as backorders/premium freight; over-forecasting shows up as higher days of supply.",
    )
    df_dem = dem.df.copy()
    if "week" in df_dem.columns:
        df_dem["week"] = pd.to_datetime(df_dem["week"])

    if len(df_dem):
        # Let user pick a slice
        fams = sorted(df_dem["sku_family"].dropna().unique().tolist()) if "sku_family" in df_dem.columns else []
        regs = sorted(df_dem["region"].dropna().unique().tolist()) if "region" in df_dem.columns else []
        c1, c2 = st.columns(2)
        fam = c1.selectbox("SKU family", fams, index=0) if fams else None
        reg = c2.selectbox("Region", regs, index=0) if regs else None

        plot = df_dem.copy()
        if fam:
            plot = plot[plot["sku_family"] == fam]
        if reg:
            plot = plot[plot["region"] == reg]

        # Plot actual + forecast (if present)
        plot_long = plot.melt(id_vars=["week"], value_vars=[c for c in ["actual_units", "forecast_units"] if c in plot.columns],
                              var_name="series", value_name="units")
        line_chart(plot_long, x="week", y="units", color="series", title=f"{fam or ''} {reg or ''}".strip())
        with st.expander("Show underlying data"):
            st.dataframe(plot.sort_values("week"))
    else:
        st.info("No demand/forecast data available yet.")

    # --- Accuracy hotspots ---
    st.subheader("Forecast accuracy hotspots (avg MAPE)")
    st.caption("If forecasts haven’t been generated in Databricks yet, this will use mock data.")
    st.dataframe(mape.df)

    # --- ML in the loop: late risk ---
    st.subheader("Late-delivery risk (ML in the loop)")
    render_chart_annotation(
        title="What to notice",
        body="This is the ML signal you can act on now: focus attention and capacity on the highest-risk segments before service misses happen.",
    )

    df_risk = risk.df.copy()
    if len(df_risk):
        df_risk["order_date"] = pd.to_datetime(df_risk["order_date"])
        # Hotspots: average risk by region + family
        hotspots = (
            df_risk.groupby(["customer_region", "sku_family"], as_index=False)["late_risk_prob"]
            .mean()
            .sort_values("late_risk_prob", ascending=False)
            .head(12)
        )
        st.dataframe(hotspots)
    else:
        st.info("No risk scoring data available yet.")

    render_action_hint(
        title="Action this enables",
        body="Use the late-risk hotspots to reserve capacity and prioritize interventions: protect the top at-risk family/region orders with allocation and fleet planning, reducing OTIF misses and reactive premium freight.",
    )

