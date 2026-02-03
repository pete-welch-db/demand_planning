"""
Control Tower Dashboard
=======================
Main dashboard with embedded Databricks AI/BI dashboard, network map, 
demand analytics, and ML-powered risk scoring.
"""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from components.metrics import Kpi, line_chart, render_kpi_row
from components.narrative import render_action_hint, render_chart_annotation, render_tab_intro
from config import AppConfig, THEME
from data.service import (
    get_control_tower_weekly,
    get_demand_vs_forecast,
    get_mape_by_family_region,
    get_order_late_risk,
    get_plant_locations,
    get_dc_locations,
    get_freight_lanes,
)


def _fmt_pct(x: float) -> str:
    return f"{x*100:.1f}%"


def render(cfg: AppConfig, use_mock: bool) -> None:
    """Render the Control Tower page with tabs."""
    st.title("Control Tower")

    render_tab_intro(
        persona="Persona: Demand planner + logistics manager",
        business_question="Where are service (OTIF) and demand accuracy trending off-plan, and what should we act on this week?",
        context="Use this view to explore the full picture: embedded dashboards, network visualization, demand trends, and ML risk signals.",
    )

    # Create tabs - logical storytelling order
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 Overview",
        "🗺️ Network Map", 
        "📈 Service & OTIF",
        "📦 Demand vs Forecast",
        "🎯 Accuracy Hotspots",
        "⚠️ Late-Risk (ML)"
    ])

    # =========================================================================
    # TAB 1: OVERVIEW (Embedded AI/BI Dashboard)
    # =========================================================================
    with tab1:
        _render_overview_tab(cfg, use_mock)

    # =========================================================================
    # TAB 2: NETWORK MAP
    # =========================================================================
    with tab2:
        _render_network_tab(cfg, use_mock)

    # =========================================================================
    # TAB 3: SERVICE & OTIF
    # =========================================================================
    with tab3:
        _render_otif_tab(cfg, use_mock)

    # =========================================================================
    # TAB 4: DEMAND VS FORECAST
    # =========================================================================
    with tab4:
        _render_demand_tab(cfg, use_mock)

    # =========================================================================
    # TAB 5: ACCURACY HOTSPOTS
    # =========================================================================
    with tab5:
        _render_accuracy_tab(cfg, use_mock)

    # =========================================================================
    # TAB 6: LATE-RISK (ML)
    # =========================================================================
    with tab6:
        _render_risk_tab(cfg, use_mock)


# =============================================================================
# TAB 1: OVERVIEW
# =============================================================================
def _render_overview_tab(cfg: AppConfig, use_mock: bool) -> None:
    """Embedded Databricks AI/BI Dashboard with KPI summary."""
    
    # Load KPI data
    ctl = get_control_tower_weekly(cfg, use_mock)
    
    if ctl.warning:
        st.info("Using mock data (SQL connection unavailable)")
    
    df_ctl = ctl.df.copy()
    if "week" in df_ctl.columns:
        df_ctl["week"] = pd.to_datetime(df_ctl["week"])

    # KPI snapshot (latest week)
    latest_week = df_ctl["week"].max() if len(df_ctl) else None
    latest = df_ctl[df_ctl["week"] == latest_week] if latest_week is not None else df_ctl

    otif = float(latest["otif_rate"].mean()) if "otif_rate" in latest.columns and len(latest) else float("nan")
    freight = float(latest["freight_cost_per_ton"].mean()) if "freight_cost_per_ton" in latest.columns and len(latest) else float("nan")
    premium = float(latest["premium_freight_pct"].mean()) if "premium_freight_pct" in latest.columns and len(latest) else float("nan")
    co2 = float(latest["co2_kg_per_ton"].mean()) if "co2_kg_per_ton" in latest.columns and len(latest) else float("nan")

    # Compact KPI row
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("OTIF Rate", _fmt_pct(otif) if otif == otif else "—")
    with col2:
        st.metric("Freight $/ton", f"${freight:,.0f}" if freight == freight else "—")
    with col3:
        st.metric("Premium Freight %", _fmt_pct(premium) if premium == premium else "—")
    with col4:
        st.metric("CO₂ kg/ton", f"{co2:,.1f}" if co2 == co2 else "—")

    # Embedded Databricks Dashboard
    if cfg.dashboard_embed_url:
        st.markdown(f"""
        <div style="
            border-radius: 8px; 
            overflow: hidden; 
            border: 1px solid #e0dfdc; 
            background: white;
            margin-top: 16px;
        ">
            <iframe
                src="{cfg.dashboard_embed_url}"
                width="100%"
                height="650"
                frameborder="0">
            </iframe>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.info("Dashboard not configured. Set `DASHBOARD_EMBED_URL` to embed the Databricks AI/BI dashboard.")
        st.markdown("""
        **To configure:**
        1. Publish your Databricks AI/BI dashboard
        2. Copy the embed URL
        3. Add to your environment: `DASHBOARD_EMBED_URL=<url>`
        """)


# =============================================================================
# TAB 2: NETWORK MAP
# =============================================================================
def _render_network_tab(cfg: AppConfig, use_mock: bool) -> None:
    """Interactive network map with facilities and freight lanes."""
    st.subheader("Supply Chain Network")
    render_chart_annotation(
        title="What to notice",
        body="Thicker, redder lines indicate higher-cost freight lanes. Hover over facilities or lanes for details. Look for long-haul routes that could benefit from regional sourcing.",
    )

    # Load data
    plants_result = get_plant_locations(cfg, use_mock)
    dcs_result = get_dc_locations(cfg, use_mock)
    lanes_result = get_freight_lanes(cfg, use_mock)

    # Consolidated warning
    warnings = [r.warning for r in [plants_result, dcs_result, lanes_result] if r.warning]
    if warnings:
        st.info("Using mock data (SQL connection unavailable)")

    plants_df = plants_result.df
    dcs_df = dcs_result.df
    lanes_df = lanes_result.df

    # KPI summary
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Plants", len(plants_df))
    with col2:
        st.metric("Distribution Centers", len(dcs_df))
    with col3:
        st.metric("Active Lanes", len(lanes_df))
    with col4:
        if len(lanes_df) and "freight_cost_per_ton" in lanes_df.columns:
            avg_cost = lanes_df["freight_cost_per_ton"].mean()
            st.metric("Avg Freight $/ton", f"${avg_cost:.0f}")
        else:
            st.metric("Avg Freight $/ton", "—")

    # Network map
    if len(plants_df) and len(dcs_df):
        fig = _create_network_map(plants_df, dcs_df, lanes_df)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": True, "scrollZoom": True})
    else:
        st.info("No location data available.")

    # Freight lanes table in expander
    with st.expander("📋 Freight Lane Details"):
        if len(lanes_df):
            display_df = lanes_df.copy()
            if "freight_cost_per_ton" in display_df.columns:
                display_df["Cost $/ton"] = display_df["freight_cost_per_ton"].apply(lambda x: f"${x:.2f}")
            if "co2_kg_per_ton" in display_df.columns:
                display_df["CO₂ kg/ton"] = display_df["co2_kg_per_ton"].apply(lambda x: f"{x:.1f}")
            
            display_cols = ["plant_name", "dc_name", "Cost $/ton", "CO₂ kg/ton", "weekly_volume"]
            display_cols = [c for c in display_cols if c in display_df.columns]
            
            st.dataframe(
                display_df[display_cols],
                column_config={
                    "plant_name": "Origin Plant",
                    "dc_name": "Destination DC",
                    "weekly_volume": "Weekly Volume",
                },
                hide_index=True,
                use_container_width=True,
            )
        else:
            st.info("No freight lane data available.")

    # Facility tables in expander
    with st.expander("🏭 View All Facilities"):
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Plants**")
            if len(plants_df):
                st.dataframe(plants_df[["plant_id", "plant_name", "plant_city", "plant_state", "plant_region"]], hide_index=True)
        with col2:
            st.markdown("**Distribution Centers**")
            if len(dcs_df):
                st.dataframe(dcs_df[["dc_id", "dc_name", "dc_city", "dc_state", "dc_region"]], hide_index=True)

    render_action_hint(
        title="Action this enables",
        body="Use lane cost analysis to prioritize network optimization: consolidate shipments on high-cost lanes, evaluate regional sourcing for long-haul routes, and identify candidates for mode shifts.",
    )


def _create_network_map(plants_df: pd.DataFrame, dcs_df: pd.DataFrame, lanes_df: pd.DataFrame) -> go.Figure:
    """Create an interactive Plotly map with facilities and freight lanes."""
    fig = go.Figure()

    # Add freight lane lines (before markers so they appear behind)
    if len(lanes_df):
        min_cost = lanes_df["freight_cost_per_ton"].min()
        max_cost = lanes_df["freight_cost_per_ton"].max()
        cost_range = max_cost - min_cost if max_cost > min_cost else 1

        for _, lane in lanes_df.iterrows():
            cost_normalized = (lane["freight_cost_per_ton"] - min_cost) / cost_range
            r = int(50 + 205 * cost_normalized)
            g = int(180 - 130 * cost_normalized)
            b = int(50)
            line_color = f"rgb({r},{g},{b})"
            
            width = 2 + (lane.get("weekly_volume", 50) / 30)
            width = min(width, 8)

            fig.add_trace(go.Scattergeo(
                lon=[lane["plant_lon"], lane["dc_lon"]],
                lat=[lane["plant_lat"], lane["dc_lat"]],
                mode="lines",
                line=dict(width=width, color=line_color),
                opacity=0.7,
                hoverinfo="text",
                text=f"{lane['plant_name']} → {lane['dc_name']}<br>Freight: ${lane['freight_cost_per_ton']:.2f}/ton<br>CO₂: {lane['co2_kg_per_ton']:.1f} kg/ton",
                showlegend=False,
            ))

    # Add plant markers
    fig.add_trace(go.Scattergeo(
        lon=plants_df["plant_lon"],
        lat=plants_df["plant_lat"],
        mode="markers+text",
        marker=dict(size=14, color=THEME["accent_primary"], symbol="circle", line=dict(width=2, color="white")),
        text=plants_df["plant_id"],
        textposition="top center",
        textfont=dict(size=9, color=THEME["text_primary"]),
        hoverinfo="text",
        hovertext=plants_df.apply(
            lambda r: f"<b>{r['plant_name']}</b><br>{r['plant_city']}, {r['plant_state']}<br>Region: {r['plant_region']}",
            axis=1
        ),
        name="Plants",
    ))

    # Add DC markers
    fig.add_trace(go.Scattergeo(
        lon=dcs_df["dc_lon"],
        lat=dcs_df["dc_lat"],
        mode="markers+text",
        marker=dict(size=12, color=THEME["navy_800"], symbol="square", line=dict(width=2, color="white")),
        text=dcs_df["dc_id"],
        textposition="top center",
        textfont=dict(size=9, color=THEME["text_primary"]),
        hoverinfo="text",
        hovertext=dcs_df.apply(
            lambda r: f"<b>{r['dc_name']}</b><br>{r['dc_city']}, {r['dc_state']}<br>Region: {r['dc_region']}",
            axis=1
        ),
        name="DCs",
    ))

    fig.update_layout(
        geo=dict(
            scope="usa",
            projection_type="albers usa",
            showland=True,
            landcolor="#F4F3EE",
            showlakes=True,
            lakecolor="white",
            showsubunits=True,
            subunitcolor="#E6E4E0",
            countrycolor="#E6E4E0",
            bgcolor="rgba(0,0,0,0)",
        ),
        margin=dict(l=0, r=0, t=40, b=0),
        height=480,
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01, bgcolor="rgba(255,255,255,0.8)"),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )

    fig.add_annotation(
        text="<b>ADS Supply Chain Network</b> | <span style='color:#FF3621'>● Plants</span> | <span style='color:#111C33'>■ DCs</span> | Lines colored by freight cost (green=low, red=high)",
        xref="paper", yref="paper",
        x=0.5, y=1.02,
        showarrow=False,
        font=dict(size=12),
    )

    return fig


# =============================================================================
# TAB 3: SERVICE & OTIF
# =============================================================================
def _render_otif_tab(cfg: AppConfig, use_mock: bool) -> None:
    """OTIF trend tab."""
    ctl = get_control_tower_weekly(cfg, use_mock)
    df_ctl = ctl.df.copy()
    
    if "week" in df_ctl.columns:
        df_ctl["week"] = pd.to_datetime(df_ctl["week"])

    st.subheader("OTIF Trend by Region")
    render_chart_annotation(
        title="What to notice",
        body="Look for regions with sustained OTIF decline over multiple weeks. Pair with freight and premium freight to see if service is being protected via expensive expedites.",
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

    render_action_hint(
        title="Action this enables",
        body="Identify regions requiring expedited attention and allocate resources to prevent service degradation.",
    )


# =============================================================================
# TAB 4: DEMAND VS FORECAST
# =============================================================================
def _render_demand_tab(cfg: AppConfig, use_mock: bool) -> None:
    """Demand vs forecast tab."""
    dem = get_demand_vs_forecast(cfg, use_mock)
    df_dem = dem.df.copy()
    
    if "week" in df_dem.columns:
        df_dem["week"] = pd.to_datetime(df_dem["week"])

    st.subheader("Demand vs Forecast (Weekly)")
    render_chart_annotation(
        title="What to notice",
        body="Compare actual vs forecast. Under-forecasting shows up as backorders/premium freight; over-forecasting shows up as higher days of supply.",
    )

    if len(df_dem):
        fams = sorted(df_dem["sku_family"].dropna().unique().tolist()) if "sku_family" in df_dem.columns else []
        regs = sorted(df_dem["region"].dropna().unique().tolist()) if "region" in df_dem.columns else []
        
        c1, c2 = st.columns(2)
        fam = c1.selectbox("SKU family", fams, index=0, key="demand_fam_tab") if fams else None
        reg = c2.selectbox("Region", regs, index=0, key="demand_reg_tab") if regs else None

        plot = df_dem.copy()
        if fam:
            plot = plot[plot["sku_family"] == fam]
        if reg:
            plot = plot[plot["region"] == reg]

        plot_long = plot.melt(
            id_vars=["week"],
            value_vars=[c for c in ["actual_units", "forecast_units"] if c in plot.columns],
            var_name="series",
            value_name="units"
        )
        line_chart(plot_long, x="week", y="units", color="series", title=f"{fam or ''} {reg or ''}".strip())
        
        with st.expander("📋 View Data"):
            st.dataframe(plot.sort_values("week"), use_container_width=True)
    else:
        st.info("No demand/forecast data available yet.")


# =============================================================================
# TAB 5: ACCURACY HOTSPOTS
# =============================================================================
def _render_accuracy_tab(cfg: AppConfig, use_mock: bool) -> None:
    """Forecast accuracy hotspots tab."""
    mape = get_mape_by_family_region(cfg, use_mock)
    df_mape = mape.df

    st.subheader("Forecast Accuracy Hotspots (MAPE)")
    render_chart_annotation(
        title="What to notice",
        body="Higher MAPE indicates less accurate forecasts. Focus improvement efforts on high-MAPE SKU families and regions.",
    )
    st.caption("Average MAPE by SKU family and region (lower is better)")

    if len(df_mape):
        st.dataframe(df_mape, use_container_width=True, hide_index=True)
    else:
        st.info("No MAPE data available yet.")

    render_action_hint(
        title="Action this enables",
        body="Prioritize demand sensing improvements and planner attention on the highest-error family/region combinations.",
    )


# =============================================================================
# TAB 6: LATE-RISK (ML)
# =============================================================================
def _render_risk_tab(cfg: AppConfig, use_mock: bool) -> None:
    """ML late-delivery risk tab."""
    risk = get_order_late_risk(cfg, use_mock)
    df_risk = risk.df.copy()

    st.subheader("Late-Delivery Risk (ML Predictions)")
    render_chart_annotation(
        title="What to notice",
        body="This is the ML signal you can act on now: focus attention and capacity on the highest-risk segments before service misses happen.",
    )

    if len(df_risk):
        if "order_date" in df_risk.columns:
            df_risk["order_date"] = pd.to_datetime(df_risk["order_date"])

        # Summary metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            high_risk_count = len(df_risk[df_risk["late_risk_prob"] >= 0.5])
            st.metric("🚨 High-Risk Orders", high_risk_count)
        with col2:
            avg_risk = df_risk["late_risk_prob"].mean()
            st.metric("📊 Avg Risk Score", f"{avg_risk:.1%}")
        with col3:
            max_risk = df_risk["late_risk_prob"].max()
            st.metric("⚠️ Max Risk Score", f"{max_risk:.1%}")

        # Hotspots: average risk by region + family
        hotspots = (
            df_risk.groupby(["customer_region", "sku_family"], as_index=False)["late_risk_prob"]
            .mean()
            .sort_values("late_risk_prob", ascending=False)
            .head(12)
        )

        col1, col2 = st.columns([1, 1])
        with col1:
            st.markdown("**Top Risk Hotspots (Region × Family)**")
            st.dataframe(hotspots, use_container_width=True, hide_index=True)

        with col2:
            st.markdown("**Recent High-Risk Orders**")
            high_risk = df_risk[df_risk["late_risk_prob"] >= 0.5].head(10)
            if len(high_risk):
                display_cols = ["order_date", "customer_region", "sku_family", "late_risk_prob"]
                display_cols = [c for c in display_cols if c in high_risk.columns]
                st.dataframe(high_risk[display_cols], use_container_width=True, hide_index=True)
            else:
                st.info("No high-risk orders in recent data.")
    else:
        st.info("No risk scoring data available yet.")

    render_action_hint(
        title="Action this enables",
        body="Use the late-risk hotspots to reserve capacity and prioritize interventions: protect the top at-risk family/region orders with allocation and fleet planning.",
    )
