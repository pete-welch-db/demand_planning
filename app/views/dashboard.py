"""
Control Tower Dashboard
=======================
Main dashboard with embedded Databricks AI/BI dashboard, network map, 
demand analytics, and ML-powered risk scoring.
"""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from components.metrics import Kpi, line_chart, render_kpi_row
from components.narrative import render_action_hint, render_chart_annotation, render_tab_intro
from config import AppConfig, THEME
from data.service import (
    get_control_tower_weekly,
    get_demand_vs_forecast,
    get_mape_by_family_region,
    get_mape_by_sku,
    get_order_late_risk,
    get_plant_locations,
    get_dc_locations,
    get_freight_lanes,
    get_order_volume_kpis,
    get_service_performance_kpis,
    get_transport_mode_comparison,
    get_product_family_mix,
    get_orders_by_channel,
)


def _fmt_pct(x: float) -> str:
    return f"{x*100:.1f}%"


def _ensure_numeric(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """Convert specified columns to numeric, coercing errors to NaN.
    
    The Databricks SQL connector sometimes returns numeric values as strings,
    which breaks pandas aggregation functions like .mean() and .sum().
    """
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


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
    order_vol = get_order_volume_kpis(cfg, use_mock)
    svc_perf = get_service_performance_kpis(cfg, use_mock)
    transport = get_transport_mode_comparison(cfg, use_mock)
    product_mix = get_product_family_mix(cfg, use_mock)
    
    if ctl.warning:
        st.info("Using mock data (SQL connection unavailable)")
    
    df_ctl = ctl.df.copy()
    if "week" in df_ctl.columns:
        df_ctl["week"] = pd.to_datetime(df_ctl["week"])
    
    # Ensure numeric columns are actually numeric (SQL connector sometimes returns strings)
    df_ctl = _ensure_numeric(df_ctl, ["otif_rate", "freight_cost_per_ton", "premium_freight_pct", "co2_kg_per_ton"])

    # KPI snapshot (latest week)
    latest_week = df_ctl["week"].max() if len(df_ctl) else None
    latest = df_ctl[df_ctl["week"] == latest_week] if latest_week is not None else df_ctl

    otif = float(latest["otif_rate"].mean()) if "otif_rate" in latest.columns and len(latest) else float("nan")
    freight = float(latest["freight_cost_per_ton"].mean()) if "freight_cost_per_ton" in latest.columns and len(latest) else float("nan")
    premium = float(latest["premium_freight_pct"].mean()) if "premium_freight_pct" in latest.columns and len(latest) else float("nan")
    co2 = float(latest["co2_kg_per_ton"].mean()) if "co2_kg_per_ton" in latest.columns and len(latest) else float("nan")

    # Extract new metrics (ensure numeric conversion from SQL strings)
    df_order_vol = _ensure_numeric(order_vol.df, ["total_orders", "unique_customers"])
    total_orders = int(df_order_vol["total_orders"].iloc[0]) if len(df_order_vol) and "total_orders" in df_order_vol.columns else 0
    unique_customers = int(df_order_vol["unique_customers"].iloc[0]) if len(df_order_vol) and "unique_customers" in df_order_vol.columns else 0
    
    df_svc = _ensure_numeric(svc_perf.df, ["perfect_order_rate", "backorder_rate", "cancellation_rate"])
    perfect_order = float(df_svc["perfect_order_rate"].iloc[0]) if len(df_svc) and "perfect_order_rate" in df_svc.columns else float("nan")
    backorder_rate = float(df_svc["backorder_rate"].iloc[0]) if len(df_svc) and "backorder_rate" in df_svc.columns else float("nan")
    cancel_rate = float(df_svc["cancellation_rate"].iloc[0]) if len(df_svc) and "cancellation_rate" in df_svc.columns else float("nan")

    # Section: Core Operational KPIs
    st.subheader("Core Operational KPIs")
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    with col1:
        st.metric("OTIF Rate", _fmt_pct(otif) if otif == otif else "—")
    with col2:
        st.metric("Perfect Order", _fmt_pct(perfect_order) if perfect_order == perfect_order else "—")
    with col3:
        st.metric("Freight $/ton", f"${freight:,.0f}" if freight == freight else "—")
    with col4:
        st.metric("Premium Freight", _fmt_pct(premium) if premium == premium else "—")
    with col5:
        st.metric("CO₂ kg/ton", f"{co2:,.1f}" if co2 == co2 else "—")
    with col6:
        st.metric("Backorder Rate", _fmt_pct(backorder_rate) if backorder_rate == backorder_rate else "—")

    # Section: Volume & Customer Metrics
    st.subheader("Volume & Customer Metrics (13 weeks)")
    vcol1, vcol2, vcol3, vcol4 = st.columns(4)
    with vcol1:
        st.metric("Total Orders", f"{total_orders:,}")
    with vcol2:
        st.metric("Unique Customers", f"{unique_customers:,}")
    with vcol3:
        st.metric("Cancellation Rate", _fmt_pct(cancel_rate) if cancel_rate == cancel_rate else "—")
    with vcol4:
        # Product mix summary
        df_mix = _ensure_numeric(product_mix.df, ["pct_of_total", "order_count"])
        if len(df_mix):
            top_family = df_mix.iloc[0]["sku_family"] if len(df_mix) else "—"
            top_pct = float(df_mix.iloc[0]["pct_of_total"]) if len(df_mix) else 0
            st.metric("Top Product", f"{top_family.title()} ({top_pct:.0f}%)")
        else:
            st.metric("Top Product", "—")

    # Section: Transport Mode Comparison
    df_transport = _ensure_numeric(transport.df, ["freight_cost_per_ton", "co2_kg_per_ton", "tons_shipped"])
    if len(df_transport) > 1:
        st.subheader("Transport Mode Efficiency")
        tcol1, tcol2 = st.columns(2)
        with tcol1:
            st.markdown("**Freight Cost Comparison ($/ton)**")
            for _, row in df_transport.iterrows():
                mode = str(row["transport_mode"]).replace("_", " ").title()
                cost = row["freight_cost_per_ton"]
                tons = row.get("tons_shipped", 0)
                st.markdown(f"- **{mode}**: ${cost:,.0f}/ton ({tons:,.0f} tons shipped)")
        with tcol2:
            st.markdown("**CO₂ Emissions by Mode (kg/ton)**")
            for _, row in df_transport.iterrows():
                mode = str(row["transport_mode"]).replace("_", " ").title()
                co2_val = row["co2_kg_per_ton"]
                st.markdown(f"- **{mode}**: {co2_val:.1f} kg/ton")

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
    lanes_df = _ensure_numeric(lanes_result.df, ["freight_cost_per_ton", "co2_kg_per_ton", "weekly_volume"])

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

    # Databricks brand colors for high contrast
    PLANT_COLOR = "#FF3621"  # Lava 600
    DC_COLOR = "#0B1220"     # Navy 900
    LABEL_COLOR = "#0B1220"  # Navy 900 for maximum contrast
    
    # Add freight lane lines (before markers so they appear behind)
    if len(lanes_df):
        min_cost = lanes_df["freight_cost_per_ton"].min()
        max_cost = lanes_df["freight_cost_per_ton"].max()
        cost_range = max_cost - min_cost if max_cost > min_cost else 1

        for _, lane in lanes_df.iterrows():
            cost_normalized = (lane["freight_cost_per_ton"] - min_cost) / cost_range
            # Green to red gradient (low to high cost)
            r = int(50 + 180 * cost_normalized)
            g = int(160 - 100 * cost_normalized)
            b = int(50)
            line_color = f"rgb({r},{g},{b})"
            
            width = 2 + (lane.get("weekly_volume", 50) / 30)
            width = min(width, 8)

            fig.add_trace(go.Scattergeo(
                lon=[lane["plant_lon"], lane["dc_lon"]],
                lat=[lane["plant_lat"], lane["dc_lat"]],
                mode="lines",
                line=dict(width=width, color=line_color),
                opacity=0.8,
                hoverinfo="text",
                text=f"<b>{lane['plant_name']} → {lane['dc_name']}</b><br>Freight: ${lane['freight_cost_per_ton']:.2f}/ton<br>CO₂: {lane['co2_kg_per_ton']:.1f} kg/ton",
                showlegend=False,
            ))

    # Add plant markers with high-contrast labels
    fig.add_trace(go.Scattergeo(
        lon=plants_df["plant_lon"],
        lat=plants_df["plant_lat"],
        mode="markers+text",
        marker=dict(
            size=16, 
            color=PLANT_COLOR, 
            symbol="circle", 
            line=dict(width=3, color="white")
        ),
        text=plants_df["plant_name"].str.replace("ADS ", "").str.replace(" Plant", "").str.upper(),
        textposition="top center",
        textfont=dict(
            size=11, 
            color=LABEL_COLOR, 
            family="DM Sans, Arial, sans-serif",
        ),
        hoverinfo="text",
        hovertext=plants_df.apply(
            lambda r: f"<b>{r['plant_name']}</b><br>{r['plant_city']}, {r['plant_state']}<br>Region: {r['plant_region']}",
            axis=1
        ),
        name="Plants",
    ))

    # Add DC markers with high-contrast labels
    fig.add_trace(go.Scattergeo(
        lon=dcs_df["dc_lon"],
        lat=dcs_df["dc_lat"],
        mode="markers+text",
        marker=dict(
            size=14, 
            color=DC_COLOR, 
            symbol="square", 
            line=dict(width=3, color="white")
        ),
        text=dcs_df["dc_name"].str.replace("ADS ", "").str.replace(" Distribution", "").str.replace(" Facility", "").str.replace(" Yard", "").str.replace(" Logistics", "").str.upper(),
        textposition="top center",
        textfont=dict(
            size=10, 
            color=LABEL_COLOR, 
            family="DM Sans, Arial, sans-serif",
        ),
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
            landcolor="#E8E6E1",  # Slightly darker for better label contrast
            showlakes=True,
            lakecolor="#F8F8F8",
            showsubunits=True,
            subunitcolor="#D0CEC9",  # Darker state borders
            countrycolor="#D0CEC9",
            bgcolor="white",
        ),
        margin=dict(l=0, r=0, t=50, b=0),
        height=500,
        legend=dict(
            yanchor="top", 
            y=0.99, 
            xanchor="left", 
            x=0.01, 
            bgcolor="rgba(255,255,255,0.95)",
            bordercolor="#E6E4E0",
            borderwidth=1,
            font=dict(size=12, color=LABEL_COLOR),
        ),
        paper_bgcolor="white",
        plot_bgcolor="white",
    )

    # Title annotation with proper contrast
    fig.add_annotation(
        text="<b>ADS Supply Chain Network</b>  |  <span style='color:#FF3621'>● Plants</span>  |  <span style='color:#0B1220'>■ DCs</span>  |  Lines: green=low cost, red=high cost",
        xref="paper", yref="paper",
        x=0.5, y=1.06,
        showarrow=False,
        font=dict(size=13, color=LABEL_COLOR, family="DM Sans, Arial, sans-serif"),
        bgcolor="rgba(255,255,255,0.9)",
        borderpad=6,
    )

    return fig


# =============================================================================
# TAB 3: SERVICE & OTIF
# =============================================================================
def _render_otif_tab(cfg: AppConfig, use_mock: bool) -> None:
    """OTIF trend and service performance tab."""
    ctl = get_control_tower_weekly(cfg, use_mock)
    svc_perf = get_service_performance_kpis(cfg, use_mock)
    channels = get_orders_by_channel(cfg, use_mock)
    
    df_ctl = ctl.df.copy()
    df_svc = _ensure_numeric(svc_perf.df, ["perfect_order_rate", "backorder_rate", "cancellation_rate"])
    df_channels = _ensure_numeric(channels.df, ["otif_rate", "order_count"])
    
    if "week" in df_ctl.columns:
        df_ctl["week"] = pd.to_datetime(df_ctl["week"])
    df_ctl = _ensure_numeric(df_ctl, ["otif_rate", "freight_cost_per_ton", "premium_freight_pct", "co2_kg_per_ton"])

    # Service Performance KPIs
    st.subheader("Service Performance Metrics (13 weeks)")
    if len(df_svc):
        perfect_order = float(df_svc["perfect_order_rate"].iloc[0])
        backorder_rate = float(df_svc["backorder_rate"].iloc[0])
        cancel_rate = float(df_svc["cancellation_rate"].iloc[0])
        
        sc1, sc2, sc3 = st.columns(3)
        with sc1:
            st.metric("Perfect Order Rate", _fmt_pct(perfect_order), help="Orders delivered on-time, in-full, without cancellation or backorder")
        with sc2:
            st.metric("Backorder Rate", _fmt_pct(backorder_rate), help="Percentage of orders with backorder status")
        with sc3:
            st.metric("Cancellation Rate", _fmt_pct(cancel_rate), help="Percentage of orders cancelled")

    # Channel Performance
    if len(df_channels):
        st.subheader("OTIF by Sales Channel")
        render_chart_annotation(
            title="What to notice",
            body="Compare OTIF performance across sales channels. DOT and contractor channels often have tighter SLAs.",
        )
        
        ccol1, ccol2 = st.columns([2, 1])
        with ccol1:
            # Bar chart of OTIF by channel
            fig = px.bar(
                df_channels.sort_values("otif_rate", ascending=True),
                x="otif_rate",
                y="sales_channel",
                orientation="h",
                title="OTIF Rate by Sales Channel",
                labels={"otif_rate": "OTIF Rate", "sales_channel": "Channel"},
            )
            fig.update_layout(
                xaxis_tickformat=".0%",
                xaxis_range=[0.8, 1.0],
                height=250,
                paper_bgcolor="white",
                plot_bgcolor="white",
                showlegend=False,
            )
            fig.update_traces(marker_color=THEME.get("brand_red", "#FF3621"))
            st.plotly_chart(fig, use_container_width=True)
        
        with ccol2:
            st.markdown("**Channel Volume**")
            for _, row in df_channels.iterrows():
                channel = str(row["sales_channel"]).upper()
                orders = int(row["order_count"])
                otif = float(row["otif_rate"])
                st.markdown(f"**{channel}**: {orders:,} orders ({_fmt_pct(otif)} OTIF)")

    st.divider()
    
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
    df_dem = _ensure_numeric(dem.df.copy(), ["actual_units", "forecast_units"])
    
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
# TAB 5: ACCURACY HOTSPOTS (with drill-down)
# =============================================================================
def _render_accuracy_tab(cfg: AppConfig, use_mock: bool) -> None:
    """Forecast accuracy hotspots tab with SKU drill-down."""
    mape = get_mape_by_family_region(cfg, use_mock)
    df_mape = _ensure_numeric(mape.df, ["avg_mape"])

    st.subheader("Forecast Accuracy Hotspots (MAPE)")
    render_chart_annotation(
        title="What to notice",
        body="Click a product family below to drill down to individual SKU accuracy. Higher MAPE = less accurate forecasts.",
    )

    if len(df_mape) == 0:
        st.info("No MAPE data available yet.")
        return

    # Aggregate by family for the clickable summary
    if "sku_family" in df_mape.columns and "avg_mape" in df_mape.columns:
        family_summary = (
            df_mape.groupby("sku_family", as_index=False)["avg_mape"]
            .mean()
            .sort_values("avg_mape", ascending=False)
        )
        family_summary["avg_mape_pct"] = (family_summary["avg_mape"] * 100).round(1).astype(str) + "%"
    else:
        family_summary = df_mape

    # Create clickable family cards
    st.markdown("#### Select Product Family to Drill Down")
    
    families = family_summary["sku_family"].tolist() if "sku_family" in family_summary.columns else []
    
    if families:
        # Create columns for family buttons
        cols = st.columns(len(families))
        
        # Initialize session state for selected family
        if "selected_sku_family" not in st.session_state:
            st.session_state.selected_sku_family = None
        
        for i, fam in enumerate(families):
            mape_val = family_summary[family_summary["sku_family"] == fam]["avg_mape"].iloc[0]
            mape_pct = f"{mape_val * 100:.1f}%"
            
            # Color based on MAPE severity
            if mape_val >= 0.25:
                color = "#B42318"  # Danger
                bg = "#FEE2E2"
            elif mape_val >= 0.15:
                color = "#D97706"  # Warning  
                bg = "#FEF3C7"
            else:
                color = "#067647"  # Success
                bg = "#D1FAE5"
            
            with cols[i]:
                # Custom styled button
                is_selected = st.session_state.selected_sku_family == fam
                border = f"3px solid {THEME['accent_primary']}" if is_selected else f"1px solid {THEME['border_color']}"
                
                st.markdown(f"""
                <div style="
                    background: {bg}; 
                    border: {border}; 
                    border-radius: 12px; 
                    padding: 16px; 
                    text-align: center;
                    cursor: pointer;
                    margin-bottom: 8px;
                ">
                    <p style="font-size: 1.5rem; font-weight: 700; color: {color}; margin: 0;">{mape_pct}</p>
                    <p style="font-size: 0.9rem; font-weight: 600; color: {THEME['text_primary']}; margin: 4px 0 0 0; text-transform: capitalize;">{fam}</p>
                </div>
                """, unsafe_allow_html=True)
                
                if st.button(f"View {fam.title()} SKUs", key=f"btn_{fam}", use_container_width=True):
                    st.session_state.selected_sku_family = fam
                    st.rerun()

    # Show family x region heatmap
    st.markdown("#### MAPE by Family × Region")
    st.dataframe(
        df_mape.pivot_table(index="sku_family", columns="region", values="avg_mape", aggfunc="mean")
        .style.format("{:.1%}")
        .background_gradient(cmap="RdYlGn_r"),
        use_container_width=True,
    )

    # Drill-down section
    st.markdown("---")
    
    if st.session_state.get("selected_sku_family"):
        selected_family = st.session_state.selected_sku_family
        
        st.markdown(f"### 🔍 SKU Detail: **{selected_family.title()}**")
        
        # Load SKU-level data
        sku_result = get_mape_by_sku(cfg, use_mock, selected_family)
        df_sku = _ensure_numeric(sku_result.df, ["avg_mape"])
        
        if len(df_sku):
            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("SKUs Analyzed", len(df_sku["sku_id"].unique()) if "sku_id" in df_sku.columns else len(df_sku))
            with col2:
                avg_mape = df_sku["avg_mape"].mean() if "avg_mape" in df_sku.columns else 0
                st.metric("Avg MAPE", f"{avg_mape:.1%}")
            with col3:
                worst_mape = df_sku["avg_mape"].max() if "avg_mape" in df_sku.columns else 0
                st.metric("Worst MAPE", f"{worst_mape:.1%}")
            with col4:
                best_mape = df_sku["avg_mape"].min() if "avg_mape" in df_sku.columns else 0
                st.metric("Best MAPE", f"{best_mape:.1%}")
            
            # Region filter
            regions = ["All Regions"] + sorted(df_sku["region"].unique().tolist()) if "region" in df_sku.columns else ["All Regions"]
            selected_region = st.selectbox("Filter by Region", regions, key="sku_region_filter")
            
            filtered_sku = df_sku.copy()
            if selected_region != "All Regions":
                filtered_sku = filtered_sku[filtered_sku["region"] == selected_region]
            
            # SKU detail table
            st.markdown("#### Individual SKU Performance")
            
            display_cols = ["sku_id", "sku_name", "region", "avg_mape", "weeks_measured"]
            display_cols = [c for c in display_cols if c in filtered_sku.columns]
            
            # Format for display
            display_df = filtered_sku[display_cols].copy()
            if "avg_mape" in display_df.columns:
                display_df["avg_mape"] = display_df["avg_mape"].apply(lambda x: f"{x:.1%}")
            
            st.dataframe(
                display_df.rename(columns={
                    "sku_id": "Part Number",
                    "sku_name": "Description",
                    "region": "Region",
                    "avg_mape": "MAPE",
                    "weeks_measured": "Weeks"
                }),
                use_container_width=True,
                hide_index=True,
                height=350,
            )
            
            # Action buttons
            st.markdown("#### 🎯 Take Action")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("📊 Run What-If Scenario", type="primary", use_container_width=True):
                    # Store context for scenarios page
                    st.session_state.scenario_context = {
                        "sku_family": selected_family,
                        "avg_mape": avg_mape,
                        "worst_skus": filtered_sku.head(5)["sku_id"].tolist() if "sku_id" in filtered_sku.columns else []
                    }
                    st.info(f"💡 Navigate to **What-If Scenarios** page (in sidebar) to model accuracy improvements for {selected_family}")
            
            with col2:
                if st.button("📥 Export SKU Data", use_container_width=True):
                    csv = filtered_sku.to_csv(index=False)
                    st.download_button(
                        "Download CSV",
                        csv,
                        f"mape_{selected_family}_skus.csv",
                        "text/csv",
                        key="download_sku_csv"
                    )
            
            with col3:
                if st.button("❌ Clear Selection", use_container_width=True):
                    st.session_state.selected_sku_family = None
                    st.rerun()
        else:
            st.info(f"No SKU-level data available for {selected_family}.")
    else:
        st.info("👆 Click a product family above to view individual SKU accuracy metrics")

    render_action_hint(
        title="Action this enables",
        body="Identify specific part numbers with poor forecast accuracy. Use the What-If Scenarios to model improvement impact and prioritize demand sensing investments.",
    )


# =============================================================================
# TAB 6: LATE-RISK (ML)
# =============================================================================
def _render_risk_tab(cfg: AppConfig, use_mock: bool) -> None:
    """ML late-delivery risk tab."""
    risk = get_order_late_risk(cfg, use_mock)
    df_risk = _ensure_numeric(risk.df.copy(), ["late_risk_prob"])

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
