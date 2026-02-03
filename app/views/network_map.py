"""
Network Map View - Interactive US map showing ADS facilities and freight lanes.

Uses Plotly scatter_mapbox for:
- Plant and DC markers
- Freight lane lines colored by cost intensity
- Hover tooltips with facility/lane details
"""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from components.narrative import render_tab_intro, render_chart_annotation, render_action_hint
from config import AppConfig, THEME
from data.service import get_plant_locations, get_dc_locations, get_freight_lanes


def render(cfg: AppConfig, use_mock: bool) -> None:
    st.title("Network Map")

    render_tab_intro(
        persona="Persona: Supply chain strategist + network planner",
        business_question="Where are our facilities located, and which freight lanes have the highest costs?",
        context="Use this view to visualize the ADS supply chain network, identify high-cost freight lanes, and spot opportunities for network optimization.",
    )

    # Load data
    plants_result = get_plant_locations(cfg, use_mock)
    dcs_result = get_dc_locations(cfg, use_mock)
    lanes_result = get_freight_lanes(cfg, use_mock)

    # Show warnings if any
    for result in [plants_result, dcs_result, lanes_result]:
        if result.warning:
            st.warning(result.warning)

    st.caption(f"Data source: **{plants_result.source}**")

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

    st.divider()

    # Map visualization
    st.subheader("Supply Chain Network Map")
    render_chart_annotation(
        title="What to notice",
        body="Thicker, redder lines indicate higher-cost freight lanes. Click on facilities or lanes for details. Look for long-haul routes that could benefit from regional sourcing.",
    )

    if len(plants_df) and len(dcs_df):
        fig = _create_network_map(plants_df, dcs_df, lanes_df)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": True, "scrollZoom": True})
    else:
        st.info("No location data available.")

    st.divider()

    # Freight lanes table
    st.subheader("Freight Lane Details")
    render_chart_annotation(
        title="Top lanes by cost",
        body="These are the highest-cost freight lanes in the network. Consider consolidation, mode shifts, or regional sourcing to reduce costs.",
    )

    if len(lanes_df):
        # Format for display
        display_df = lanes_df.copy()
        if "freight_cost_per_ton" in display_df.columns:
            display_df["freight_cost_per_ton"] = display_df["freight_cost_per_ton"].apply(lambda x: f"${x:.2f}")
        if "co2_kg_per_ton" in display_df.columns:
            display_df["co2_kg_per_ton"] = display_df["co2_kg_per_ton"].apply(lambda x: f"{x:.1f}")
        
        # Select columns to display
        display_cols = ["plant_name", "dc_name", "freight_cost_per_ton", "co2_kg_per_ton", "weekly_volume"]
        display_cols = [c for c in display_cols if c in display_df.columns]
        
        st.dataframe(
            display_df[display_cols].head(20),
            column_config={
                "plant_name": "Origin Plant",
                "dc_name": "Destination DC",
                "freight_cost_per_ton": "Freight $/ton",
                "co2_kg_per_ton": "CO₂ kg/ton",
                "weekly_volume": "Weekly Volume",
            },
            hide_index=True,
        )
    else:
        st.info("No freight lane data available.")

    # Facility tables in expanders
    with st.expander("View All Plants"):
        if len(plants_df):
            st.dataframe(plants_df, hide_index=True)
        else:
            st.info("No plant data available.")

    with st.expander("View All Distribution Centers"):
        if len(dcs_df):
            st.dataframe(dcs_df, hide_index=True)
        else:
            st.info("No DC data available.")

    render_action_hint(
        title="Action this enables",
        body="Use lane cost analysis to prioritize network optimization projects: consolidate shipments on high-cost lanes, evaluate regional sourcing for long-haul routes, and identify candidates for mode shifts (truck to rail/intermodal).",
    )


def _create_network_map(
    plants_df: pd.DataFrame,
    dcs_df: pd.DataFrame,
    lanes_df: pd.DataFrame,
) -> go.Figure:
    """Create an interactive Plotly map with facilities and freight lanes."""
    
    fig = go.Figure()

    # Add freight lane lines (before markers so they appear behind)
    if len(lanes_df):
        # Normalize freight cost for color scaling
        min_cost = lanes_df["freight_cost_per_ton"].min()
        max_cost = lanes_df["freight_cost_per_ton"].max()
        cost_range = max_cost - min_cost if max_cost > min_cost else 1

        for _, lane in lanes_df.iterrows():
            # Color intensity based on freight cost (higher = more red)
            cost_normalized = (lane["freight_cost_per_ton"] - min_cost) / cost_range
            # Interpolate from green (low cost) to red (high cost)
            r = int(50 + 205 * cost_normalized)
            g = int(180 - 130 * cost_normalized)
            b = int(50)
            line_color = f"rgb({r},{g},{b})"
            
            # Line width based on volume (if available)
            width = 2 + (lane.get("weekly_volume", 50) / 30)
            width = min(width, 8)  # Cap width

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
        marker=dict(
            size=14,
            color=THEME["accent_primary"],
            symbol="circle",
            line=dict(width=2, color="white"),
        ),
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
        marker=dict(
            size=12,
            color=THEME["navy_800"],
            symbol="square",
            line=dict(width=2, color="white"),
        ),
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

    # Update layout for US-focused map
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
        height=550,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
            bgcolor="rgba(255,255,255,0.8)",
        ),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )

    # Add title annotation
    fig.add_annotation(
        text="<b>ADS Supply Chain Network</b> | <span style='color:#FF3621'>● Plants</span> | <span style='color:#111C33'>■ DCs</span> | Lines colored by freight cost (green=low, red=high)",
        xref="paper", yref="paper",
        x=0.5, y=1.02,
        showarrow=False,
        font=dict(size=12),
    )

    return fig
