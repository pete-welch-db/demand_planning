"""
What-If Scenarios Page
======================
Interactive scenario planning for demand planning optimization.

Scenarios:
- Forecast Accuracy: Improve MAPE to reduce buffers and premium freight
- Freight Optimization: Reduce costs through lane consolidation and mode shifts
- Inventory Planning: Optimize safety stock levels
- Risk Mitigation: Proactive actions to reduce late-delivery risk
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from components.metrics import Kpi, render_kpi_row
from components.narrative import render_action_hint, render_chart_annotation
from config import AppConfig, THEME
from data.service import (
    get_control_tower_weekly,
    get_demand_vs_forecast,
    get_mape_by_family_region,
    get_order_late_risk,
    get_freight_lanes,
)


def render(cfg: AppConfig, use_mock: bool) -> None:
    """Render the What-If Scenarios page."""
    
    # Header
    st.markdown("""
    <div style="margin-bottom: 24px;">
        <h1 style="margin: 0;">🎯 What-If Scenarios</h1>
        <p style="font-size: 1.1rem; color: #64748b; margin: 8px 0 0 0;">
            Simulate demand planning optimization strategies and estimate impact
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # Scenario tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Forecast Accuracy",
        "🚚 Freight Optimization", 
        "📦 Inventory Planning",
        "⚠️ Risk Mitigation"
    ])
    
    with tab1:
        _render_accuracy_scenario(cfg, use_mock)
    
    with tab2:
        _render_freight_scenario(cfg, use_mock)
    
    with tab3:
        _render_inventory_scenario(cfg, use_mock)
    
    with tab4:
        _render_risk_scenario(cfg, use_mock)


# =============================================================================
# Recommendations Component
# =============================================================================
def _render_recommendations(recommendations: list, title: str = "💡 Recommended Actions") -> None:
    """Render the recommendations panel with priority colors."""
    
    priority_colors = {
        "high": "#ef4444",
        "medium": "#f59e0b",
        "low": "#10b981"
    }
    
    st.markdown(f"#### {title}")
    
    for rec in recommendations:
        color = priority_colors.get(rec.get("priority", "low"), "#6366f1")
        st.markdown(f"""
        <div style="background: #f8f9fa; border-left: 3px solid {color}; border-radius: 8px; padding: 12px; margin-bottom: 8px;">
            <div style="display: flex; align-items: flex-start; gap: 10px;">
                <span style="font-size: 1.2rem;">{rec['icon']}</span>
                <div>
                    <p style="color: #1a1a1a; font-weight: 600; margin: 0; font-size: 0.9rem;">{rec['title']}</p>
                    <p style="color: #64748b; margin: 4px 0 0 0; font-size: 0.8rem;">{rec['desc']}</p>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    # Priority legend
    st.markdown("""
    <div style="display: flex; gap: 16px; margin-top: 12px; padding: 8px 12px; background: #f1f5f9; border-radius: 8px;">
        <div style="display: flex; align-items: center; gap: 6px;">
            <div style="width: 12px; height: 12px; background: #ef4444; border-radius: 2px;"></div>
            <span style="color: #64748b; font-size: 0.75rem;">High Priority</span>
        </div>
        <div style="display: flex; align-items: center; gap: 6px;">
            <div style="width: 12px; height: 12px; background: #f59e0b; border-radius: 2px;"></div>
            <span style="color: #64748b; font-size: 0.75rem;">Medium</span>
        </div>
        <div style="display: flex; align-items: center; gap: 6px;">
            <div style="width: 12px; height: 12px; background: #10b981; border-radius: 2px;"></div>
            <span style="color: #64748b; font-size: 0.75rem;">Low</span>
        </div>
    </div>
    """, unsafe_allow_html=True)


def _create_metric_card(value: str, label: str, delta: float = None) -> str:
    """Create an HTML metric card."""
    delta_html = ""
    if delta is not None:
        delta_color = "#10b981" if delta < 0 else "#ef4444"
        delta_sign = "" if delta < 0 else "+"
        delta_html = f'<p style="color: {delta_color}; font-size: 0.85rem; margin: 4px 0 0 0;">{delta_sign}{delta:.1f}%</p>'
    
    return f"""
    <div style="background: #f8f9fa; border-radius: 12px; padding: 16px; text-align: center; border: 1px solid #e2e8f0;">
        <p style="color: #1a1a1a; font-size: 1.5rem; font-weight: 700; margin: 0;">{value}</p>
        <p style="color: #64748b; font-size: 0.85rem; margin: 4px 0 0 0;">{label}</p>
        {delta_html}
    </div>
    """


# =============================================================================
# TAB 1: Forecast Accuracy Scenario
# =============================================================================
def _get_accuracy_recommendations(improvement: int, baseline_mape: float, top_families: list) -> list:
    """Generate recommendations for accuracy improvement scenario."""
    recommendations = []
    
    if improvement >= 25:
        recommendations.append({
            "icon": "🎯",
            "title": "Aggressive Target",
            "desc": "Consider phased rollout—start with top 3 SKU families",
            "priority": "high"
        })
    
    if baseline_mape > 0.25:
        recommendations.append({
            "icon": "🔴",
            "title": "High Baseline Error",
            "desc": f"Current MAPE {baseline_mape*100:.0f}% suggests data quality issues",
            "priority": "high"
        })
        recommendations.append({
            "icon": "🔍",
            "title": "Root Cause Analysis",
            "desc": "Investigate demand signal latency and outlier handling",
            "priority": "high"
        })
    
    if top_families:
        recommendations.append({
            "icon": "📊",
            "title": f"Focus: {top_families[0]}",
            "desc": "Highest-error family—prioritize for demand sensing",
            "priority": "high"
        })
    
    recommendations.append({
        "icon": "🤖",
        "title": "ML Model Upgrade",
        "desc": "Add external signals (weather, promotions, events)",
        "priority": "medium"
    })
    
    recommendations.append({
        "icon": "📈",
        "title": "Ensemble Methods",
        "desc": "Combine statistical + ML models for better accuracy",
        "priority": "medium"
    })
    
    recommendations.append({
        "icon": "🔄",
        "title": "Weekly Retraining",
        "desc": "Automate model refresh with latest demand signals",
        "priority": "low"
    })
    
    recommendations.append({
        "icon": "📋",
        "title": "Planner Collaboration",
        "desc": "Incorporate market intelligence into forecast adjustments",
        "priority": "low"
    })
    
    return recommendations[:6]


def _render_accuracy_scenario(cfg: AppConfig, use_mock: bool) -> None:
    """Render the forecast accuracy improvement scenario."""
    
    st.markdown("### Forecast Accuracy Improvement")
    st.markdown("*Improve MAPE to reduce safety stock buffers and premium freight*")
    
    # Check for context from Accuracy Hotspots drill-down
    scenario_context = st.session_state.get("scenario_context", {})
    selected_family_from_context = scenario_context.get("sku_family")
    
    if selected_family_from_context:
        st.success(f"📊 **Context loaded from Accuracy Hotspots:** Analyzing {selected_family_from_context.title()} family")
        if st.button("Clear context", key="clear_accuracy_context"):
            st.session_state.scenario_context = {}
            st.rerun()
    
    # Load data
    mape_result = get_mape_by_family_region(cfg, use_mock)
    df = mape_result.df.copy()
    
    if len(df) == 0 or "avg_mape" not in df.columns:
        st.info("No MAPE data available. Run the data pipeline to generate forecast accuracy metrics.")
        return
    
    # If context provided, filter to that family
    if selected_family_from_context and "sku_family" in df.columns:
        df_filtered = df[df["sku_family"] == selected_family_from_context]
        if len(df_filtered) > 0:
            baseline_mape = float(df_filtered["avg_mape"].mean())
        else:
            baseline_mape = float(df["avg_mape"].mean())
    else:
        baseline_mape = float(df["avg_mape"].mean())
    
    top_families = df.sort_values("avg_mape", ascending=False)["sku_family"].head(3).tolist() if "sku_family" in df.columns else []
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.markdown("#### Scenario Parameters")
        
        # Family filter
        families = ["All Families"] + sorted(df["sku_family"].unique().tolist()) if "sku_family" in df.columns else ["All Families"]
        default_idx = 0
        if selected_family_from_context and selected_family_from_context in families:
            default_idx = families.index(selected_family_from_context)
        
        selected_family = st.selectbox(
            "SKU Family Focus",
            families,
            index=default_idx,
            key="accuracy_family_filter"
        )
        
        # Update baseline MAPE based on selection
        if selected_family != "All Families" and "sku_family" in df.columns:
            family_df = df[df["sku_family"] == selected_family]
            if len(family_df) > 0:
                baseline_mape = float(family_df["avg_mape"].mean())
        
        improvement = st.slider(
            "MAPE Improvement Target (%)",
            min_value=5,
            max_value=40,
            value=15,
            step=5,
            key="accuracy_improvement"
        )
        
        implementation_months = st.slider(
            "Implementation Timeline (months)",
            min_value=3,
            max_value=18,
            value=6,
            key="accuracy_timeline"
        )
        
        investment = st.number_input(
            "Estimated Investment ($)",
            min_value=0,
            max_value=1000000,
            value=150000,
            step=25000,
            key="accuracy_investment"
        )
        
        st.markdown("---")
        
        # Update top_families based on selection
        if selected_family != "All Families":
            focus_family = [selected_family]
        else:
            focus_family = top_families
        
        recommendations = _get_accuracy_recommendations(improvement, baseline_mape, focus_family)
        _render_recommendations(recommendations)
    
    with col2:
        improved_mape = baseline_mape * (1 - improvement / 100.0)
        
        # Illustrative impact calculations
        est_safety_stock_reduction = 0.6 * (improvement / 100.0)
        est_premium_freight_reduction = 0.8 * (improvement / 100.0)
        est_annual_savings = investment * 2.5 * (improvement / 20)  # Rough estimate
        roi_months = investment / (est_annual_savings / 12) if est_annual_savings > 0 else 99
        
        st.markdown("#### Projected Impact")
        
        # KPI cards
        kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
        
        with kpi_col1:
            st.markdown(_create_metric_card(
                f"{baseline_mape*100:.1f}%",
                "Current MAPE"
            ), unsafe_allow_html=True)
        
        with kpi_col2:
            st.markdown(_create_metric_card(
                f"{improved_mape*100:.1f}%",
                "Target MAPE",
                -improvement
            ), unsafe_allow_html=True)
        
        with kpi_col3:
            st.markdown(_create_metric_card(
                f"${est_annual_savings/1e3:.0f}K",
                "Est. Annual Savings"
            ), unsafe_allow_html=True)
        
        # Impact breakdown
        st.markdown("#### Downstream Impact")
        
        impact_col1, impact_col2, impact_col3 = st.columns(3)
        with impact_col1:
            st.metric("Safety Stock Reduction", f"{est_safety_stock_reduction*100:.0f}%", delta=f"-{est_safety_stock_reduction*100:.0f}%")
        with impact_col2:
            st.metric("Premium Freight Reduction", f"{est_premium_freight_reduction*100:.0f}%", delta=f"-{est_premium_freight_reduction*100:.0f}%")
        with impact_col3:
            st.metric("ROI Timeline", f"{roi_months:.1f} months")
        
        # MAPE improvement trajectory
        st.markdown("#### Improvement Trajectory")
        
        months = list(range(1, implementation_months + 13))
        baseline = [baseline_mape * 100] * len(months)
        improved = []
        for m in months:
            if m <= implementation_months:
                progress = m / implementation_months
                reduction = progress * (improvement / 100)
            else:
                reduction = improvement / 100
            improved.append(baseline_mape * 100 * (1 - reduction))
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=months, y=baseline, mode='lines', name='Baseline', line=dict(color='#ef4444', dash='dash')))
        fig.add_trace(go.Scatter(x=months, y=improved, mode='lines', name='With Improvement', line=dict(color='#10b981', width=3), fill='tonexty', fillcolor='rgba(16, 185, 129, 0.1)'))
        
        fig.update_layout(
            height=280,
            margin=dict(l=60, r=20, t=20, b=50),
            xaxis_title="Month",
            yaxis_title="MAPE (%)",
            legend=dict(orientation='h', y=1.1),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="#f8f9fa",
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Focus areas table
        st.markdown("#### Priority Focus Areas (Highest MAPE)")
        st.dataframe(df.head(10), use_container_width=True, hide_index=True)
    
    render_action_hint(
        title="Action this enables",
        body="As MAPE improves, planners can safely reduce safety stock buffers and avoid reactive premium freight—directly improving working capital and logistics costs.",
    )


# =============================================================================
# TAB 2: Freight Optimization Scenario
# =============================================================================
def _get_freight_recommendations(cost_reduction: int, top_lanes: list, avg_co2: float) -> list:
    """Generate recommendations for freight optimization."""
    recommendations = []
    
    if cost_reduction >= 20:
        recommendations.append({
            "icon": "🚛",
            "title": "Mode Shift Analysis",
            "desc": "Evaluate rail/intermodal for long-haul lanes",
            "priority": "high"
        })
    
    if top_lanes:
        recommendations.append({
            "icon": "📍",
            "title": f"Focus: {top_lanes[0]}",
            "desc": "Highest-cost lane—consolidation opportunity",
            "priority": "high"
        })
    
    recommendations.append({
        "icon": "📦",
        "title": "Shipment Consolidation",
        "desc": "Combine partial loads for better trailer utilization",
        "priority": "high"
    })
    
    if avg_co2 > 50:
        recommendations.append({
            "icon": "🌱",
            "title": "Sustainability Impact",
            "desc": f"High CO₂ ({avg_co2:.0f} kg/ton)—prioritize green carriers",
            "priority": "medium"
        })
    
    recommendations.append({
        "icon": "🤝",
        "title": "Carrier Negotiation",
        "desc": "Volume commitments for better rates on top lanes",
        "priority": "medium"
    })
    
    recommendations.append({
        "icon": "📊",
        "title": "Regional Sourcing",
        "desc": "Reduce long-haul by serving from closer plants",
        "priority": "medium"
    })
    
    recommendations.append({
        "icon": "⏰",
        "title": "Lead Time Buffer",
        "desc": "Extend lead times to enable ground vs. expedited",
        "priority": "low"
    })
    
    return recommendations[:6]


def _render_freight_scenario(cfg: AppConfig, use_mock: bool) -> None:
    """Render the freight optimization scenario."""
    
    st.markdown("### Freight Cost Optimization")
    st.markdown("*Reduce transportation costs through lane optimization and mode shifts*")
    
    # Load data
    lanes_result = get_freight_lanes(cfg, use_mock)
    ctl_result = get_control_tower_weekly(cfg, use_mock)
    
    lanes_df = lanes_result.df.copy()
    ctl_df = ctl_result.df.copy()
    
    if len(lanes_df) == 0:
        st.info("No freight lane data available.")
        return
    
    baseline_freight = lanes_df["freight_cost_per_ton"].mean() if "freight_cost_per_ton" in lanes_df.columns else 150
    avg_co2 = lanes_df["co2_kg_per_ton"].mean() if "co2_kg_per_ton" in lanes_df.columns else 45
    top_lanes = lanes_df.sort_values("freight_cost_per_ton", ascending=False).head(3).apply(
        lambda r: f"{r.get('plant_name', 'Plant')} → {r.get('dc_name', 'DC')}", axis=1
    ).tolist() if len(lanes_df) else []
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.markdown("#### Scenario Parameters")
        
        cost_reduction = st.slider(
            "Freight Cost Reduction Target (%)",
            min_value=5,
            max_value=30,
            value=15,
            step=5,
            key="freight_reduction"
        )
        
        lanes_to_optimize = st.slider(
            "Number of Lanes to Optimize",
            min_value=5,
            max_value=50,
            value=20,
            key="lanes_count"
        )
        
        mode_shift_pct = st.slider(
            "Mode Shift to Rail/Intermodal (%)",
            min_value=0,
            max_value=40,
            value=10,
            key="mode_shift"
        )
        
        st.markdown("---")
        
        recommendations = _get_freight_recommendations(cost_reduction, top_lanes, avg_co2)
        _render_recommendations(recommendations)
    
    with col2:
        improved_freight = baseline_freight * (1 - cost_reduction / 100.0)
        total_lanes = len(lanes_df)
        est_annual_savings = baseline_freight * total_lanes * 52 * (cost_reduction / 100) * 100  # Rough estimate
        co2_reduction = avg_co2 * mode_shift_pct / 100 * 0.3  # Rail is ~30% lower CO2
        
        st.markdown("#### Projected Impact")
        
        kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
        
        with kpi_col1:
            st.markdown(_create_metric_card(
                f"${baseline_freight:.0f}",
                "Current $/ton"
            ), unsafe_allow_html=True)
        
        with kpi_col2:
            st.markdown(_create_metric_card(
                f"${improved_freight:.0f}",
                "Target $/ton",
                -cost_reduction
            ), unsafe_allow_html=True)
        
        with kpi_col3:
            st.markdown(_create_metric_card(
                f"${est_annual_savings/1e6:.1f}M",
                "Est. Annual Savings"
            ), unsafe_allow_html=True)
        
        # Sustainability impact
        st.markdown("#### Sustainability Co-Benefits")
        
        sus_col1, sus_col2 = st.columns(2)
        with sus_col1:
            st.metric("CO₂ Reduction", f"{co2_reduction:.1f} kg/ton", delta=f"-{co2_reduction/avg_co2*100:.0f}%")
        with sus_col2:
            st.metric("Lanes Shifted to Rail", f"{int(total_lanes * mode_shift_pct / 100)}")
        
        # Top cost lanes chart
        st.markdown("#### Highest-Cost Freight Lanes")
        
        top_10 = lanes_df.sort_values("freight_cost_per_ton", ascending=False).head(10)
        if len(top_10):
            top_10["lane"] = top_10.apply(lambda r: f"{r.get('plant_name', 'P')[:10]} → {r.get('dc_name', 'D')[:10]}", axis=1)
            
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=top_10["lane"],
                y=top_10["freight_cost_per_ton"],
                marker_color='#ef4444',
                name='Current'
            ))
            fig.add_trace(go.Bar(
                x=top_10["lane"],
                y=top_10["freight_cost_per_ton"] * (1 - cost_reduction/100),
                marker_color='#10b981',
                name='Target'
            ))
            
            fig.update_layout(
                barmode='group',
                height=300,
                margin=dict(l=60, r=20, t=20, b=100),
                xaxis_tickangle=-45,
                yaxis_title="Freight Cost ($/ton)",
                legend=dict(orientation='h', y=1.1),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="#f8f9fa",
            )
            st.plotly_chart(fig, use_container_width=True)
    
    render_action_hint(
        title="Action this enables",
        body="Use lane cost analysis to prioritize optimization: consolidate shipments, negotiate carrier rates, and shift modes on high-cost long-haul routes.",
    )


# =============================================================================
# TAB 3: Inventory Planning Scenario
# =============================================================================
def _get_inventory_recommendations(ss_reduction: int, current_dos: float, target_dos: float) -> list:
    """Generate recommendations for inventory optimization."""
    recommendations = []
    
    if ss_reduction >= 20:
        recommendations.append({
            "icon": "⚠️",
            "title": "Risk Assessment",
            "desc": "Validate service level impact before aggressive cuts",
            "priority": "high"
        })
    
    if current_dos > 45:
        recommendations.append({
            "icon": "📦",
            "title": "High Inventory",
            "desc": f"{current_dos:.0f} days of supply—excess buffer opportunity",
            "priority": "high"
        })
    
    recommendations.append({
        "icon": "🎯",
        "title": "SKU Segmentation",
        "desc": "Different policies for A/B/C items by velocity",
        "priority": "high"
    })
    
    recommendations.append({
        "icon": "📊",
        "title": "Demand Sensing",
        "desc": "Real-time signals reduce required safety stock",
        "priority": "medium"
    })
    
    recommendations.append({
        "icon": "🔄",
        "title": "Review Frequency",
        "desc": "Weekly vs monthly reorder points for fast movers",
        "priority": "medium"
    })
    
    recommendations.append({
        "icon": "🤝",
        "title": "Supplier Collaboration",
        "desc": "VMI/consignment for high-volume items",
        "priority": "low"
    })
    
    recommendations.append({
        "icon": "📈",
        "title": "Service Level Trade-offs",
        "desc": "Optimize by segment—99.5% for A, 95% for C items",
        "priority": "low"
    })
    
    return recommendations[:6]


def _render_inventory_scenario(cfg: AppConfig, use_mock: bool) -> None:
    """Render the inventory planning scenario."""
    
    st.markdown("### Inventory & Safety Stock Optimization")
    st.markdown("*Reduce working capital while maintaining service levels*")
    
    # Mock baseline data (in real app, would come from inventory tables)
    baseline_dos = 42  # days of supply
    baseline_ss_value = 12_500_000  # safety stock value
    service_level = 0.97
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.markdown("#### Scenario Parameters")
        
        ss_reduction = st.slider(
            "Safety Stock Reduction Target (%)",
            min_value=5,
            max_value=35,
            value=15,
            step=5,
            key="ss_reduction"
        )
        
        target_service = st.slider(
            "Target Service Level (%)",
            min_value=90.0,
            max_value=99.5,
            value=97.0,
            step=0.5,
            key="target_service"
        )
        
        review_frequency = st.selectbox(
            "Inventory Review Frequency",
            ["Monthly", "Bi-weekly", "Weekly", "Daily"],
            index=2,
            key="review_freq"
        )
        
        st.markdown("---")
        
        target_dos = baseline_dos * (1 - ss_reduction / 100)
        recommendations = _get_inventory_recommendations(ss_reduction, baseline_dos, target_dos)
        _render_recommendations(recommendations)
    
    with col2:
        new_ss_value = baseline_ss_value * (1 - ss_reduction / 100)
        working_capital_freed = baseline_ss_value - new_ss_value
        carrying_cost_saved = working_capital_freed * 0.25  # 25% carrying cost
        
        st.markdown("#### Projected Impact")
        
        kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
        
        with kpi_col1:
            st.markdown(_create_metric_card(
                f"{baseline_dos:.0f} days",
                "Current Days of Supply"
            ), unsafe_allow_html=True)
        
        with kpi_col2:
            st.markdown(_create_metric_card(
                f"{target_dos:.0f} days",
                "Target Days of Supply",
                -ss_reduction
            ), unsafe_allow_html=True)
        
        with kpi_col3:
            st.markdown(_create_metric_card(
                f"${working_capital_freed/1e6:.1f}M",
                "Working Capital Freed"
            ), unsafe_allow_html=True)
        
        # Financial impact
        st.markdown("#### Financial Impact")
        
        fin_col1, fin_col2 = st.columns(2)
        with fin_col1:
            st.metric("Annual Carrying Cost Saved", f"${carrying_cost_saved/1e3:.0f}K")
        with fin_col2:
            st.metric("Inventory Turns Improvement", f"+{ss_reduction * 0.15:.1f}")
        
        # Service level trade-off visualization
        st.markdown("#### Service Level vs. Safety Stock Trade-off")
        
        # Generate trade-off curve
        ss_levels = np.linspace(0.5, 1.5, 20)  # 50% to 150% of current
        service_levels = 0.99 - 0.15 * (1 - ss_levels) ** 2  # Illustrative curve
        service_levels = np.clip(service_levels, 0.85, 0.995)
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=ss_levels * 100,
            y=service_levels * 100,
            mode='lines',
            line=dict(color='#6366f1', width=3),
            name='Trade-off Curve'
        ))
        
        # Mark current and target
        fig.add_trace(go.Scatter(
            x=[100],
            y=[service_level * 100],
            mode='markers',
            marker=dict(size=15, color='#ef4444'),
            name='Current'
        ))
        
        fig.add_trace(go.Scatter(
            x=[100 - ss_reduction],
            y=[target_service],
            mode='markers',
            marker=dict(size=15, color='#10b981', symbol='star'),
            name='Target'
        ))
        
        fig.update_layout(
            height=280,
            margin=dict(l=60, r=20, t=20, b=50),
            xaxis_title="Safety Stock Level (% of Current)",
            yaxis_title="Service Level (%)",
            legend=dict(orientation='h', y=1.15),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="#f8f9fa",
        )
        st.plotly_chart(fig, use_container_width=True)
    
    render_action_hint(
        title="Action this enables",
        body="Balance working capital efficiency with service level commitments—use demand accuracy improvements to safely reduce buffers without impacting OTIF.",
    )


# =============================================================================
# TAB 4: Risk Mitigation Scenario
# =============================================================================
def _get_risk_recommendations(mitigation: int, baseline_risk: float, high_risk_count: int) -> list:
    """Generate recommendations for risk mitigation."""
    recommendations = []
    
    if high_risk_count > 50:
        recommendations.append({
            "icon": "🚨",
            "title": "High-Risk Volume",
            "desc": f"{high_risk_count} orders at risk—prioritize top 20%",
            "priority": "high"
        })
    
    if baseline_risk > 0.15:
        recommendations.append({
            "icon": "🔴",
            "title": "Elevated Risk Level",
            "desc": f"{baseline_risk*100:.0f}% avg risk—systematic issue",
            "priority": "high"
        })
    
    recommendations.append({
        "icon": "📦",
        "title": "Capacity Reservation",
        "desc": "Pre-book carrier capacity for high-risk lanes",
        "priority": "high"
    })
    
    recommendations.append({
        "icon": "🔄",
        "title": "Allocation Priority",
        "desc": "Route inventory to at-risk orders first",
        "priority": "high"
    })
    
    recommendations.append({
        "icon": "📊",
        "title": "Daily Risk Review",
        "desc": "Morning standup on top 10 at-risk orders",
        "priority": "medium"
    })
    
    recommendations.append({
        "icon": "🤝",
        "title": "Customer Communication",
        "desc": "Proactive outreach on potential delays",
        "priority": "medium"
    })
    
    recommendations.append({
        "icon": "📈",
        "title": "Model Refinement",
        "desc": "Add leading indicators to improve prediction",
        "priority": "low"
    })
    
    return recommendations[:6]


def _render_risk_scenario(cfg: AppConfig, use_mock: bool) -> None:
    """Render the risk mitigation scenario."""
    
    st.markdown("### Late-Delivery Risk Mitigation")
    st.markdown("*Proactive actions to prevent OTIF misses using ML predictions*")
    
    # Load data
    risk_result = get_order_late_risk(cfg, use_mock)
    df_risk = risk_result.df.copy()
    
    if len(df_risk) == 0 or "late_risk_prob" not in df_risk.columns:
        st.info("No late-risk scoring data available. Run the ML pipeline to generate predictions.")
        return
    
    baseline_risk = float(df_risk["late_risk_prob"].mean())
    high_risk_count = len(df_risk[df_risk["late_risk_prob"] >= 0.5])
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.markdown("#### Scenario Parameters")
        
        mitigation = st.slider(
            "Risk Mitigation Target (%)",
            min_value=10,
            max_value=50,
            value=25,
            step=5,
            key="risk_mitigation"
        )
        
        protection_threshold = st.slider(
            "Protection Threshold (Risk Score)",
            min_value=0.3,
            max_value=0.8,
            value=0.5,
            step=0.1,
            key="protection_threshold"
        )
        
        expedite_budget = st.number_input(
            "Weekly Expedite Budget ($)",
            min_value=0,
            max_value=100000,
            value=25000,
            step=5000,
            key="expedite_budget"
        )
        
        st.markdown("---")
        
        recommendations = _get_risk_recommendations(mitigation, baseline_risk, high_risk_count)
        _render_recommendations(recommendations)
    
    with col2:
        mitigated_risk = baseline_risk * (1 - mitigation / 100.0)
        orders_to_protect = len(df_risk[df_risk["late_risk_prob"] >= protection_threshold])
        otif_impact = mitigation * 0.4  # Rough estimate: 40% of risk reduction translates to OTIF improvement
        
        st.markdown("#### Projected Impact")
        
        kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
        
        with kpi_col1:
            st.markdown(_create_metric_card(
                f"{baseline_risk*100:.1f}%",
                "Current Avg Risk"
            ), unsafe_allow_html=True)
        
        with kpi_col2:
            st.markdown(_create_metric_card(
                f"{mitigated_risk*100:.1f}%",
                "Target Avg Risk",
                -mitigation
            ), unsafe_allow_html=True)
        
        with kpi_col3:
            st.markdown(_create_metric_card(
                f"{orders_to_protect}",
                "Orders to Protect"
            ), unsafe_allow_html=True)
        
        # OTIF impact
        st.markdown("#### Service Level Impact")
        
        otif_col1, otif_col2 = st.columns(2)
        with otif_col1:
            st.metric("Est. OTIF Improvement", f"+{otif_impact:.1f}%")
        with otif_col2:
            st.metric("Premium Freight Avoided", f"${expedite_budget * 0.6:.0f}/week")
        
        # Risk distribution
        st.markdown("#### Current Risk Distribution")
        
        # Create histogram of risk scores
        fig = go.Figure()
        fig.add_trace(go.Histogram(
            x=df_risk["late_risk_prob"],
            nbinsx=20,
            marker_color='#6366f1',
            opacity=0.7,
            name='Order Count'
        ))
        
        # Add threshold line
        fig.add_vline(x=protection_threshold, line_dash="dash", line_color="#ef4444")
        fig.add_annotation(x=protection_threshold, y=1, yref="paper", text=f"Protection<br>Threshold", showarrow=False, font=dict(color="#ef4444"))
        
        fig.update_layout(
            height=280,
            margin=dict(l=60, r=20, t=20, b=50),
            xaxis_title="Late Risk Probability",
            yaxis_title="Order Count",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="#f8f9fa",
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Top risk segments
        if "customer_region" in df_risk.columns and "sku_family" in df_risk.columns:
            st.markdown("#### Top Risk Segments")
            hotspots = (
                df_risk.groupby(["customer_region", "sku_family"], as_index=False)["late_risk_prob"]
                .mean()
                .sort_values("late_risk_prob", ascending=False)
                .head(8)
            )
            st.dataframe(hotspots, use_container_width=True, hide_index=True)
    
    render_action_hint(
        title="Action this enables",
        body="Use ML risk predictions to reserve capacity and prioritize allocation—protect at-risk orders before OTIF misses occur, reducing reactive premium freight.",
    )
