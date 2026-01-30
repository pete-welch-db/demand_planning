from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from config import THEME


@dataclass(frozen=True)
class Kpi:
    label: str
    value: str
    delta: Optional[str] = None
    help: Optional[str] = None


def render_kpi_row(kpis: list[Kpi]) -> None:
    cols = st.columns(len(kpis))
    for c, k in zip(cols, kpis):
        with c:
            delta_html = ""
            if k.delta:
                cls = "positive" if str(k.delta).strip().startswith(("+", "▲")) else "negative" if str(k.delta).strip().startswith(("-", "▼")) else ""
                delta_html = f'<div class="metric-delta {cls}">{k.delta}</div>'

            st.markdown(
                f"""
<div class="metric-card">
  <div class="metric-label">{k.label}</div>
  <div class="metric-value">{k.value}</div>
  {delta_html}
</div>
                """,
                unsafe_allow_html=True,
            )


def create_plotly_theme() -> dict:
    """
    Shared Plotly styling (STYLING.md):
    - transparent backgrounds
    - DM Sans
    - branded colorway
    - soft grids
    """
    # For readability in Streamlit, use a white chart surface (cards) rather than transparency.
    return {
        "font_family": "DM Sans, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif",
        "font_color": THEME["text_primary"],
        "paper_bgcolor": THEME["bg_card"],
        "plot_bgcolor": THEME["bg_card"],
        # Brand-forward but readable palette (avoid overly saturated rainbow lines).
        "colorway": [
            THEME["navy_900"],
            THEME["accent_primary"],
            THEME["navy_800"],
            THEME["accent_secondary"],
            "#6B7280",
            "#9CA3AF",
        ],
        "gridcolor": THEME["grid"],
        "axis_linecolor": THEME["border_color"],
        "legend": {
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "xanchor": "left",
            "x": 0,
            "font": {"color": THEME["text_secondary"]},
        },
        "title_font": {"color": THEME["navy_900"], "size": 16},
    }


def apply_plotly_theme(fig: go.Figure, x_title: str, y_title: str) -> go.Figure:
    theme = create_plotly_theme()
    fig.update_layout(
        margin=dict(l=10, r=10, t=44, b=10),
        font=dict(family=theme["font_family"], color=theme["font_color"]),
        paper_bgcolor=theme["paper_bgcolor"],
        plot_bgcolor=theme["plot_bgcolor"],
        colorway=theme["colorway"],
        legend=theme["legend"],
        title_font=theme["title_font"],
    )
    fig.update_traces(line=dict(width=2))
    fig.update_xaxes(
        title_text=x_title,
        gridcolor=theme["gridcolor"],
        zeroline=False,
        linecolor=theme["axis_linecolor"],
        tickfont=dict(color=THEME["text_secondary"]),
        title_font=dict(color=THEME["text_secondary"]),
    )
    fig.update_yaxes(
        title_text=y_title,
        gridcolor=theme["gridcolor"],
        zeroline=False,
        linecolor=theme["axis_linecolor"],
        tickfont=dict(color=THEME["text_secondary"]),
        title_font=dict(color=THEME["text_secondary"]),
    )
    return fig


def line_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    color: Optional[str] = None,
    title: str = "",
    y_format: Optional[str] = None,  # "percent" | "currency" | None
):
    fig = px.line(df, x=x, y=y, color=color, title=title)
    fig = apply_plotly_theme(fig, x_title=x, y_title=y)
    if y_format == "percent":
        fig.update_yaxes(tickformat=".0%")
    elif y_format == "currency":
        fig.update_yaxes(tickprefix="$", separatethousands=True)
    st.plotly_chart(fig, use_container_width=True)

