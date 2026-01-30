from __future__ import annotations

import streamlit as st

from config import THEME


APP_TITLE = "Demand Planning Control Tower (Demo)"


def apply_theme() -> None:
    st.set_page_config(
        page_title=APP_TITLE,
        page_icon="",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Centralized theme tokens (config.py) -> CSS variables (STYLING.md)
    radius = int(THEME["radius_px"])
    css = """
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&display=swap');

:root{
  --lava-600: __LAVA_600__;
  --lava-500: __LAVA_500__;
  --navy-900: __NAVY_900__;
  --navy-800: __NAVY_800__;

  --bg-primary: __BG_PRIMARY__;   /* Oat Medium */
  --bg-secondary: __BG_SECONDARY__; /* Oat Light  */
  --card-bg: __CARD_BG__;
  --card-border: __CARD_BORDER__;

  --text-primary: __TEXT_PRIMARY__;
  --text-secondary: __TEXT_SECONDARY__;
  --grid: __GRID__;
  --shadow: __SHADOW__;
  --radius: __RADIUS_PX__px;
  --spacing: 8px;
}

/* Hide default Streamlit chrome */
#MainMenu { visibility: hidden; }
header { visibility: hidden; }
footer { visibility: hidden; }

/* App background + global typography */
html, body, [data-testid="stAppViewContainer"]{
  background: var(--bg-primary) !important;
  font-family: "DM Sans", system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif !important;
  color: var(--text-primary) !important;
}

/* Sidebar background */
[data-testid="stSidebar"]{
  background: var(--bg-secondary) !important;
  border-right: 1px solid var(--card-border) !important;
}
[data-testid="stSidebar"] *{
  font-family: "DM Sans", system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif !important;
}

/* Sidebar nav (Scrap Intelligence-style buttons) */
[data-testid="stSidebar"] div[role="radiogroup"] > label{
  background: var(--card-bg) !important;
  border: 1px solid var(--card-border) !important;
  border-radius: 12px !important;
  padding: 10px 12px !important;
  margin: 0 0 10px 0 !important;
}
[data-testid="stSidebar"] div[role="radiogroup"] > label:hover{
  border-color: rgba(255, 54, 33, 0.40) !important;
}
/* Selected */
[data-testid="stSidebar"] div[role="radiogroup"] > label:has(input:checked){
  border-color: var(--lava-600) !important;
  box-shadow: 0 1px 3px rgba(255,54,33,0.15) !important;
}

/* Tighter page padding */
.block-container{
  padding-top: 0.75rem !important;
  padding-bottom: 2rem !important;
}

/* Branded header */
.dbx-header{
  display:flex;
  align-items:center;
  justify-content:space-between;
  gap: 12px;
  background: var(--bg-secondary);
  border: 1px solid var(--card-border);
  border-radius: var(--radius);
  box-shadow: var(--shadow);
  padding: 10px 14px;
  margin: 0 0 14px 0;
}
.dbx-header-left{
  display:flex;
  align-items:center;
  gap: 10px;
}
.dbx-title{
  font-size: 20px;
  font-weight: 700;
  color: var(--navy-900);
  line-height: 1.1;
}
.dbx-subtitle{
  font-size: 14px;
  font-weight: 500;
  color: var(--text-secondary);
}
.pill{
  display:inline-flex;
  align-items:center;
  gap:6px;
  background: white;
  border: 1px solid var(--card-border);
  border-radius: 999px;
  padding: 6px 10px;
  font-size: 13px;
  font-weight: 600;
  color: var(--navy-800);
}
.pill .dot{
  width:8px;
  height:8px;
  border-radius:999px;
  background: var(--lava-600);
  display:inline-block;
}

/* Landing hero + value cards (STYLING.md) */
.hero{
  background: var(--bg-secondary);
  border: 1px solid var(--card-border);
  border-radius: var(--radius);
  box-shadow: var(--shadow);
  padding: 18px 18px;
  margin-bottom: 14px;
}
.hero-title{
  font-size: 36px;
  font-weight: 700;
  color: var(--navy-900);
  line-height: 1.05;
  margin: 0 0 6px 0;
}
.hero-narrative{
  font-size: 16px;
  font-weight: 400;
  color: var(--text-secondary);
  line-height: 1.5;
  margin: 0;
}
.section-title{
  font-size: 24px;
  font-weight: 600;
  color: var(--navy-900);
  margin: 14px 0 10px 0;
}
.value-card{
  background: var(--card-bg);
  border: 1px solid var(--card-border);
  border-radius: var(--radius);
  box-shadow: var(--shadow);
  padding: 14px 14px;
}
.value-card-title{
  font-size: 14px;
  font-weight: 600;
  color: var(--navy-800);
  margin-bottom: 8px;
}
.value-card-body{
  font-size: 16px;
  font-weight: 400;
  color: var(--text-secondary);
  line-height: 1.5;
}
.how-step{
  background: var(--bg-secondary);
  border: 1px solid var(--card-border);
  border-radius: var(--radius);
  box-shadow: var(--shadow);
  padding: 14px 14px;
}
.how-step-title{
  font-size: 16px;
  font-weight: 600;
  color: var(--navy-900);
  margin-bottom: 6px;
}
.how-step-body{
  font-size: 14px;
  font-weight: 400;
  color: var(--text-secondary);
  line-height: 1.5;
}

/* Metric cards (STYLING.md classes) */
.metric-card{
  background: var(--card-bg);
  border: 1px solid var(--card-border);
  border-radius: var(--radius);
  box-shadow: var(--shadow);
  padding: 12px 14px;
}
.metric-label{
  font-size: 14px;
  font-weight: 500;
  color: var(--text-secondary);
  margin-bottom: 6px;
}
.metric-value{
  font-size: 24px;
  font-weight: 700;
  color: var(--text-primary);
  line-height: 1.2;
}
.metric-delta{
  margin-top: 6px;
  font-size: 14px;
  font-weight: 600;
}
.metric-delta.positive{ color: __SUCCESS__; }
.metric-delta.negative{ color: __DANGER__; }

/* Buttons */
div.stButton > button{
  border-radius: 10px !important;
  font-weight: 600 !important;
  border: 1px solid transparent !important;
  background: var(--lava-600) !important;
  color: white !important;
}
div.stButton > button:hover{
  background: var(--lava-500) !important;
}

/* Inputs / sliders / expander */
div[data-baseweb="input"] input, textarea{
  border-radius: 10px !important;
}
details{
  background: var(--bg-secondary);
  border: 1px solid var(--card-border);
  border-radius: var(--radius);
  padding: 8px 10px;
}

/* Tabs (pill-style) */
div[data-testid="stTabs"] > div{
  gap: 8px;
}
button[data-baseweb="tab"]{
  border-radius: 999px !important;
  border: 1px solid var(--card-border) !important;
  background: white !important;
  color: var(--navy-800) !important;
  font-weight: 600 !important;
}
button[data-baseweb="tab"][aria-selected="true"]{
  background: var(--lava-600) !important;
  color: white !important;
  border-color: var(--lava-600) !important;
}

/* Misc */
.subtle{ color: var(--text-secondary); font-size: 14px; }

/* Charts: render on card surface (improves contrast on Oat background) */
div[data-testid="stPlotlyChart"]{
  background: var(--card-bg);
  border: 1px solid var(--card-border);
  border-radius: var(--radius);
  box-shadow: var(--shadow);
  padding: 8px 10px;
}

/* Tab narrative components (required per playbook) */
.tab-intro{
  background: var(--bg-secondary);
  border: 1px solid var(--card-border);
  border-radius: var(--radius);
  box-shadow: var(--shadow);
  padding: 14px 14px;
  margin: 0 0 14px 0;
}
.tab-intro-persona{
  font-size: 14px;
  font-weight: 600;
  color: var(--navy-800);
  margin-bottom: 6px;
}
.tab-intro-question{
  font-size: 18px;
  font-weight: 700;
  color: var(--navy-900);
  margin-bottom: 6px;
}
.tab-intro-context{
  font-size: 14px;
  font-weight: 400;
  color: var(--text-secondary);
  line-height: 1.5;
}

.callout{
  border: 1px solid var(--card-border);
  border-radius: var(--radius);
  box-shadow: var(--shadow);
  padding: 12px 14px;
  margin: 10px 0;
}
.callout-title{
  font-size: 14px;
  font-weight: 700;
  color: var(--navy-900);
  margin-bottom: 6px;
}
.callout-body{
  font-size: 14px;
  font-weight: 400;
  color: var(--text-secondary);
  line-height: 1.5;
}
.callout-annot{
  background: #FFFFFF;
  border-left: 4px solid var(--navy-800);
}
.callout-action{
  background: #FFFFFF;
  border-left: 4px solid var(--lava-600);
}
</style>
"""

    tokens = {
        "__LAVA_600__": str(THEME["accent_primary"]),
        "__LAVA_500__": str(THEME["accent_secondary"]),
        "__NAVY_900__": str(THEME["navy_900"]),
        "__NAVY_800__": str(THEME["navy_800"]),
        "__BG_PRIMARY__": str(THEME["bg_primary"]),
        "__BG_SECONDARY__": str(THEME["bg_secondary"]),
        "__CARD_BG__": str(THEME["bg_card"]),
        "__CARD_BORDER__": str(THEME["border_color"]),
        "__TEXT_PRIMARY__": str(THEME["text_primary"]),
        "__TEXT_SECONDARY__": str(THEME["text_secondary"]),
        "__GRID__": str(THEME["grid"]),
        "__SHADOW__": str(THEME["shadow"]),
        "__RADIUS_PX__": str(radius),
        "__SUCCESS__": str(THEME["success"]),
        "__DANGER__": str(THEME["danger"]),
    }
    for k, v in tokens.items():
        css = css.replace(k, v)

    st.markdown(css, unsafe_allow_html=True)

