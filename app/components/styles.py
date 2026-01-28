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
    st.markdown(
        f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&display=swap');

:root{
  --lava-600:{THEME["accent_primary"]};
  --lava-500:{THEME["accent_secondary"]};
  --navy-900:{THEME["navy_900"]};
  --navy-800:{THEME["navy_800"]};

  --bg-primary:{THEME["bg_primary"]};   /* Oat Medium */
  --bg-secondary:{THEME["bg_secondary"]}; /* Oat Light  */
  --card-bg:{THEME["bg_card"]};
  --card-border:{THEME["border_color"]};

  --text-primary:{THEME["text_primary"]};
  --text-secondary:{THEME["text_secondary"]};
  --grid:{THEME["grid"]};
  --shadow:{THEME["shadow"]};
  --radius:{radius}px;
  --spacing:8px;
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
.metric-delta.positive{{ color:{THEME["success"]}; }}
.metric-delta.negative{{ color:{THEME["danger"]}; }}

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
        """,
        unsafe_allow_html=True,
    )

