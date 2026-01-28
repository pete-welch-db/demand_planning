from __future__ import annotations

import base64
import os
from typing import Optional

import streamlit as st


def _read_asset_b64(rel_path: str) -> Optional[str]:
    here = os.path.dirname(__file__)
    asset_path = os.path.abspath(os.path.join(here, "..", "assets", rel_path))
    if not os.path.exists(asset_path):
        return None
    with open(asset_path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def render_header(app_name: str, subtitle: str, right_pill: str) -> None:
    logo_b64 = _read_asset_b64("databricks_mark.svg")
    logo_html = ""
    if logo_b64:
        logo_html = f'<img src="data:image/svg+xml;base64,{logo_b64}" style="height:20px; width:auto;" />'

    st.markdown(
        f"""
<div class="dbx-header">
  <div class="dbx-header-left">
    {logo_html}
    <div>
      <div class="dbx-title">{app_name}</div>
      <div class="dbx-subtitle">{subtitle}</div>
    </div>
  </div>
  <div class="pill"><span class="dot"></span>{right_pill}</div>
</div>
        """,
        unsafe_allow_html=True,
    )

