from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv


#
# Shared theme tokens (Databricks demo styling)
# - Centralized here to match the “Scrap Intelligence-style” pattern.
# - Keep in sync with STYLING.md (single source of truth for styling rules).
#
THEME = {
    # Backgrounds (Oat)
    "bg_primary": "#EEEDE9",     # Oat Medium (page background)
    "bg_secondary": "#F9F7F4",   # Oat Light (sidebar / alternate sections)
    "bg_card": "#FFFFFF",       # card surface
    # Accents (Lava + Navy)
    "accent_primary": "#FF3621",    # Lava 600
    "accent_secondary": "#FF5F46",  # Lava 500 (hover)
    "navy_900": "#0B1220",
    "navy_800": "#111C33",
    # Text + borders
    "text_primary": "#111827",
    "text_secondary": "rgba(17, 24, 39, 0.72)",
    "border_color": "#e0dfdc",
    "grid": "rgba(17, 24, 39, 0.10)",
    "shadow": "0 1px 2px rgba(16,24,40,0.06)",
    "radius_px": 10,
    # Status colors
    "success": "#067647",
    "warning": "#F59E0B",
    "danger": "#B42318",
}


@dataclass(frozen=True)
class AppConfig:
    # Required for “real data” mode (Databricks SQL)
    databricks_host: str
    databricks_http_path: str
    databricks_catalog: str
    databricks_schema: str

    # Optional but common
    genie_space_id: Optional[str]
    dashboard_embed_url: Optional[str]

    # Optional auth (not required for mock mode). If unset, real-data queries will fail and fallback will kick in.
    databricks_token: Optional[str]

    # Defaults
    default_use_mock: bool

    @property
    def fq_schema(self) -> str:
        # Unity Catalog fully qualified schema name
        return f"`{self.databricks_catalog}`.`{self.databricks_schema}`"


def _getenv(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name, default)
    if v is None:
        return None
    v = v.strip()
    return v if v else None


def get_config() -> AppConfig:
    """
    Centralized config: this is the ONLY place env vars are read.
    - Loads `.env` if present (local dev)
    - Works with Databricks Apps env var injection
    """
    load_dotenv(override=False)

    return AppConfig(
        databricks_host=_getenv("DATABRICKS_HOST", "https://adb-984752964297111.11.azuredatabricks.net") or "",
        databricks_http_path=_getenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/148ccb90800933a1") or "",
        databricks_catalog=_getenv("DATABRICKS_CATALOG", "welch") or "",
        databricks_schema=_getenv("DATABRICKS_SCHEMA", "demand_planning_demo") or "",
        genie_space_id=_getenv("GENIE_SPACE_ID"),
        dashboard_embed_url=_getenv(
            "DASHBOARD_EMBED_URL",
            "https://adb-984752964297111.11.azuredatabricks.net/embed/dashboardsv3/01f0fd5419f41998aa76722bb82632cb?o=984752964297111",
        ),
        databricks_token=_getenv("DATABRICKS_TOKEN"),
        default_use_mock=(_getenv("USE_MOCK_DATA", "true") or "true").lower() == "true",
    )

