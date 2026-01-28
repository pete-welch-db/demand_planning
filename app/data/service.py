from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import pandas as pd

from config import AppConfig
from data.connection import DatabricksAuthError, get_sql_client
from data import queries
from data import mock_data


@dataclass(frozen=True)
class DataResult:
    df: pd.DataFrame
    source: str  # "mock" | "databricks_sql"
    warning: str | None = None


def _fallback(use_mock: bool, fn_live: Callable[[], pd.DataFrame], fn_mock: Callable[[], pd.DataFrame]) -> DataResult:
    if use_mock:
        return DataResult(df=fn_mock(), source="mock")
    try:
        return DataResult(df=fn_live(), source="databricks_sql")
    except (DatabricksAuthError, Exception) as e:
        return DataResult(df=fn_mock(), source="mock", warning=f"Fell back to mock data: {type(e).__name__}")


def get_control_tower_weekly(cfg: AppConfig, use_mock: bool) -> DataResult:
    client = get_sql_client(cfg)

    return _fallback(
        use_mock,
        fn_live=lambda: client.query(queries.q_weekly_control_tower(cfg)),
        fn_mock=lambda: mock_data.control_tower_weekly_mock(),
    )


def get_mape_by_family_region(cfg: AppConfig, use_mock: bool) -> DataResult:
    client = get_sql_client(cfg)
    return _fallback(
        use_mock,
        fn_live=lambda: client.query(queries.q_mape_by_family_region(cfg)),
        fn_mock=lambda: mock_data.mape_by_family_region_mock(),
    )


def get_demand_vs_forecast(cfg: AppConfig, use_mock: bool) -> DataResult:
    client = get_sql_client(cfg)
    return _fallback(
        use_mock,
        fn_live=lambda: client.query(queries.q_weekly_demand_vs_forecast(cfg)),
        fn_mock=lambda: mock_data.demand_vs_forecast_mock(),
    )


def get_order_late_risk(cfg: AppConfig, use_mock: bool) -> DataResult:
    client = get_sql_client(cfg)
    return _fallback(
        use_mock,
        fn_live=lambda: client.query(queries.q_order_late_risk(cfg)),
        fn_mock=lambda: mock_data.order_late_risk_mock(),
    )

