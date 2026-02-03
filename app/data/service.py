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


def get_mape_by_sku(cfg: AppConfig, use_mock: bool, sku_family: str = None) -> DataResult:
    """Get SKU-level MAPE data, optionally filtered by family."""
    client = get_sql_client(cfg)
    return _fallback(
        use_mock,
        fn_live=lambda: client.query(queries.q_mape_by_sku(cfg, sku_family)),
        fn_mock=lambda: mock_data.mape_by_sku_mock(sku_family),
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


def get_plant_locations(cfg: AppConfig, use_mock: bool) -> DataResult:
    client = get_sql_client(cfg)
    return _fallback(
        use_mock,
        fn_live=lambda: client.query(queries.q_plant_locations(cfg)),
        fn_mock=lambda: mock_data.plant_locations_mock(),
    )


def get_dc_locations(cfg: AppConfig, use_mock: bool) -> DataResult:
    client = get_sql_client(cfg)
    return _fallback(
        use_mock,
        fn_live=lambda: client.query(queries.q_dc_locations(cfg)),
        fn_mock=lambda: mock_data.dc_locations_mock(),
    )


def get_freight_lanes(cfg: AppConfig, use_mock: bool) -> DataResult:
    client = get_sql_client(cfg)
    return _fallback(
        use_mock,
        fn_live=lambda: client.query(queries.q_freight_lanes(cfg)),
        fn_mock=lambda: mock_data.freight_lanes_mock(),
    )


def get_order_volume_kpis(cfg: AppConfig, use_mock: bool) -> DataResult:
    """Get order volume and customer metrics (13 weeks)."""
    client = get_sql_client(cfg)
    return _fallback(
        use_mock,
        fn_live=lambda: client.query(queries.q_order_volume_kpis(cfg)),
        fn_mock=lambda: mock_data.order_volume_kpis_mock(),
    )


def get_service_performance_kpis(cfg: AppConfig, use_mock: bool) -> DataResult:
    """Get service performance KPIs (perfect order rate, cancellation, backorder)."""
    client = get_sql_client(cfg)
    return _fallback(
        use_mock,
        fn_live=lambda: client.query(queries.q_service_performance_kpis(cfg)),
        fn_mock=lambda: mock_data.service_performance_kpis_mock(),
    )


def get_transport_mode_comparison(cfg: AppConfig, use_mock: bool) -> DataResult:
    """Get transport mode cost and CO2 comparison."""
    client = get_sql_client(cfg)
    return _fallback(
        use_mock,
        fn_live=lambda: client.query(queries.q_transport_mode_comparison(cfg)),
        fn_mock=lambda: mock_data.transport_mode_comparison_mock(),
    )


def get_product_family_mix(cfg: AppConfig, use_mock: bool) -> DataResult:
    """Get product family volume distribution."""
    client = get_sql_client(cfg)
    return _fallback(
        use_mock,
        fn_live=lambda: client.query(queries.q_product_family_mix(cfg)),
        fn_mock=lambda: mock_data.product_family_mix_mock(),
    )


def get_orders_by_channel(cfg: AppConfig, use_mock: bool) -> DataResult:
    """Get orders and OTIF by sales channel."""
    client = get_sql_client(cfg)
    return _fallback(
        use_mock,
        fn_live=lambda: client.query(queries.q_orders_by_channel(cfg)),
        fn_mock=lambda: mock_data.orders_by_channel_mock(),
    )

