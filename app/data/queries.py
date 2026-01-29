from __future__ import annotations

from config import AppConfig


def q_weekly_control_tower(cfg: AppConfig) -> str:
    return f"""
    SELECT
      week,
      region,
      plant_id,
      dc_id,
      otif_rate,
      freight_cost_per_ton,
      premium_freight_pct,
      co2_kg_per_ton,
      sku_family,
      energy_kwh_per_unit
    FROM {cfg.fq_schema}.control_tower_weekly
    ORDER BY week DESC
    LIMIT 2000
    """


def q_mape_by_family_region(cfg: AppConfig) -> str:
    return f"""
    SELECT
      sku_family,
      region,
      avg(mape) AS avg_mape
    FROM {cfg.fq_schema}.kpi_mape_weekly
    GROUP BY 1,2
    ORDER BY avg_mape DESC
    LIMIT 50
    """


def q_weekly_demand_vs_forecast(cfg: AppConfig) -> str:
    # demand actuals + ridge backtest forecasts (if present)
    return f"""
    WITH a AS (
      SELECT week, sku_family, region, actual_units
      FROM {cfg.fq_schema}.weekly_demand_actual
    ),
    f AS (
      SELECT week, sku_family, region, forecast_units
      FROM {cfg.fq_schema}.demand_forecast
    )
    SELECT
      a.week,
      a.sku_family,
      a.region,
      a.actual_units,
      f.forecast_units
    FROM a
    LEFT JOIN f USING (week, sku_family, region)
    ORDER BY week DESC
    LIMIT 4000
    """


def q_order_late_risk(cfg: AppConfig) -> str:
    return f"""
    SELECT
      order_date,
      customer_region,
      channel,
      dc_id,
      plant_id,
      sku_family,
      units_ordered,
      days_to_request,
      late_risk_prob,
      late_risk_flag,
      actual_late
    FROM {cfg.fq_schema}.order_late_risk_scored_ml
    ORDER BY order_date DESC
    LIMIT 5000
    """

