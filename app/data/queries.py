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


def q_plant_locations(cfg: AppConfig) -> str:
    return f"""
    SELECT
      plant_id,
      plant_name,
      plant_city,
      plant_state,
      plant_region,
      plant_lat,
      plant_lon
    FROM {cfg.fq_schema}.silver_plant_locations
    """


def q_dc_locations(cfg: AppConfig) -> str:
    return f"""
    SELECT
      dc_id,
      dc_name,
      dc_city,
      dc_state,
      dc_region,
      dc_lat,
      dc_lon
    FROM {cfg.fq_schema}.silver_dc_locations
    """


def q_freight_lanes(cfg: AppConfig) -> str:
    return f"""
    WITH lanes AS (
      SELECT
        plant_id,
        dc_id,
        AVG(freight_cost_per_ton) AS freight_cost_per_ton,
        AVG(co2_kg_per_ton) AS co2_kg_per_ton,
        COUNT(*) AS weekly_volume
      FROM {cfg.fq_schema}.control_tower_weekly
      WHERE week >= date_sub(current_date(), 91)
      GROUP BY plant_id, dc_id
    )
    SELECT
      l.plant_id,
      p.plant_name,
      p.plant_lat,
      p.plant_lon,
      l.dc_id,
      d.dc_name,
      d.dc_lat,
      d.dc_lon,
      ROUND(l.freight_cost_per_ton, 2) AS freight_cost_per_ton,
      ROUND(l.co2_kg_per_ton, 1) AS co2_kg_per_ton,
      l.weekly_volume
    FROM lanes l
    JOIN {cfg.fq_schema}.silver_plant_locations p ON l.plant_id = p.plant_id
    JOIN {cfg.fq_schema}.silver_dc_locations d ON l.dc_id = d.dc_id
    ORDER BY freight_cost_per_ton DESC
    """

