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


def q_mape_by_sku(cfg: AppConfig, sku_family: str = None) -> str:
    """Get SKU-level MAPE data, optionally filtered by family."""
    where_clause = f"WHERE sku_family = '{sku_family}'" if sku_family else ""
    return f"""
    SELECT
      sku_id,
      sku_name,
      sku_family,
      region,
      avg(mape) AS avg_mape,
      count(*) AS weeks_measured
    FROM {cfg.fq_schema}.kpi_mape_sku_weekly
    {where_clause}
    GROUP BY 1, 2, 3, 4
    ORDER BY avg_mape DESC
    LIMIT 100
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


def q_order_volume_kpis(cfg: AppConfig) -> str:
    """Order volume and customer metrics (13 weeks)."""
    return f"""
    SELECT
      COUNT(*) AS total_orders,
      COUNT(DISTINCT customer_id) AS unique_customers,
      COUNT(DISTINCT channel) AS sales_channels,
      SUM(units_ordered) AS total_units_ordered
    FROM {cfg.fq_schema}.silver_erp_orders
    WHERE order_date >= date_sub(current_date(), 91)
    """


def q_service_performance_kpis(cfg: AppConfig) -> str:
    """Service performance metrics including perfect order rate (13 weeks)."""
    return f"""
    SELECT
      ROUND(AVG(CASE WHEN is_cancelled = FALSE AND is_backorder = FALSE AND is_otif = TRUE THEN 1.0 ELSE 0.0 END), 4) AS perfect_order_rate,
      ROUND(AVG(CASE WHEN is_cancelled = TRUE THEN 1.0 ELSE 0.0 END), 4) AS cancellation_rate,
      ROUND(AVG(CASE WHEN is_backorder = TRUE THEN 1.0 ELSE 0.0 END), 4) AS backorder_rate
    FROM {cfg.fq_schema}.silver_erp_orders
    WHERE order_date >= date_sub(current_date(), 91)
    """


def q_transport_mode_comparison(cfg: AppConfig) -> str:
    """Transport mode cost and CO2 comparison."""
    return f"""
    SELECT
      mode AS transport_mode,
      ROUND(SUM(freight_cost_usd) / NULLIF(SUM(total_weight_tons), 0), 2) AS freight_cost_per_ton,
      ROUND(SUM(co2_kg) / NULLIF(SUM(total_weight_tons), 0), 2) AS co2_kg_per_ton,
      SUM(total_weight_tons) AS tons_shipped
    FROM {cfg.fq_schema}.silver_tms_shipments
    WHERE ship_date >= date_sub(current_date(), 91)
    GROUP BY mode
    ORDER BY tons_shipped DESC
    """


def q_product_family_mix(cfg: AppConfig) -> str:
    """Product family volume distribution."""
    return f"""
    SELECT
      sku_family,
      SUM(units_ordered) AS total_units,
      ROUND(SUM(units_ordered) * 100.0 / SUM(SUM(units_ordered)) OVER (), 1) AS pct_of_total
    FROM {cfg.fq_schema}.silver_erp_orders
    WHERE order_date >= date_sub(current_date(), 91)
    GROUP BY sku_family
    ORDER BY total_units DESC
    """


def q_orders_by_channel(cfg: AppConfig) -> str:
    """Orders and OTIF performance by sales channel."""
    return f"""
    SELECT
      channel AS sales_channel,
      COUNT(*) AS order_count,
      SUM(units_ordered) AS units_ordered,
      ROUND(AVG(CASE WHEN is_otif = TRUE THEN 1.0 ELSE 0.0 END), 4) AS otif_rate
    FROM {cfg.fq_schema}.silver_erp_orders
    WHERE order_date >= date_sub(current_date(), 91)
    GROUP BY channel
    ORDER BY order_count DESC
    """

