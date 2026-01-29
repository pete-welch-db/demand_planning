from __future__ import annotations

# Databricks notebook source
# MAGIC %md
# MAGIC ## 4) Post-forecast KPI refresh (DBSQL / App-ready)
# MAGIC
# MAGIC With the simplified setup, **DLT produces Silver/Gold** tables.
# MAGIC This notebook only creates **post-forecast** analytic views that depend on model outputs (e.g., MAPE).
# MAGIC
# MAGIC **Run order**
# MAGIC 1) `02_generate_bronze` (writes `bronze_*_raw`)
# MAGIC 2) DLT pipeline `dlt_supply_chain_medallion` (produces `silver_*` and `gold_*`)
# MAGIC 3) `03_forecast_weekly_mlflow` (writes `demand_forecast*`)
# MAGIC 4) Run this notebook (refresh MAPE views)

# COMMAND ----------
# MAGIC %run ./00_common_setup

# COMMAND ----------
from pyspark.sql import functions as F


def _safe_drop_any(fq_name: str) -> None:
    # Drop view/table regardless of type (ignore mismatch errors).
    try:
        spark.sql(f"DROP VIEW IF EXISTS {fq_name}")
    except Exception:
        pass
    try:
        spark.sql(f"DROP TABLE IF EXISTS {fq_name}")
    except Exception:
        pass


# COMMAND ----------
# MAGIC %md
# MAGIC ### 4.1 Forecast accuracy (MAPE) — joins Gold actuals to forecast table

# COMMAND ----------
# `demand_forecast` is written by `03_forecast_weekly_mlflow` as a Delta table.
# Create a placeholder if it doesn't exist yet (so this notebook can run safely).
if not spark.catalog.tableExists(cfg.table("demand_forecast").replace("`", "")):
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {cfg.table("demand_forecast")} (
      week DATE,
      sku_family STRING,
      region STRING,
      forecast_units DOUBLE,
      lower_ci DOUBLE,
      upper_ci DOUBLE
    ) USING DELTA
    """)

_safe_drop_any(cfg.table("kpi_mape_weekly"))

spark.sql(f"""
CREATE OR REPLACE VIEW {cfg.table("kpi_mape_weekly")} AS
WITH joined AS (
  SELECT
    a.week,
    a.sku_family,
    a.region,
    a.actual_units,
    f.forecast_units
  FROM {cfg.table("weekly_demand_actual")} a
  INNER JOIN {cfg.table("demand_forecast")} f
    ON a.week = f.week AND a.sku_family = f.sku_family AND a.region = f.region
)
SELECT
  week,
  sku_family,
  region,
  avg({mape_expr("actual_units", "forecast_units")}) AS mape
FROM joined
GROUP BY 1,2,3
""")

display(spark.table(cfg.table("kpi_mape_weekly")).orderBy(F.desc("week")).limit(20))

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4.2 Convenience view: demand actual vs forecast (backtest + future)

# COMMAND ----------
if spark.catalog.tableExists(cfg.table("demand_forecast_future").replace("`", "")):
    _safe_drop_any(cfg.table("demand_vs_forecast_weekly"))
    spark.sql(f"""
    CREATE OR REPLACE VIEW {cfg.table("demand_vs_forecast_weekly")} AS
    WITH a AS (
      SELECT week, sku_family, region, actual_units
      FROM {cfg.table("weekly_demand_actual")}
    ),
    f AS (
      SELECT week, sku_family, region, forecast_units
      FROM {cfg.table("demand_forecast")}
    ),
    ff AS (
      SELECT week, sku_family, region, forecast_units AS forecast_units_future
      FROM {cfg.table("demand_forecast_future")}
    )
    SELECT
      a.week,
      a.sku_family,
      a.region,
      a.actual_units,
      f.forecast_units,
      ff.forecast_units_future
    FROM a
    LEFT JOIN f USING (week, sku_family, region)
    LEFT JOIN ff USING (week, sku_family, region)
    """)

    display(spark.table(cfg.table("demand_vs_forecast_weekly")).orderBy(F.desc("week")).limit(20))

