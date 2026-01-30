# Databricks notebook source
# MAGIC %md
# MAGIC ## 6) KPI + Metric Refresh (Post-forecast KPIs + UC Metric Views)
# MAGIC
# MAGIC **One notebook, one job step** that:
# MAGIC - Refreshes **post-forecast KPI views** (MAPE + convenience joins)
# MAGIC - Creates/updates **Unity Catalog metric views** (Databricks-native semantic layer)
# MAGIC
# MAGIC ### Prereqs / run order
# MAGIC 1) `02_generate_bronze.py`
# MAGIC 2) Run DLT pipeline (creates Gold: `weekly_demand_actual`, `control_tower_weekly`, etc.)
# MAGIC 3) `03_forecast_weekly_mlflow.py` (creates `demand_forecast*`)
# MAGIC 4) `05_ml_late_risk.py` (creates `order_late_risk_scored_ml`)
# MAGIC 5) Run this notebook

# COMMAND ----------
# MAGIC %run ./00_common_setup

# COMMAND ----------
from pyspark.sql import functions as F

# Set catalog/schema context for DDL operations
spark.sql(f"USE CATALOG `{cfg.catalog}`")
spark.sql(f"USE SCHEMA `{cfg.schema}`")


def _exists(obj_name: str) -> bool:
    try:
        return spark.catalog.tableExists(f"{cfg.catalog}.{cfg.schema}.{obj_name}")
    except Exception:
        return False


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
# MAGIC ### 6.1 Post-forecast KPI views (DBSQL/App-ready)

# COMMAND ----------
if not _exists("weekly_demand_actual"):
    print(f"SKIP kpi views: missing {cfg.fq_schema}.weekly_demand_actual (run DLT)")
else:
    # `demand_forecast` is written by `03_forecast_weekly_mlflow` as a Delta table.
    # Create a placeholder if it doesn't exist yet (so this notebook can run safely).
    if not spark.catalog.tableExists(cfg.table("demand_forecast").replace("`", "")):
        spark.sql(
            f"""
        CREATE TABLE IF NOT EXISTS {cfg.table("demand_forecast")} (
          week DATE,
          sku_family STRING,
          region STRING,
          forecast_units DOUBLE,
          lower_ci DOUBLE,
          upper_ci DOUBLE
        ) USING DELTA
        """
        )

    _safe_drop_any(cfg.table("kpi_mape_weekly"))

    spark.sql(
        f"""
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
    """
    )

    if spark.catalog.tableExists(cfg.table("demand_forecast_future").replace("`", "")):
        _safe_drop_any(cfg.table("demand_vs_forecast_weekly"))
        spark.sql(
            f"""
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
        """
        )

    print("Refreshed KPI views: kpi_mape_weekly, demand_vs_forecast_weekly (if future exists).")


# COMMAND ----------
# MAGIC %md
# MAGIC ### 6.2 Unity Catalog Metric Views (semantic layer)
# MAGIC
# MAGIC Uses SQL: `CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE YAML AS $$ ... $$`

# COMMAND ----------
CATALOG = cfg.catalog
SCHEMA = cfg.schema
FQ = lambda name: f"`{CATALOG}`.`{SCHEMA}`.`{name}`"

# COMMAND ----------
# Control tower KPIs (weekly)
if not _exists("control_tower_weekly"):
    print(f"SKIP mv_control_tower_kpis: missing source {cfg.fq_schema}.control_tower_weekly")
else:
    spark.sql(
        f"""
CREATE OR REPLACE VIEW {FQ("mv_control_tower_kpis")}
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Control tower KPIs (service, cost, sustainability) based on gold.control_tower_weekly."
source: {CATALOG}.{SCHEMA}.control_tower_weekly
dimensions:
  - name: week
    expr: week
  - name: region
    expr: region
  - name: plant_id
    expr: plant_id
  - name: dc_id
    expr: dc_id
  - name: sku_family
    expr: sku_family
measures:
  - name: OTIF
    expr: AVG(otif_rate)
  - name: Freight $/ton
    expr: AVG(freight_cost_per_ton)
  - name: Premium freight %
    expr: AVG(premium_freight_pct)
  - name: CO2 kg/ton
    expr: AVG(co2_kg_per_ton)
  - name: Energy kWh/unit
    expr: AVG(energy_kwh_per_unit)
$$
"""
    )

# Forecast accuracy metric view (MAPE)
if not _exists("kpi_mape_weekly"):
    print(f"SKIP mv_forecast_accuracy: missing source {cfg.fq_schema}.kpi_mape_weekly")
else:
    spark.sql(
        f"""
CREATE OR REPLACE VIEW {FQ("mv_forecast_accuracy")}
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Forecast accuracy (MAPE) based on post-forecast view kpi_mape_weekly."
source: {CATALOG}.{SCHEMA}.kpi_mape_weekly
dimensions:
  - name: week
    expr: week
  - name: sku_family
    expr: sku_family
  - name: region
    expr: region
measures:
  - name: MAPE
    expr: AVG(mape)
$$
"""
    )

# Late risk metric view
if not _exists("order_late_risk_scored_ml"):
    print(f"SKIP mv_late_delivery_risk: missing source {cfg.fq_schema}.order_late_risk_scored_ml")
else:
    spark.sql(
        f"""
CREATE OR REPLACE VIEW {FQ("mv_late_delivery_risk")}
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Late delivery risk KPIs based on scored orders (Notebook 5 output)."
source: {CATALOG}.{SCHEMA}.order_late_risk_scored_ml
dimensions:
  - name: order_date
    expr: order_date
  - name: order_week
    expr: DATE_TRUNC('WEEK', order_date)
  - name: customer_region
    expr: customer_region
  - name: channel
    expr: channel
  - name: plant_id
    expr: plant_id
  - name: dc_id
    expr: dc_id
  - name: sku_family
    expr: sku_family
measures:
  - name: Avg late-risk prob
    expr: AVG(late_risk_prob)
  - name: Observed late rate
    expr: AVG(actual_late)
  - name: Orders scored
    expr: COUNT(1)
$$
"""
    )

print("Metric views refreshed (where sources exist).")

# COMMAND ----------
display(spark.sql(f"SHOW VIEWS IN `{CATALOG}`.`{SCHEMA}`"))

