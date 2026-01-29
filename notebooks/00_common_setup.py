# Databricks notebook source
# MAGIC %md
# MAGIC ## 0) Common Setup (Azure Databricks / Lakehouse best practices)
# MAGIC
# MAGIC Centralizes parameters (catalog/schema, scale) and shared helper functions used by the rest of the demo notebooks.
# MAGIC
# MAGIC **Best practices baked in**
# MAGIC - Use **Unity Catalog** objects when available (catalog + schema).
# MAGIC - Keep demo idempotent (safe re-runs).

# COMMAND ----------
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

from pyspark.sql import functions as F

# COMMAND ----------
# Widgets (easy for live demo)
dbutils.widgets.text("catalog", "welch")  # set to your UC catalog (or "hive_metastore")
dbutils.widgets.text("schema", "demand_planning_demo")
dbutils.widgets.text("run_id", "demo_run")  # include in table names if you want multiple isolated runs

dbutils.widgets.dropdown("include_external_signals", "true", ["true", "false"])

# Scale controls (defaults are moderate + fast)
dbutils.widgets.text("years", "3")
dbutils.widgets.text("num_plants", "10")
dbutils.widgets.text("num_dcs", "10")
dbutils.widgets.text("num_skus", "400")
dbutils.widgets.text("num_customers", "1500")
dbutils.widgets.text("orders_per_day", "300")


# COMMAND ----------
@dataclass(frozen=True)
class DemoConfig:
    catalog: str
    schema: str
    run_id: str
    include_external_signals: bool
    years: int
    num_plants: int
    num_dcs: int
    num_skus: int
    num_customers: int
    orders_per_day: int

    @property
    def fq_schema(self) -> str:
        return f"`{self.catalog}`.`{self.schema}`"

    def table(self, base_name: str) -> str:
        # Keep names stable for demo; optionally isolate runs by suffixing run_id.
        # If you'd rather isolate runs: return f"{self.fq_schema}.`{base_name}_{self.run_id}`"
        return f"{self.fq_schema}.`{base_name}`"


def get_config() -> DemoConfig:
    return DemoConfig(
        catalog=dbutils.widgets.get("catalog").strip(),
        schema=dbutils.widgets.get("schema").strip(),
        run_id=dbutils.widgets.get("run_id").strip(),
        include_external_signals=dbutils.widgets.get("include_external_signals").lower() == "true",
        years=int(dbutils.widgets.get("years")),
        num_plants=int(dbutils.widgets.get("num_plants")),
        num_dcs=int(dbutils.widgets.get("num_dcs")),
        num_skus=int(dbutils.widgets.get("num_skus")),
        num_customers=int(dbutils.widgets.get("num_customers")),
        orders_per_day=int(dbutils.widgets.get("orders_per_day")),
    )


cfg = get_config()
print(cfg)

# COMMAND ----------
# Create catalog/schema (Unity Catalog). If you're using hive_metastore, catalog creation is not applicable.
if cfg.catalog.lower() != "hive_metastore":
    # Catalog creation may require elevated privileges; best-effort for demo setup.
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{cfg.catalog}`")
    except Exception as e:
        print("WARN: could not create catalog (may already exist or insufficient privileges):", e)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.fq_schema}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Helper: demo date range

# COMMAND ----------
def demo_date_bounds(years: int) -> Dict[str, str]:
    """
    Returns ISO date strings for start/end inclusive.
    We anchor the end date to "today - 1 day" so the demo feels current.
    """
    end = spark.sql("SELECT date_sub(current_date(), 1) AS d").collect()[0]["d"]
    start = spark.sql(f"SELECT add_months(date_sub(current_date(), 1), -12*{years}) AS d").collect()[0]["d"]
    return {"start_date": str(start), "end_date": str(end)}


bounds = demo_date_bounds(cfg.years)
display(spark.createDataFrame([bounds]))

# COMMAND ----------
# MAGIC %md
# MAGIC ### Helper: robust MAPE with explicit zero-demand handling

# COMMAND ----------
def mape_expr(actual_col: str, forecast_col: str) -> str:
    """
    SQL expression for MAPE with explicit zero-demand handling:
    - if actual = 0 and forecast = 0 => 0
    - if actual = 0 and forecast > 0 => 1 (100% error)
    - else abs(actual-forecast)/actual
    """
    return f"""
      CASE
        WHEN {actual_col} = 0 AND {forecast_col} = 0 THEN 0.0
        WHEN {actual_col} = 0 AND {forecast_col} <> 0 THEN 1.0
        ELSE abs({actual_col} - {forecast_col}) / {actual_col}
      END
    """


print(mape_expr("actual_units", "forecast_units"))

