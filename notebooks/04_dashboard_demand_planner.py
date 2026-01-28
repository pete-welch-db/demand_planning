# Databricks notebook source
# MAGIC %md
# MAGIC ## 4) Dashboard — Demand Planner View
# MAGIC
# MAGIC Focus:
# MAGIC - Weekly demand vs forecast (by sku_family, region)
# MAGIC - MAPE + bias + backorders proxy
# MAGIC - Drill into top “problem” family/region combinations

# COMMAND ----------
# MAGIC %run ./00_setup

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
# Widgets for interactivity
dbutils.widgets.dropdown("region", "Midwest", ["Northeast", "Southeast", "Midwest", "SouthCentral", "West"])
dbutils.widgets.dropdown("sku_family", "pipe", ["pipe", "chambers", "structures"])

region = dbutils.widgets.get("region")
sku_family = dbutils.widgets.get("sku_family")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Demand vs forecast (weekly)

# COMMAND ----------
actual = (
    spark.table(cfg.table("weekly_demand_actual"))
    .where((F.col("region") == region) & (F.col("sku_family") == sku_family))
    .select("week", F.col("actual_units").alias("actual_units"), F.col("shipped_units").alias("shipped_units"))
)

future = (
    spark.table(cfg.table("demand_forecast_future"))
    .where((F.col("region") == region) & (F.col("sku_family") == sku_family))
    .select("week", "forecast_units", "lower_ci", "upper_ci", "model_name")
)

hist_fcst = (
    spark.table(cfg.table("demand_forecast_all"))
    .where((F.col("region") == region) & (F.col("sku_family") == sku_family) & (F.col("is_backtest") == 1.0))
    .select("week", "forecast_units", "model_name")
)

display(actual.orderBy("week"))
display(hist_fcst.orderBy("week"))
display(future.orderBy("week"))

# COMMAND ----------
# MAGIC %md
# MAGIC ### MAPE + bias + backorders proxy (weekly)

# COMMAND ----------
orders = spark.table(cfg.table("erp_orders")).where("order_status <> 'cancelled'")

backorders_weekly = (
    orders
    .where((F.col("customer_region") == region) & (F.col("sku_family") == sku_family))
    .groupBy(F.date_trunc("week", F.col("order_date")).alias("week"))
    .agg(
        F.sum((F.col("units_ordered") - F.col("units_shipped")).cast("double")).alias("backorder_units_proxy"),
        F.sum(F.col("units_ordered").cast("double")).alias("ordered_units"),
    )
    .withColumn("backorder_rate_proxy", F.col("backorder_units_proxy") / F.greatest(F.lit(1.0), F.col("ordered_units")))
)

mape = (
    spark.table(cfg.table("kpi_mape_weekly"))
    .where((F.col("region") == region) & (F.col("sku_family") == sku_family))
    .select("week", "mape")
)

# Simple bias on backtest weeks (forecast - actual) / actual
bias = (
    actual.join(hist_fcst.where(F.col("model_name") == "ridge_time_lags").select("week", "forecast_units"), on="week", how="inner")
    .withColumn(
        "bias",
        F.expr("""
          CASE WHEN actual_units = 0 AND forecast_units = 0 THEN 0.0
               WHEN actual_units = 0 AND forecast_units <> 0 THEN 1.0
               ELSE (forecast_units - actual_units) / actual_units
          END
        """),
    )
    .select("week", "bias")
)

planner_kpis = (
    mape.join(bias, on="week", how="full")
        .join(backorders_weekly.select("week", "backorder_units_proxy", "backorder_rate_proxy"), on="week", how="left")
        .orderBy("week")
)

display(planner_kpis)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Top 10 problem areas (MAPE) across all regions/families

# COMMAND ----------
top_problems = (
    spark.table(cfg.table("kpi_mape_weekly"))
    .groupBy("sku_family", "region")
    .agg(F.avg("mape").alias("avg_mape"))
    .orderBy(F.desc("avg_mape"))
    .limit(10)
)
display(top_problems)

