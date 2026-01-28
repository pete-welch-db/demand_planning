# Databricks notebook source
# MAGIC %md
# MAGIC ## 6) Dashboard — Sustainability View
# MAGIC
# MAGIC Focus:
# MAGIC - CO₂ per ton shipped by region and mode
# MAGIC - Energy intensity per unit by plant and sku_family
# MAGIC - Trend lines over time (improvement potential)

# COMMAND ----------
# MAGIC %run ./00_setup

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------
# MAGIC %md
# MAGIC ### CO₂ per ton shipped (weekly, by mode)

# COMMAND ----------
ship = spark.table(cfg.table("tms_shipments"))

# Map DC to region using ERP orders (good enough for demo)
dc_region = (
    spark.table(cfg.table("erp_orders"))
    .select("dc_id", F.col("customer_region").alias("region"))
    .groupBy("dc_id", "region")
    .count()
)

dc_region = (
    dc_region
    .withColumn("rn", F.row_number().over(Window.partitionBy("dc_id").orderBy(F.desc("count"))))
    .where("rn = 1")
    .select("dc_id", "region")
)

co2_weekly = (
    ship.withColumn("week", F.date_trunc("week", F.col("ship_date")).cast("date"))
        .join(dc_region, on="dc_id", how="left")
        .groupBy("week", "region", "mode")
        .agg(
            F.sum("co2_kg").alias("co2_kg"),
            F.sum("total_weight_tons").alias("total_weight_tons"),
        )
        .withColumn("co2_kg_per_ton", F.col("co2_kg") / F.greatest(F.lit(0.0001), F.col("total_weight_tons")))
        .orderBy("week", "region", "mode")
)

display(co2_weekly)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Energy intensity per unit (weekly)

# COMMAND ----------
energy = spark.table(cfg.table("kpi_energy_intensity_weekly"))
display(energy.orderBy(F.desc("week")).limit(50))

# COMMAND ----------
# MAGIC %md
# MAGIC ### Trend snapshot: last 13 weeks

# COMMAND ----------
recent_weeks = energy.select("week").distinct().orderBy(F.desc("week")).limit(13)

energy_13w = (
    energy.join(recent_weeks, on="week", how="inner")
    .groupBy("plant_id", "sku_family")
    .agg(
        F.avg("energy_kwh_per_unit").alias("avg_energy_kwh_per_unit_13w"),
    )
    .orderBy(F.desc("avg_energy_kwh_per_unit_13w"))
)
display(energy_13w)

