# Databricks notebook source
# MAGIC %md
# MAGIC ## 5) Dashboard — Logistics & Service View
# MAGIC
# MAGIC Focus:
# MAGIC - OTIF & Perfect Order proxy by region and DC
# MAGIC - Freight cost per ton + premium freight % by lane (plant→DC)
# MAGIC - Top underperforming DCs / lanes

# COMMAND ----------
# MAGIC %run ./00_setup

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
dbutils.widgets.dropdown("region", "Midwest", ["Northeast", "Southeast", "Midwest", "SouthCentral", "West"])
region = dbutils.widgets.get("region")

# COMMAND ----------
# MAGIC %md
# MAGIC ### OTIF by week + DC (filterable by region)

# COMMAND ----------
otif = spark.table(cfg.table("kpi_otif_weekly")).where(F.col("region") == region)
display(otif.orderBy(F.desc("week")))

# COMMAND ----------
# MAGIC %md
# MAGIC ### Worst DCs (recent 13 weeks)

# COMMAND ----------
recent_weeks = (
    otif.select("week").distinct().orderBy(F.desc("week")).limit(13)
)

worst_dcs = (
    otif.join(recent_weeks, on="week", how="inner")
        .groupBy("dc_id")
        .agg(
            F.avg("otif_rate").alias("otif_rate_13w"),
            F.sum("orders").alias("orders_13w"),
        )
        .orderBy(F.asc("otif_rate_13w"))
        .limit(10)
)
display(worst_dcs)

# COMMAND ----------
# MAGIC %md
# MAGIC ### Freight cost per ton, premium freight % (lane = plant→DC)

# COMMAND ----------
freight = spark.table(cfg.table("kpi_freight_weekly"))
premium = spark.table(cfg.table("kpi_premium_freight_weekly"))

lane_weekly = (
    freight.groupBy("week", "plant_id", "dc_id")
    .agg(
        F.sum("freight_cost_usd").alias("freight_cost_usd"),
        F.sum("total_weight_tons").alias("total_weight_tons"),
        F.sum("co2_kg").alias("co2_kg"),
    )
    .withColumn("freight_cost_per_ton", F.col("freight_cost_usd") / F.greatest(F.lit(0.0001), F.col("total_weight_tons")))
    .withColumn("co2_kg_per_ton", F.col("co2_kg") / F.greatest(F.lit(0.0001), F.col("total_weight_tons")))
    .join(premium, on=["week", "plant_id", "dc_id"], how="left")
)

display(lane_weekly.orderBy(F.desc("week")).limit(30))

# COMMAND ----------
# MAGIC %md
# MAGIC ### Top underperforming lanes (recent 13 weeks)

# COMMAND ----------
recent_lane = lane_weekly.join(lane_weekly.select("week").distinct().orderBy(F.desc("week")).limit(13), on="week", how="inner")

worst_lanes = (
    recent_lane.groupBy("plant_id", "dc_id")
    .agg(
        F.avg("freight_cost_per_ton").alias("avg_freight_cost_per_ton_13w"),
        F.avg("premium_freight_pct").alias("avg_premium_freight_pct_13w"),
        F.sum("freight_cost_usd").alias("freight_cost_usd_13w"),
        F.sum("total_weight_tons").alias("tons_13w"),
    )
    .orderBy(F.desc("avg_freight_cost_per_ton_13w"))
    .limit(10)
)
display(worst_lanes)

