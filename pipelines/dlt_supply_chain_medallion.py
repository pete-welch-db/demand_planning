# Databricks notebook source
# MAGIC %md
# MAGIC ## Lakeflow Spark Declarative Pipelines (DLT/SDP) — Bronze → Silver → Gold
# MAGIC
# MAGIC Target: manufacturing supply chain visibility + demand planning demo.
# MAGIC
# MAGIC **Setup pattern (no duplication)**
# MAGIC 1) Run `notebooks/01_generate_synthetic_data` to generate **Bronze/raw** Delta tables:
# MAGIC    - `bronze_erp_orders_raw`
# MAGIC    - `bronze_inventory_positions_raw`
# MAGIC    - `bronze_tms_shipments_raw`
# MAGIC    - `bronze_production_output_raw`
# MAGIC    - `bronze_external_signals_raw` (optional)
# MAGIC 2) Run this DLT pipeline to transform **Bronze → Silver → Gold**.
# MAGIC 3) Run ML notebooks after Silver/Gold exist.
# MAGIC
# MAGIC ### Pipeline parameters (Spark conf)
# MAGIC Configure these in the Pipeline UI under **Configuration**:
# MAGIC - `demo.source_catalog` (default: target catalog)
# MAGIC - `demo.source_schema` (default: target schema)
# MAGIC - `demo.include_external_signals` (default `true`)

# COMMAND ----------
import dlt
from pyspark.sql import functions as F

# COMMAND ----------
# Optional: ML scoring into Gold (late delivery risk)
# If the model doesn't exist yet, we gracefully degrade by emitting a heuristic risk score.

# COMMAND ----------
def _conf_bool(key: str, default: bool) -> bool:
    v = spark.conf.get(key, str(default)).strip().lower()
    return v in ("1", "true", "t", "yes", "y")


INCLUDE_EXTERNAL = _conf_bool("demo.include_external_signals", True)

# Source tables (Bronze/raw) location. Default to the pipeline target catalog/schema.
SOURCE_CATALOG = spark.conf.get("demo.source_catalog", spark.conf.get("pipelines.catalog", "")).strip()
SOURCE_SCHEMA = spark.conf.get("demo.source_schema", spark.conf.get("pipelines.target", "")).strip()

def _src_table(name: str) -> str:
    return f"`{SOURCE_CATALOG}`.`{SOURCE_SCHEMA}`.`{name}`"

# COMMAND ----------
# COMMAND ----------
# MAGIC %md
# MAGIC ## BRONZE (raw)
# MAGIC
# MAGIC In this demo, Bronze is generated once (synthetic) and persisted as Delta tables.
# MAGIC This pipeline **reads** those Bronze tables; it does not re-generate them.

# COMMAND ----------
@dlt.view(name="bronze_erp_orders_raw")
def bronze_erp_orders_raw():
    return spark.table(_src_table("bronze_erp_orders_raw"))


@dlt.view(name="bronze_inventory_positions_raw")
def bronze_inventory_positions_raw():
    return spark.table(_src_table("bronze_inventory_positions_raw"))


@dlt.view(name="bronze_tms_shipments_raw")
def bronze_tms_shipments_raw():
    return spark.table(_src_table("bronze_tms_shipments_raw"))


@dlt.view(name="bronze_production_output_raw")
def bronze_production_output_raw():
    return spark.table(_src_table("bronze_production_output_raw"))


@dlt.view(name="bronze_external_signals_raw")
def bronze_external_signals_raw():
    if not INCLUDE_EXTERNAL:
        return spark.createDataFrame([], "date date, region string, construction_index double, precipitation_mm double, avg_temp_c double")
    return spark.table(_src_table("bronze_external_signals_raw"))


# COMMAND ----------
# MAGIC %md
# MAGIC ## SILVER (cleaned/standardized operational tables)

# COMMAND ----------
@dlt.table(
    name="silver_erp_orders",
    comment="Silver: cleaned ERP orders with standardized types and derived service flags.",
)
@dlt.expect_or_drop("nonnegative_shipped", "units_shipped >= 0")
def silver_erp_orders():
    o = dlt.read("bronze_erp_orders_raw")
    return (
        o.withColumn("order_week", F.date_trunc("week", F.col("order_date")).cast("date"))
        .withColumn("is_cancelled", F.col("order_status") == F.lit("cancelled"))
        .withColumn("is_backorder", F.col("order_status") == F.lit("backorder"))
        .withColumn("is_in_full", F.col("units_shipped") == F.col("units_ordered"))
        .withColumn("is_on_time", F.col("actual_delivery_date").isNotNull() & (F.col("actual_delivery_date") <= F.col("requested_delivery_date")))
        .withColumn("is_otif", F.col("is_in_full") & F.col("is_on_time") & (~F.col("is_cancelled")))
    )


@dlt.table(
    name="silver_tms_shipments",
    comment="Silver: cleaned TMS shipments with lane fields + week bucket.",
)
def silver_tms_shipments():
    s = dlt.read("bronze_tms_shipments_raw")
    return (
        s.withColumn("ship_week", F.date_trunc("week", F.col("ship_date")).cast("date"))
        .withColumn("lane_id", F.concat_ws("→", F.col("plant_id"), F.col("dc_id")))
    )


@dlt.table(
    name="silver_inventory_positions",
    comment="Silver: standardized inventory snapshots with location_id/type.",
)
def silver_inventory_positions():
    i = dlt.read("bronze_inventory_positions_raw")
    return (
        i.withColumn("location_id", F.coalesce(F.col("plant_id"), F.col("dc_id")))
        .withColumn("location_type", F.when(F.col("plant_id").isNotNull(), F.lit("plant")).otherwise(F.lit("dc")))
    )


@dlt.table(
    name="silver_production_output",
    comment="Silver: standardized production output with week bucket.",
)
def silver_production_output():
    p = dlt.read("bronze_production_output_raw")
    return p.withColumn("week", F.date_trunc("week", F.col("date")).cast("date"))


@dlt.table(
    name="silver_external_signals",
    comment="Silver: weekly external signals by region (optional).",
)
def silver_external_signals():
    e = dlt.read("bronze_external_signals_raw")
    if not INCLUDE_EXTERNAL:
        return spark.createDataFrame([], "week date, region string, construction_index double, precipitation_mm double, avg_temp_c double")
    return (
        e.withColumn("week", F.date_trunc("week", F.col("date")).cast("date"))
        .groupBy("week", "region")
        .agg(
            F.avg("construction_index").alias("construction_index"),
            F.avg("precipitation_mm").alias("precipitation_mm"),
            F.avg("avg_temp_c").alias("avg_temp_c"),
        )
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## GOLD (KPI + analytics tables powering the Streamlit app)

# COMMAND ----------
@dlt.table(
    name="weekly_demand_actual",
    comment="Gold: weekly demand actuals by sku_family x region (analytics base).",
)
def weekly_demand_actual():
    o = dlt.read("silver_erp_orders").where("order_status <> 'cancelled'")
    return (
        o.groupBy(F.col("order_week").alias("week"), "sku_family", F.col("customer_region").alias("region"))
        .agg(
            F.sum("units_ordered").alias("actual_units"),
            F.sum("units_shipped").alias("shipped_units"),
            F.count("*").alias("order_lines"),
        )
    )


@dlt.table(
    name="kpi_otif_weekly",
    comment="Gold: OTIF rate by week x region x DC.",
)
def kpi_otif_weekly():
    o = dlt.read("silver_erp_orders").where("order_status <> 'cancelled'")
    order_level = (
        o.groupBy("order_id", F.col("customer_region").alias("region"), "dc_id", F.col("order_week").alias("week"))
        .agg(
            F.max(F.col("is_on_time").cast("int")).alias("on_time"),
            F.min(F.col("is_in_full").cast("int")).alias("in_full"),
        )
    )
    return (
        order_level.groupBy("week", "region", "dc_id")
        .agg(
            F.count("*").alias("orders"),
            F.avg(F.when((F.col("on_time") == 1) & (F.col("in_full") == 1), F.lit(1.0)).otherwise(F.lit(0.0))).alias("otif_rate"),
            F.avg(F.when((F.col("on_time") == 1) & (F.col("in_full") == 1), F.lit(1.0)).otherwise(F.lit(0.0))).alias("perfect_order_rate_proxy"),
        )
    )


@dlt.table(
    name="kpi_freight_weekly",
    comment="Gold: freight + CO2 KPIs by week x lane fields x mode.",
)
def kpi_freight_weekly():
    s = dlt.read("silver_tms_shipments")
    return (
        s.groupBy(F.col("ship_week").alias("week"), "dc_id", "plant_id", "mode")
        .agg(
            F.sum("freight_cost_usd").alias("freight_cost_usd"),
            F.sum("total_weight_tons").alias("total_weight_tons"),
            F.sum("co2_kg").alias("co2_kg"),
        )
        .withColumn("freight_cost_per_ton", F.col("freight_cost_usd") / F.expr("nullif(total_weight_tons, 0)"))
        .withColumn("co2_kg_per_ton", F.col("co2_kg") / F.expr("nullif(total_weight_tons, 0)"))
    )


@dlt.table(
    name="kpi_premium_freight_weekly",
    comment="Gold: premium freight share (carrier) by week x plant x DC.",
)
def kpi_premium_freight_weekly():
    f = dlt.read("kpi_freight_weekly")
    return (
        f.groupBy("week", "dc_id", "plant_id")
        .agg(
            (F.sum(F.when(F.col("mode") == "carrier", F.col("freight_cost_usd")).otherwise(F.lit(0.0))) / F.expr("nullif(sum(freight_cost_usd), 0)")).alias(
                "premium_freight_pct"
            )
        )
    )


@dlt.table(
    name="kpi_energy_intensity_weekly",
    comment="Gold: energy intensity by week x plant x sku_family.",
)
def kpi_energy_intensity_weekly():
    p = dlt.read("silver_production_output")
    return (
        p.groupBy("week", "plant_id", "sku_family")
        .agg(F.sum("energy_kwh").alias("energy_kwh"), F.sum("units_produced").alias("units_produced"))
        .withColumn("energy_kwh_per_unit", F.col("energy_kwh") / F.expr("nullif(units_produced, 0)"))
    )


@dlt.table(
    name="control_tower_weekly",
    comment="Gold: consolidated control-tower table powering the Streamlit app.",
)
def control_tower_weekly():
    freight = (
        dlt.read("kpi_freight_weekly")
        .groupBy("week", "dc_id", "plant_id")
        .agg(
            F.sum("freight_cost_usd").alias("freight_cost_usd"),
            F.sum("total_weight_tons").alias("total_weight_tons"),
            F.sum("co2_kg").alias("co2_kg"),
        )
        .withColumn("freight_cost_per_ton", F.col("freight_cost_usd") / F.expr("nullif(total_weight_tons, 0)"))
        .withColumn("co2_kg_per_ton", F.col("co2_kg") / F.expr("nullif(total_weight_tons, 0)"))
    )

    premium = dlt.read("kpi_premium_freight_weekly")
    otif = dlt.read("kpi_otif_weekly")
    energy = dlt.read("kpi_energy_intensity_weekly")

    # Attach region from OTIF (region is defined on ERP side)
    out = (
        freight.join(otif.select("week", "dc_id", "region", "otif_rate"), on=["week", "dc_id"], how="left")
        .join(premium, on=["week", "dc_id", "plant_id"], how="left")
        .join(energy, on=["week", "plant_id"], how="left")
        .select(
            "week",
            "region",
            "plant_id",
            "dc_id",
            "otif_rate",
            "freight_cost_per_ton",
            "premium_freight_pct",
            "co2_kg_per_ton",
            "sku_family",
            "energy_kwh_per_unit",
        )
    )
    return out


@dlt.table(
    name="order_late_risk_scored",
    comment="Gold: late delivery risk scores for recent orders (MLflow model if available; otherwise heuristic fallback).",
)
def order_late_risk_scored():
    """
    This table is intentionally ML-in-loop: it changes decisions (expedite, capacity reservation, allocation).

    Pipeline config (optional):
    - demo.late_risk_model_name: UC model name, e.g. main.demand_planning_demo.order_late_risk_model
    """
    o = dlt.read("silver_erp_orders").where("order_status <> 'cancelled'").select(
        "order_id",
        "order_date",
        "requested_delivery_date",
        F.col("customer_region").alias("customer_region"),
        "channel",
        "dc_id",
        "plant_id",
        "sku_family",
        "units_ordered",
        "unit_price",
        "is_on_time",
    )

    # Feature set (matches training notebook)
    feat = (
        o.withColumn("days_to_request", F.datediff("requested_delivery_date", "order_date").cast("double"))
        .withColumn("dow", F.dayofweek("order_date").cast("double"))
        .withColumn("woy", F.weekofyear("order_date").cast("double"))
        .withColumn("month", F.month("order_date").cast("double"))
        .withColumn("order_week", F.date_trunc("week", F.col("order_date")).cast("date"))
        .withColumn("actual_late", (~F.col("is_on_time")).cast("int"))
    )

    # Join external signals if enabled (weekly, by region)
    if INCLUDE_EXTERNAL:
        ext = dlt.read("silver_external_signals")
        feat = (
            feat.join(ext, (feat.order_week == ext.week) & (feat.customer_region == ext.region), "left")
            .drop(ext.week)
            .drop(ext.region)
        )
    else:
        feat = (
            feat.withColumn("construction_index", F.lit(None).cast("double"))
            .withColumn("precipitation_mm", F.lit(None).cast("double"))
            .withColumn("avg_temp_c", F.lit(None).cast("double"))
        )

    model_name = spark.conf.get("demo.late_risk_model_name", "").strip()

    # Try MLflow model; fallback to heuristic if unavailable
    try:
        if not model_name:
            raise RuntimeError("No demo.late_risk_model_name configured.")

        import mlflow.spark

        # Load the registered Spark ML pipeline model and score (expects raw feature columns).
        m = mlflow.spark.load_model(f"models:/{model_name}/latest")
        pred = m.transform(feat)

        scored = (
            pred.withColumn("late_risk_prob", F.col("probability").getItem(1))
            .withColumn("late_risk_flag", (F.col("late_risk_prob") >= F.lit(0.5)).cast("int"))
        )
    except Exception:
        # Heuristic: higher risk with shorter lead time + high units + contractor/DOT + winter weeks
        scored = (
            feat.withColumn(
                "late_risk_prob",
                F.least(
                    F.lit(0.99),
                    F.greatest(
                        F.lit(0.01),
                        F.lit(0.12)
                        + F.when(F.col("days_to_request") <= 5, F.lit(0.18)).otherwise(F.lit(0.0))
                        + F.when(F.col("channel").isin(["contractor", "DOT"]), F.lit(0.08)).otherwise(F.lit(0.0))
                        + F.when(F.col("units_ordered") >= 50, F.lit(0.10)).otherwise(F.lit(0.0))
                        + F.when(F.col("woy").isin(list(range(1, 9)) + list(range(48, 53))), F.lit(0.06)).otherwise(F.lit(0.0))
                    ),
                ),
            )
            .withColumn("late_risk_flag", (F.col("late_risk_prob") >= F.lit(0.5)).cast("int"))
        )

    return scored.select(
        "order_id",
        "order_date",
        "customer_region",
        "channel",
        "dc_id",
        "plant_id",
        "sku_family",
        "units_ordered",
        "days_to_request",
        "late_risk_prob",
        "late_risk_flag",
        "actual_late",
    )

