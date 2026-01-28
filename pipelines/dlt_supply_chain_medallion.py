# Databricks notebook source
# MAGIC %md
# MAGIC ## Lakeflow Spark Declarative Pipelines (DLT/SDP) — Bronze → Silver → Gold
# MAGIC
# MAGIC Target: manufacturing supply chain visibility + demand planning demo.
# MAGIC
# MAGIC **Bronze**: raw synthetic data (ERP, inventory, shipments, production, optional external signals)  
# MAGIC **Silver**: cleaned, standardized, lightly joined operational tables  
# MAGIC **Gold**: KPI + analytics tables powering the Streamlit app (`control_tower_weekly`, `weekly_demand_actual`, etc.)
# MAGIC
# MAGIC ### Pipeline parameters (Spark conf)
# MAGIC Configure these in the Pipeline UI under **Configuration**:
# MAGIC - `demo.years` (default `3`)
# MAGIC - `demo.num_plants` (default `10`)
# MAGIC - `demo.num_dcs` (default `10`)
# MAGIC - `demo.num_skus` (default `400`)
# MAGIC - `demo.num_customers` (default `1500`)
# MAGIC - `demo.orders_per_day` (default `300`)
# MAGIC - `demo.include_external_signals` (default `true`)

# COMMAND ----------
import dlt
from pyspark.sql import functions as F

# COMMAND ----------
# Optional: ML scoring into Gold (late delivery risk)
# If the model doesn't exist yet, we gracefully degrade by emitting a heuristic risk score.

# COMMAND ----------
# Config helpers (SDP/DLT-friendly: use spark.conf, not dbutils widgets)
def _conf_int(key: str, default: int) -> int:
    try:
        return int(spark.conf.get(key, str(default)))
    except Exception:
        return default


def _conf_bool(key: str, default: bool) -> bool:
    v = spark.conf.get(key, str(default)).strip().lower()
    return v in ("1", "true", "t", "yes", "y")


YEARS = _conf_int("demo.years", 3)
NUM_PLANTS = _conf_int("demo.num_plants", 10)
NUM_DCS = _conf_int("demo.num_dcs", 10)
NUM_SKUS = _conf_int("demo.num_skus", 400)
NUM_CUSTOMERS = _conf_int("demo.num_customers", 1500)
ORDERS_PER_DAY = _conf_int("demo.orders_per_day", 300)
INCLUDE_EXTERNAL = _conf_bool("demo.include_external_signals", True)

REGIONS = ["Northeast", "Southeast", "Midwest", "SouthCentral", "West"]
SKU_FAMILIES = ["pipe", "chambers", "structures"]
CHANNELS = ["distributor", "contractor", "DOT", "ag"]

# COMMAND ----------
# MAGIC %md
# MAGIC ### Utility: date bounds + calendar

# COMMAND ----------
def _date_bounds(years: int):
    end = spark.sql("SELECT date_sub(current_date(), 1) AS d").collect()[0]["d"]
    start = spark.sql(f"SELECT add_months(date_sub(current_date(), 1), -12*{years}) AS d").collect()[0]["d"]
    return str(start), str(end)


START_DATE, END_DATE = _date_bounds(YEARS)


def _calendar_df():
    return (
        spark.sql(
            f"SELECT explode(sequence(to_date('{START_DATE}'), to_date('{END_DATE}'), interval 1 day)) AS date"
        )
        .withColumn("doy", F.dayofyear("date"))
        .withColumn("dow", F.dayofweek("date"))
        .withColumn("week", F.date_trunc("week", F.col("date")).cast("date"))
        .withColumn("season_annual", (F.sin(2 * F.lit(3.1415926535) * F.col("doy") / F.lit(365.25)) + 1.0) / 2.0)
        .withColumn("weekday_factor", F.when(F.col("dow").isin([1, 7]), F.lit(0.85)).otherwise(F.lit(1.0)))
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## BRONZE (raw synthetic)

# COMMAND ----------
@dlt.table(
    name="bronze_dims_plants",
    comment="Bronze: synthetic plant dimension (regional manufacturing footprint).",
)
def bronze_dims_plants():
    return (
        spark.range(NUM_PLANTS)
        .select((F.col("id") + 1).cast("int").alias("plant_n"))
        .withColumn("plant_id", F.format_string("PL%02d", F.col("plant_n")))
        .withColumn(
            "plant_region",
            F.element_at(F.array(*[F.lit(r) for r in REGIONS]), (F.col("plant_n") % F.lit(len(REGIONS))) + 1),
        )
        .drop("plant_n")
    )


@dlt.table(
    name="bronze_dims_dcs",
    comment="Bronze: synthetic DC dimension.",
)
def bronze_dims_dcs():
    return (
        spark.range(NUM_DCS)
        .select((F.col("id") + 1).cast("int").alias("dc_n"))
        .withColumn("dc_id", F.format_string("DC%02d", F.col("dc_n")))
        .withColumn(
            "dc_region",
            F.element_at(F.array(*[F.lit(r) for r in REGIONS]), (F.col("dc_n") % F.lit(len(REGIONS))) + 1),
        )
        .drop("dc_n")
    )


@dlt.table(
    name="bronze_dims_skus",
    comment="Bronze: synthetic SKU dimension.",
)
def bronze_dims_skus():
    return (
        spark.range(NUM_SKUS)
        .select((F.col("id") + 1).cast("int").alias("sku_key"))
        .withColumn("sku_n", F.col("sku_key"))
        .withColumn("sku_id", F.format_string("SKU%05d", F.col("sku_n")))
        .withColumn(
            "sku_family",
            F.when(F.col("sku_n") % 10 < 6, F.lit("pipe"))
            .when(F.col("sku_n") % 10 < 8, F.lit("chambers"))
            .otherwise(F.lit("structures")),
        )
        .withColumn(
            "unit_price",
            F.when(F.col("sku_family") == "pipe", F.lit(75.0) + (F.col("sku_n") % 50) * 1.2)
            .when(F.col("sku_family") == "chambers", F.lit(240.0) + (F.col("sku_n") % 40) * 2.5)
            .otherwise(F.lit(520.0) + (F.col("sku_n") % 30) * 4.0),
        )
        .select("sku_key", "sku_id", "sku_family", "unit_price")
    )


@dlt.table(
    name="bronze_dims_customers",
    comment="Bronze: synthetic customer dimension with region + channel.",
)
def bronze_dims_customers():
    return (
        spark.range(NUM_CUSTOMERS)
        .select((F.col("id") + 1).cast("int").alias("customer_key"))
        .withColumn("cust_n", F.col("customer_key"))
        .withColumn("customer_id", F.format_string("C%06d", F.col("cust_n")))
        .withColumn(
            "customer_region",
            F.element_at(F.array(*[F.lit(r) for r in REGIONS]), (F.col("cust_n") % F.lit(len(REGIONS))) + 1),
        )
        .withColumn(
            "channel",
            F.when(F.col("cust_n") % 10 < 5, F.lit("distributor"))
            .when(F.col("cust_n") % 10 < 8, F.lit("contractor"))
            .when(F.col("cust_n") % 10 < 9, F.lit("DOT"))
            .otherwise(F.lit("ag")),
        )
        .select("customer_key", "customer_id", "customer_region", "channel")
    )


@dlt.table(
    name="bronze_external_signals_raw",
    comment="Bronze: optional synthetic external signals by day+region (construction/weather).",
)
def bronze_external_signals_raw():
    cal = _calendar_df()
    if not INCLUDE_EXTERNAL:
        # Return empty (valid schema) so downstream joins don't break.
        return spark.createDataFrame([], "date date, region string, construction_index double, precipitation_mm double, avg_temp_c double")

    return (
        cal.crossJoin(spark.createDataFrame([(r,) for r in REGIONS], ["region"]))
        .withColumn(
            "construction_index",
            (
                F.lit(85.0)
                + F.col("season_annual") * F.lit(25.0)
                + F.when(F.col("region").isin(["Northeast", "Midwest"]), F.lit(-5.0)).otherwise(F.lit(0.0))
                + F.randn(7) * F.lit(4.0)
            ).cast("double"),
        )
        .withColumn(
            "avg_temp_c",
            (
                F.lit(10.0)
                + F.sin(2 * F.lit(3.1415926535) * F.col("doy") / F.lit(365.25)) * F.lit(12.0)
                + F.when(F.col("region") == "SouthCentral", F.lit(6.0))
                .when(F.col("region") == "West", F.lit(4.0))
                .when(F.col("region") == "Northeast", F.lit(-2.0))
                .otherwise(F.lit(0.0))
                + F.randn(8) * F.lit(2.0)
            ).cast("double"),
        )
        .withColumn(
            "precipitation_mm",
            F.greatest(
                F.lit(0.0),
                (
                    F.lit(2.0)
                    + (1.0 - F.col("season_annual")) * F.lit(4.0)
                    + F.when(F.col("region") == "Southeast", F.lit(1.5)).otherwise(F.lit(0.0))
                    + F.randn(9) * F.lit(2.5)
                ),
            ).cast("double"),
        )
        .select(F.col("date"), "region", "construction_index", "precipitation_mm", "avg_temp_c")
    )


@dlt.table(
    name="bronze_erp_orders_raw",
    comment="Bronze: synthetic ERP order lines (raw).",
)
@dlt.expect_or_drop("valid_units_ordered", "units_ordered >= 0")
@dlt.expect("valid_channel", "channel IN ('distributor','contractor','DOT','ag')")
def bronze_erp_orders_raw():
    cal = _calendar_df().select("date", "season_annual", "weekday_factor")

    total_days = cal.count()
    total_orders = int(total_days * ORDERS_PER_DAY)

    dc_by_region = dlt.read("bronze_dims_dcs").groupBy("dc_region").agg(F.min("dc_id").alias("primary_dc_id")).withColumnRenamed("dc_region", "region")
    plants_by_region = (
        dlt.read("bronze_dims_plants")
        .groupBy("plant_region")
        .agg(F.collect_list("plant_id").alias("plants_in_region"))
        .withColumnRenamed("plant_region", "region")
    )

    raw = (
        spark.range(total_orders)
        .withColumn("order_id", F.format_string("O%010d", F.col("id") + 1))
        .withColumn("order_date", F.expr(f"date_add(to_date('{START_DATE}'), cast(rand(11) * {total_days} as int))"))
        .withColumn("customer_key", (F.floor(F.rand(12) * F.lit(NUM_CUSTOMERS)) + 1).cast("int"))
        .withColumn("sku_key", (F.floor(F.rand(13) * F.lit(NUM_SKUS)) + 1).cast("int"))
        .drop("id")
    )

    orders = (
        raw.join(dlt.read("bronze_dims_customers"), on="customer_key", how="left")
        .join(dlt.read("bronze_dims_skus"), on="sku_key", how="left")
        .join(cal.withColumnRenamed("date", "order_date"), on="order_date", how="left")
        .join(dc_by_region, F.col("customer_region") == F.col("region"), "left")
        .join(plants_by_region, F.col("customer_region") == plants_by_region.region, "left")
        .withColumn("dc_id", F.col("primary_dc_id"))
        .withColumn("plant_id", F.element_at("plants_in_region", (F.floor(F.rand(14) * F.size("plants_in_region")) + 1).cast("int")))
        .drop("primary_dc_id", "plants_in_region", "region")
    )

    if INCLUDE_EXTERNAL:
        ext = dlt.read("bronze_external_signals_raw").withColumnRenamed("date", "order_date").withColumnRenamed("region", "customer_region")
        orders = orders.join(ext, on=["order_date", "customer_region"], how="left")
    else:
        orders = orders.withColumn("construction_index", F.lit(100.0)).withColumn("precipitation_mm", F.lit(2.0)).withColumn("avg_temp_c", F.lit(12.0))

    orders = (
        orders
        .withColumn(
            "base_units",
            F.when(F.col("sku_family") == "pipe", F.lit(22.0))
            .when(F.col("sku_family") == "chambers", F.lit(8.0))
            .otherwise(F.lit(3.5)),
        )
        .withColumn("project_spike", F.when(F.rand(15) < 0.03, F.lit(1.0) + F.rand(16) * 8.0).otherwise(F.lit(1.0)))
        .withColumn(
            "units_ordered",
            F.greatest(
                F.lit(1),
                F.round(
                    F.col("base_units")
                    * (0.75 + 0.7 * F.col("season_annual"))
                    * F.col("weekday_factor")
                    * (0.85 + (F.col("construction_index") / 150.0))
                    * F.col("project_spike")
                    * (0.85 + F.rand(17) * 0.6)
                ).cast("int"),
            ),
        )
        .drop("base_units", "project_spike")
    )

    orders = (
        orders.withColumn(
            "requested_delivery_date",
            F.expr(
                """
          date_add(order_date,
            CASE
              WHEN channel = 'DOT' THEN 10 + cast(rand(21) * 8 as int)
              WHEN channel = 'contractor' THEN 6 + cast(rand(22) * 6 as int)
              WHEN channel = 'ag' THEN 7 + cast(rand(23) * 6 as int)
              ELSE 5 + cast(rand(24) * 6 as int)
            END
          )
        """
            ),
        )
        .withColumn("processing_days", F.when(F.rand(25) < 0.9, F.lit(1)).otherwise(F.lit(2)))
        .withColumn("transit_days", F.when(F.rand(26) < 0.85, F.lit(1)).otherwise(F.lit(2)))
        .withColumn("late_flag", F.rand(27) < 0.08)
        .withColumn("actual_ship_date", F.expr("date_add(order_date, processing_days)"))
        .withColumn(
            "actual_delivery_date",
            F.expr("date_add(actual_ship_date, transit_days + CASE WHEN late_flag THEN 1 + cast(rand(28)*3 as int) ELSE 0 END)"),
        )
        .drop("processing_days", "transit_days", "late_flag")
    )

    orders = (
        orders.withColumn(
            "order_status",
            F.when(F.rand(29) < 0.02, F.lit("cancelled"))
            .when(F.rand(30) < 0.08, F.lit("backorder"))
            .otherwise(F.lit("closed")),
        )
        .withColumn(
            "units_shipped",
            F.when(F.col("order_status") == "cancelled", F.lit(0))
            .when(F.col("order_status") == "backorder", F.round(F.col("units_ordered") * (0.4 + 0.5 * F.rand(31))).cast("int"))
            .otherwise(F.col("units_ordered")),
        )
        .withColumn("actual_ship_date", F.when(F.col("order_status") == "cancelled", F.lit(None).cast("date")).otherwise(F.col("actual_ship_date")))
        .withColumn("actual_delivery_date", F.when(F.col("order_status") == "cancelled", F.lit(None).cast("date")).otherwise(F.col("actual_delivery_date")))
    )

    return orders.select(
        "order_id",
        "order_date",
        "requested_delivery_date",
        "actual_ship_date",
        "actual_delivery_date",
        "customer_id",
        "customer_region",
        "channel",
        "plant_id",
        "dc_id",
        "sku_id",
        "sku_family",
        "units_ordered",
        "units_shipped",
        "unit_price",
        "order_status",
    )


@dlt.table(
    name="bronze_tms_shipments_raw",
    comment="Bronze: synthetic TMS shipments linked to ERP orders (raw).",
)
def bronze_tms_shipments_raw():
    orders = dlt.read("bronze_erp_orders_raw").where("order_status <> 'cancelled' AND actual_ship_date IS NOT NULL")

    tms = (
        orders.select(
            "order_id",
            "plant_id",
            "dc_id",
            F.col("actual_ship_date").alias("ship_date"),
            F.col("actual_delivery_date").alias("delivery_date"),
            "customer_region",
            "sku_family",
            "units_shipped",
            "unit_price",
        )
        .withColumn("shipment_id", F.concat(F.lit("S"), F.expr("substring(order_id, 2)")))
        .withColumn("truck_id", F.format_string("T%05d", (F.floor(F.rand(41) * 1200) + 1).cast("int")))
    )

    region_distance = (
        F.when(F.col("customer_region") == "Northeast", 420)
        .when(F.col("customer_region") == "Southeast", 520)
        .when(F.col("customer_region") == "Midwest", 610)
        .when(F.col("customer_region") == "SouthCentral", 740)
        .otherwise(860)
    )

    tms = (
        tms.withColumn("route_distance_km", (region_distance * (0.7 + 0.7 * F.rand(42))).cast("double"))
        .withColumn(
            "mode",
            F.when(F.col("route_distance_km") < 650, F.when(F.rand(43) < 0.78, F.lit("own_fleet")).otherwise(F.lit("carrier"))).otherwise(
                F.when(F.rand(44) < 0.55, F.lit("own_fleet")).otherwise(F.lit("carrier"))
            ),
        )
    )

    tms = (
        tms.withColumn(
            "unit_weight_ton",
            F.when(F.col("sku_family") == "pipe", F.lit(0.06)).when(F.col("sku_family") == "chambers", F.lit(0.18)).otherwise(F.lit(0.32)),
        )
        .withColumn("total_weight_tons", (F.col("units_shipped") * F.col("unit_weight_ton") * (0.8 + 0.4 * F.rand(45))).cast("double"))
        .drop("unit_weight_ton")
    )

    tms = (
        tms.withColumn("base_cost", F.lit(180.0))
        .withColumn("km_cost", F.col("route_distance_km") * F.lit(1.10))
        .withColumn("ton_cost", F.col("total_weight_tons") * F.lit(42.0))
        .withColumn("carrier_premium", F.when(F.col("mode") == "carrier", F.lit(1.18)).otherwise(F.lit(1.0)))
        .withColumn("freight_cost_usd", (F.col("base_cost") + F.col("km_cost") + F.col("ton_cost")) * F.col("carrier_premium") * (0.9 + 0.2 * F.rand(46)))
        .drop("base_cost", "km_cost", "ton_cost", "carrier_premium")
    )

    tms = (
        tms.withColumn("emissions_factor", F.when(F.col("mode") == "own_fleet", F.lit(0.095)).otherwise(F.lit(0.105)))
        .withColumn("co2_kg", (F.col("route_distance_km") * F.col("total_weight_tons") * F.col("emissions_factor")).cast("double"))
        .drop("emissions_factor")
    )

    return tms.select(
        "shipment_id",
        "order_id",
        "plant_id",
        "dc_id",
        "truck_id",
        "route_distance_km",
        "mode",
        "ship_date",
        "delivery_date",
        "total_weight_tons",
        "freight_cost_usd",
        "co2_kg",
    )


@dlt.table(
    name="bronze_production_output_raw",
    comment="Bronze: synthetic daily production output by plant + family.",
)
def bronze_production_output_raw():
    cal = _calendar_df().select("date", "doy")
    plants = dlt.read("bronze_dims_plants").select("plant_id")

    prod = cal.crossJoin(plants).crossJoin(spark.createDataFrame([(f,) for f in SKU_FAMILIES], ["sku_family"]))
    prod = prod.withColumn("season_annual", (F.sin(2 * F.lit(3.1415926535) * F.col("doy") / F.lit(365.25)) + 1.0) / 2.0)

    prod = (
        prod.withColumn(
            "units_produced",
            F.round(
                F.when(F.col("sku_family") == "pipe", F.lit(520))
                .when(F.col("sku_family") == "chambers", F.lit(170))
                .otherwise(F.lit(90))
                * (0.85 + 0.4 * F.col("season_annual"))
                * (0.8 + 0.5 * F.rand(61))
            ).cast("int"),
        )
        .withColumn(
            "energy_kwh",
            (
                F.col("units_produced")
                * F.when(F.col("sku_family") == "pipe", F.lit(1.9)).when(F.col("sku_family") == "chambers", F.lit(3.4)).otherwise(F.lit(5.2))
                * (0.9 + 0.25 * F.rand(62))
            ).cast("double"),
        )
        .select("date", "plant_id", "sku_family", "units_produced", "energy_kwh")
    )
    return prod


@dlt.table(
    name="bronze_inventory_positions_raw",
    comment="Bronze: synthetic daily inventory snapshots by location + SKU.",
)
def bronze_inventory_positions_raw():
    cal = _calendar_df().select(F.col("date").alias("snapshot_date"), "doy")
    plants = dlt.read("bronze_dims_plants").select(F.col("plant_id").alias("location_id")).withColumn("location_type", F.lit("plant"))
    dcs = dlt.read("bronze_dims_dcs").select(F.col("dc_id").alias("location_id")).withColumn("location_type", F.lit("dc"))
    locations = plants.unionByName(dcs)
    skus = dlt.read("bronze_dims_skus").select("sku_id", "sku_family")

    inv = cal.crossJoin(locations).crossJoin(skus).withColumn("season_annual", (F.sin(2 * F.lit(3.1415926535) * F.col("doy") / F.lit(365.25)) + 1.0) / 2.0)

    inv = (
        inv.withColumn(
            "safety_stock_units",
            F.round(
                F.when(F.col("sku_family") == "pipe", F.lit(200)).when(F.col("sku_family") == "chambers", F.lit(80)).otherwise(F.lit(45))
                * F.when(F.col("location_type") == "dc", F.lit(1.3)).otherwise(F.lit(1.0))
                * (0.85 + 0.4 * F.rand(71))
            ).cast("int"),
        )
        .withColumn(
            "on_hand_units",
            F.greatest(
                F.lit(0),
                F.round(F.col("safety_stock_units") * (1.0 + 0.65 * (1.0 - F.col("season_annual"))) * (0.7 + 0.9 * F.rand(72))).cast("int"),
            ),
        )
        .withColumn(
            "on_order_units",
            F.greatest(F.lit(0), F.round(F.col("safety_stock_units") * (0.25 + 0.75 * F.rand(73))).cast("int")),
        )
        .withColumn("days_of_supply", F.round((F.col("on_hand_units") / F.greatest(F.lit(1.0), F.col("safety_stock_units") / F.lit(20.0)))).cast("int"))
    )

    return inv.select(
        "snapshot_date",
        F.when(F.col("location_type") == "plant", F.col("location_id")).otherwise(F.lit(None).cast("string")).alias("plant_id"),
        F.when(F.col("location_type") == "dc", F.col("location_id")).otherwise(F.lit(None).cast("string")).alias("dc_id"),
        "sku_id",
        "on_hand_units",
        "on_order_units",
        "safety_stock_units",
        "days_of_supply",
    )


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

