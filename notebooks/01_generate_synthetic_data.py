# Databricks notebook source
# MAGIC %md
# MAGIC ## 1) Generate synthetic manufacturing supply chain data (Delta)
# MAGIC
# MAGIC Creates synthetic-but-realistic daily data for 2–3 years across:
# MAGIC - ~10 plants, ~10 DCs
# MAGIC - 200–500 SKUs
# MAGIC - daily transactional ERP orders + inventory snapshots + shipments + production + external signals
# MAGIC
# MAGIC **Tables created**
# MAGIC - `erp_orders`
# MAGIC - `inventory_positions`
# MAGIC - `tms_shipments`
# MAGIC - `production_output`
# MAGIC - `external_signals` (optional)

# COMMAND ----------
# MAGIC %run ./00_setup

# COMMAND ----------
from pyspark.sql import Window
from pyspark.sql import functions as F

spark.conf.set("spark.sql.shuffle.partitions", "auto")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1.1 Dimensions (regions, plants, DCs, SKUs, customers)

# COMMAND ----------
regions = ["Northeast", "Southeast", "Midwest", "SouthCentral", "West"]

plants = (
    spark.range(cfg.num_plants)
    .select(
        (F.col("id") + 1).cast("int").alias("plant_n"),
    )
    .withColumn("plant_id", F.format_string("PL%02d", F.col("plant_n")))
    .withColumn("plant_region", F.element_at(F.array(*[F.lit(r) for r in regions]), (F.col("plant_n") % F.lit(len(regions))) + 1))
    .drop("plant_n")
)

dcs = (
    spark.range(cfg.num_dcs)
    .select((F.col("id") + 1).cast("int").alias("dc_n"))
    .withColumn("dc_id", F.format_string("DC%02d", F.col("dc_n")))
    .withColumn("dc_region", F.element_at(F.array(*[F.lit(r) for r in regions]), (F.col("dc_n") % F.lit(len(regions))) + 1))
    .drop("dc_n")
)

sku_families = ["pipe", "chambers", "structures"]
skus = (
    spark.range(cfg.num_skus)
    .select((F.col("id") + 1).cast("int").alias("sku_n"))
    .withColumnRenamed("sku_n", "sku_key")
    .withColumn("sku_n", F.col("sku_key"))
    .withColumn("sku_id", F.format_string("SKU%05d", F.col("sku_n")))
    .withColumn("sku_family", F.when(F.col("sku_n") % 10 < 6, F.lit("pipe"))
                .when(F.col("sku_n") % 10 < 8, F.lit("chambers"))
                .otherwise(F.lit("structures")))
    .withColumn("unit_price",
                F.when(F.col("sku_family") == "pipe", F.lit(75.0) + (F.col("sku_n") % 50) * 1.2)
                 .when(F.col("sku_family") == "chambers", F.lit(240.0) + (F.col("sku_n") % 40) * 2.5)
                 .otherwise(F.lit(520.0) + (F.col("sku_n") % 30) * 4.0))
    .drop("sku_n")
)

channels = ["distributor", "contractor", "DOT", "ag"]
customers = (
    spark.range(cfg.num_customers)
    .select((F.col("id") + 1).cast("int").alias("cust_n"))
    .withColumnRenamed("cust_n", "customer_key")
    .withColumn("cust_n", F.col("customer_key"))
    .withColumn("customer_id", F.format_string("C%06d", F.col("cust_n")))
    .withColumn("customer_region", F.element_at(F.array(*[F.lit(r) for r in regions]), (F.col("cust_n") % F.lit(len(regions))) + 1))
    .withColumn(
        "channel",
        F.when(F.col("cust_n") % 10 < 5, F.lit("distributor"))
         .when(F.col("cust_n") % 10 < 8, F.lit("contractor"))
         .when(F.col("cust_n") % 10 < 9, F.lit("DOT"))
         .otherwise(F.lit("ag")),
    )
    .drop("cust_n")
)

# Simple “closest” mapping: each region is served by one “primary” DC and a couple plants
dc_by_region = (
    dcs.groupBy("dc_region")
      .agg(F.min("dc_id").alias("primary_dc_id"))
      .withColumnRenamed("dc_region", "region")
)
plants_by_region = (
    plants.groupBy("plant_region")
      .agg(F.collect_list("plant_id").alias("plants_in_region"))
      .withColumnRenamed("plant_region", "region")
)

display(plants.limit(10))
display(dcs.limit(10))
display(skus.groupBy("sku_family").count())
display(customers.groupBy("channel").count())

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1.2 Calendar + optional external signals (construction/weather)

# COMMAND ----------
bounds = demo_date_bounds(cfg.years)
start_date = bounds["start_date"]
end_date = bounds["end_date"]

dates = (
    spark.sql(f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS date")
    .withColumn("doy", F.dayofyear("date"))
    .withColumn("dow", F.dayofweek("date"))
    .withColumn("week", F.date_trunc("week", F.col("date")).cast("date"))
)

# Seasonality: construction demand tends to peak spring/summer; some weekday effects.
dates = (
    dates
    .withColumn("season_annual", (F.sin(2 * F.lit(3.1415926535) * F.col("doy") / F.lit(365.25)) + 1.0) / 2.0)
    .withColumn("weekday_factor", F.when(F.col("dow").isin([1, 7]), F.lit(0.85)).otherwise(F.lit(1.0)))
)

if cfg.include_external_signals:
    external = (
        dates.crossJoin(spark.createDataFrame([(r,) for r in regions], ["region"]))
        .withColumn(
            "construction_index",
            (F.lit(85.0)
             + F.col("season_annual") * F.lit(25.0)
             + F.when(F.col("region").isin(["Northeast", "Midwest"]), F.lit(-5.0)).otherwise(F.lit(0.0))
             + F.randn(7) * F.lit(4.0)
            ).cast("double")
        )
        .withColumn(
            "avg_temp_c",
            (F.lit(10.0)
             + F.sin(2 * F.lit(3.1415926535) * F.col("doy") / F.lit(365.25)) * F.lit(12.0)
             + F.when(F.col("region") == "SouthCentral", F.lit(6.0))
               .when(F.col("region") == "West", F.lit(4.0))
               .when(F.col("region") == "Northeast", F.lit(-2.0))
               .otherwise(F.lit(0.0))
             + F.randn(8) * F.lit(2.0)
            ).cast("double")
        )
        .withColumn(
            "precipitation_mm",
            F.greatest(
                F.lit(0.0),
                (F.lit(2.0)
                 + (1.0 - F.col("season_annual")) * F.lit(4.0)  # a bit wetter in “off season”
                 + F.when(F.col("region") == "Southeast", F.lit(1.5)).otherwise(F.lit(0.0))
                 + F.randn(9) * F.lit(2.5)
                ),
            ).cast("double"),
        )
        .select(F.col("date"), F.col("region"), "construction_index", "precipitation_mm", "avg_temp_c")
    )

    (external.write.format("delta").mode("overwrite").saveAsTable(cfg.table("external_signals")))

    display(spark.table(cfg.table("external_signals")).limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1.3 ERP Orders (transactional)
# MAGIC
# MAGIC We generate daily orders with:
# MAGIC - region/channel mix
# MAGIC - SKU family mix
# MAGIC - lead times and service performance
# MAGIC - partial ship/backorder behavior

# COMMAND ----------
total_days = dates.count()
total_orders = int(total_days * cfg.orders_per_day)
print(f"Generating ~{total_orders:,} orders over {total_days} days")

# One row per order-line (order_id + sku_id)
raw_orders = (
    spark.range(total_orders)
    .withColumn("order_id", F.format_string("O%010d", F.col("id") + 1))
    .withColumn("order_date", F.expr(f"date_add(to_date('{start_date}'), cast(rand(11) * {total_days} as int))"))
    .withColumn("customer_key", (F.floor(F.rand(12) * F.lit(cfg.num_customers)) + 1).cast("int"))
    .withColumn("sku_key", (F.floor(F.rand(13) * F.lit(cfg.num_skus)) + 1).cast("int"))
    .drop("id")
)

# Attach customer + sku attributes
orders = (
    raw_orders
    .join(customers, on="customer_key", how="left")
    .join(skus, on="sku_key", how="left")
    .drop("customer_key", "sku_key")
)

# Map to primary DC by region and choose a plant from that region
orders = (
    orders
    .join(dc_by_region, orders.customer_region == dc_by_region.region, "left")
    .join(plants_by_region, orders.customer_region == plants_by_region.region, "left")
    .withColumn("dc_id", F.col("primary_dc_id"))
    .withColumn("plant_id", F.element_at("plants_in_region", (F.floor(F.rand(14) * F.size("plants_in_region")) + 1).cast("int")))
    .drop("primary_dc_id", "plants_in_region", "region")
)

# Add demand seasonality via calendar + external signals
orders = orders.join(dates.select("date", "season_annual", "weekday_factor").withColumnRenamed("date", "order_date"), on="order_date", how="left")

if cfg.include_external_signals:
    ext = spark.table(cfg.table("external_signals")).withColumnRenamed("date", "order_date").withColumnRenamed("region", "customer_region")
    orders = orders.join(ext, on=["order_date", "customer_region"], how="left")
else:
    orders = orders.withColumn("construction_index", F.lit(100.0)).withColumn("precipitation_mm", F.lit(2.0)).withColumn("avg_temp_c", F.lit(12.0))

# Units ordered: baseline by family, with “project spike” behavior and construction index sensitivity
orders = (
    orders
    .withColumn("base_units",
                F.when(F.col("sku_family") == "pipe", F.lit(22.0))
                 .when(F.col("sku_family") == "chambers", F.lit(8.0))
                 .otherwise(F.lit(3.5)))
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
    .drop("base_units", "project_spike", "weekday_factor", "season_annual")
)

# Lead times and actual performance
orders = (
    orders
    .withColumn(
        "requested_delivery_date",
        F.expr("""
          date_add(order_date,
            CASE
              WHEN channel = 'DOT' THEN 10 + cast(rand(21) * 8 as int)
              WHEN channel = 'contractor' THEN 6 + cast(rand(22) * 6 as int)
              WHEN channel = 'ag' THEN 7 + cast(rand(23) * 6 as int)
              ELSE 5 + cast(rand(24) * 6 as int)
            END
          )
        """),
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

# Status + shipped quantities
orders = (
    orders
    .withColumn(
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

erp_orders = (
    orders
    .select(
        "order_id",
        "order_date",
        "requested_delivery_date",
        F.col("actual_ship_date").alias("actual_ship_date"),
        F.col("actual_delivery_date").alias("actual_delivery_date"),
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
)

(erp_orders.write.format("delta").mode("overwrite").saveAsTable(cfg.table("erp_orders")))
display(spark.table(cfg.table("erp_orders")).limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1.4 TMS Shipments (shipment-level, linked to orders)

# COMMAND ----------
orders_for_ship = spark.table(cfg.table("erp_orders")).where("order_status <> 'cancelled' AND actual_ship_date IS NOT NULL")

# One shipment per order (simplified). You can extend to multi-stop loads for richer lane analytics.
tms = (
    orders_for_ship
    .select("order_id", "plant_id", "dc_id", "actual_ship_date", "actual_delivery_date", "customer_region", "sku_family", "units_shipped", "unit_price")
    .withColumnRenamed("actual_ship_date", "ship_date")
    .withColumnRenamed("actual_delivery_date", "delivery_date")
    .withColumn("shipment_id", F.concat(F.lit("S"), F.expr("substring(order_id, 2)")))
    .withColumn("truck_id", F.format_string("T%05d", (F.floor(F.rand(41) * 1200) + 1).cast("int")))
)

# Distance proxy by region + random variability (km)
region_distance = F.when(F.col("customer_region") == "Northeast", 420)\
    .when(F.col("customer_region") == "Southeast", 520)\
    .when(F.col("customer_region") == "Midwest", 610)\
    .when(F.col("customer_region") == "SouthCentral", 740)\
    .otherwise(860)

tms = (
    tms
    .withColumn("route_distance_km", (region_distance * (0.7 + 0.7 * F.rand(42))).cast("double"))
    .withColumn(
        "mode",
        F.when(F.col("route_distance_km") < 650, F.when(F.rand(43) < 0.78, F.lit("own_fleet")).otherwise(F.lit("carrier")))
         .otherwise(F.when(F.rand(44) < 0.55, F.lit("own_fleet")).otherwise(F.lit("carrier"))),
    )
)

# Weight proxy (tons) by family and shipped units
tms = (
    tms
    .withColumn("unit_weight_ton",
                F.when(F.col("sku_family") == "pipe", F.lit(0.06))
                 .when(F.col("sku_family") == "chambers", F.lit(0.18))
                 .otherwise(F.lit(0.32)))
    .withColumn("total_weight_tons", (F.col("units_shipped") * F.col("unit_weight_ton") * (0.8 + 0.4 * F.rand(45))).cast("double"))
    .drop("unit_weight_ton")
)

# Freight cost proxy: fixed + variable per km and ton, premium for carriers
tms = (
    tms
    .withColumn("base_cost", F.lit(180.0))
    .withColumn("km_cost", F.col("route_distance_km") * F.lit(1.10))
    .withColumn("ton_cost", F.col("total_weight_tons") * F.lit(42.0))
    .withColumn("carrier_premium", F.when(F.col("mode") == "carrier", F.lit(1.18)).otherwise(F.lit(1.0)))
    .withColumn("freight_cost_usd", (F.col("base_cost") + F.col("km_cost") + F.col("ton_cost")) * F.col("carrier_premium") * (0.9 + 0.2 * F.rand(46)))
    .drop("base_cost", "km_cost", "ton_cost", "carrier_premium")
)

# CO2 proxy: kg = distance_km * weight_tons * emissions_factor
# Emissions factors are illustrative only; tune for your narrative.
tms = (
    tms
    .withColumn("emissions_factor", F.when(F.col("mode") == "own_fleet", F.lit(0.095)).otherwise(F.lit(0.105)))
    .withColumn("co2_kg", (F.col("route_distance_km") * F.col("total_weight_tons") * F.col("emissions_factor")).cast("double"))
    .drop("emissions_factor")
)

tms_shipments = tms.select(
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

(tms_shipments.write.format("delta").mode("overwrite").saveAsTable(cfg.table("tms_shipments")))
display(spark.table(cfg.table("tms_shipments")).limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1.5 Production output (daily by plant + sku_family)

# COMMAND ----------
prod = (
    dates.select("date", "doy")
    .crossJoin(plants.select("plant_id", "plant_region"))
    .crossJoin(spark.createDataFrame([(f,) for f in sku_families], ["sku_family"]))
    .withColumn("season_annual", (F.sin(2 * F.lit(3.1415926535) * F.col("doy") / F.lit(365.25)) + 1.0) / 2.0)
)

prod = (
    prod
    .withColumn(
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
            * F.when(F.col("sku_family") == "pipe", F.lit(1.9))
               .when(F.col("sku_family") == "chambers", F.lit(3.4))
               .otherwise(F.lit(5.2))
            * (0.9 + 0.25 * F.rand(62))
        ).cast("double"),
    )
    .select("date", "plant_id", "sku_family", "units_produced", "energy_kwh")
)

(prod.write.format("delta").mode("overwrite").saveAsTable(cfg.table("production_output")))
display(spark.table(cfg.table("production_output")).limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1.6 Inventory positions (daily snapshots by location + SKU)
# MAGIC
# MAGIC This is intentionally a simplified inventory “state” model that’s good enough for KPI demos.

# COMMAND ----------
locations = (
    plants.select(F.col("plant_id").alias("location_id")).withColumn("location_type", F.lit("plant"))
    .unionByName(dcs.select(F.col("dc_id").alias("location_id")).withColumn("location_type", F.lit("dc")))
)

inv_base = (
    dates.select(F.col("date").alias("snapshot_date"), "doy")
    .crossJoin(locations)
    .crossJoin(skus.select("sku_id", "sku_family"))
    .withColumn("season_annual", (F.sin(2 * F.lit(3.1415926535) * F.col("doy") / F.lit(365.25)) + 1.0) / 2.0)
)

# “Typical” safety stock varies by family and location type
inv = (
    inv_base
    .withColumn(
        "safety_stock_units",
        F.round(
            F.when(F.col("sku_family") == "pipe", F.lit(200))
             .when(F.col("sku_family") == "chambers", F.lit(80))
             .otherwise(F.lit(45))
            * F.when(F.col("location_type") == "dc", F.lit(1.3)).otherwise(F.lit(1.0))
            * (0.85 + 0.4 * F.rand(71))
        ).cast("int"),
    )
    .withColumn(
        "on_hand_units",
        F.greatest(
            F.lit(0),
            F.round(
                F.col("safety_stock_units")
                * (1.0 + 0.65 * (1.0 - F.col("season_annual")))  # carry more in off-season
                * (0.7 + 0.9 * F.rand(72))
            ).cast("int"),
        ),
    )
    .withColumn(
        "on_order_units",
        F.greatest(
            F.lit(0),
            F.round(
                F.col("safety_stock_units")
                * (0.25 + 0.75 * F.rand(73))
            ).cast("int"),
        ),
    )
    .withColumn(
        "days_of_supply",
        F.round(
            (F.col("on_hand_units") / F.greatest(F.lit(1.0), F.col("safety_stock_units") / F.lit(20.0)))
        ).cast("int"),
    )
)

inventory_positions = (
    inv
    .select(
        "snapshot_date",
        F.when(F.col("location_type") == "plant", F.col("location_id")).otherwise(F.lit(None).cast("string")).alias("plant_id"),
        F.when(F.col("location_type") == "dc", F.col("location_id")).otherwise(F.lit(None).cast("string")).alias("dc_id"),
        "sku_id",
        "on_hand_units",
        "on_order_units",
        "safety_stock_units",
        "days_of_supply",
    )
)

(inventory_positions.write.format("delta").mode("overwrite").saveAsTable(cfg.table("inventory_positions")))
display(spark.table(cfg.table("inventory_positions")).limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ### 1.7 Quick row counts (sanity check)

# COMMAND ----------
for t in ["erp_orders", "tms_shipments", "inventory_positions", "production_output"] + (["external_signals"] if cfg.include_external_signals else []):
    n = spark.table(cfg.table(t)).count()
    print(f"{t}: {n:,}")

