# Databricks notebook source
# MAGIC %md
# MAGIC ## 2) KPI calculations + Gold Views (DBSQL-friendly)
# MAGIC
# MAGIC This notebook builds curated **views** (and a couple small helper tables) that power “control tower” dashboards.
# MAGIC
# MAGIC KPIs included:
# MAGIC - Forecast Accuracy (**MAPE**) – computed after forecasts exist (joins to `demand_forecast`)
# MAGIC - **OTIF** and Perfect Order proxy
# MAGIC - Inventory turnover / Days of Supply
# MAGIC - Freight cost per ton + premium freight %
# MAGIC - CO₂ per ton shipped + energy intensity

# COMMAND ----------
# MAGIC %run ./00_setup

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2.1 Base weekly demand (actuals) for forecasting + reporting

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE VIEW {cfg.table("weekly_demand_actual")} AS
SELECT
  date_trunc('week', order_date) AS week,
  sku_family,
  customer_region AS region,
  sum(units_ordered) AS actual_units,
  sum(units_shipped) AS shipped_units,
  count(*) AS order_lines
FROM {cfg.table("erp_orders")}
WHERE order_status <> 'cancelled'
GROUP BY 1,2,3
""")

display(spark.table(cfg.table("weekly_demand_actual")).limit(10))

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2.2 OTIF + Perfect Order proxy (weekly)
# MAGIC
# MAGIC OTIF definition:
# MAGIC - delivered on or before requested date **AND**
# MAGIC - shipped quantity equals ordered quantity

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE VIEW {cfg.table("kpi_otif_weekly")} AS
WITH order_level AS (
  SELECT
    order_id,
    customer_region AS region,
    dc_id,
    date_trunc('week', order_date) AS week,
    max(CASE WHEN actual_delivery_date IS NOT NULL AND actual_delivery_date <= requested_delivery_date THEN 1 ELSE 0 END) AS on_time,
    min(CASE WHEN units_shipped = units_ordered THEN 1 ELSE 0 END) AS in_full
  FROM {cfg.table("erp_orders")}
  WHERE order_status <> 'cancelled'
  GROUP BY 1,2,3,4
),
scored AS (
  SELECT
    week,
    region,
    dc_id,
    count(*) AS orders,
    avg(CASE WHEN on_time = 1 AND in_full = 1 THEN 1.0 ELSE 0.0 END) AS otif_rate,
    -- Perfect Order proxy: OTIF AND not backorder (a lightweight stand-in for damage/claims flags)
    avg(CASE WHEN on_time = 1 AND in_full = 1 THEN 1.0 ELSE 0.0 END) AS perfect_order_rate_proxy
  FROM order_level
  GROUP BY 1,2,3
)
SELECT * FROM scored
""")

display(spark.table(cfg.table("kpi_otif_weekly")).orderBy(F.desc("week")).limit(10))

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2.3 Freight KPIs (weekly)

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE VIEW {cfg.table("kpi_freight_weekly")} AS
SELECT
  date_trunc('week', ship_date) AS week,
  dc_id,
  plant_id,
  mode,
  sum(freight_cost_usd) AS freight_cost_usd,
  sum(total_weight_tons) AS total_weight_tons,
  sum(co2_kg) AS co2_kg,
  sum(freight_cost_usd) / nullif(sum(total_weight_tons), 0) AS freight_cost_per_ton,
  sum(co2_kg) / nullif(sum(total_weight_tons), 0) AS co2_kg_per_ton
FROM {cfg.table("tms_shipments")}
GROUP BY 1,2,3,4
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {cfg.table("kpi_premium_freight_weekly")} AS
SELECT
  week,
  dc_id,
  plant_id,
  sum(CASE WHEN mode = 'carrier' THEN freight_cost_usd ELSE 0 END) / nullif(sum(freight_cost_usd), 0) AS premium_freight_pct
FROM {cfg.table("kpi_freight_weekly")}
GROUP BY 1,2,3
""")

display(spark.table(cfg.table("kpi_freight_weekly")).orderBy(F.desc("week")).limit(10))

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2.4 Inventory KPIs
# MAGIC
# MAGIC Turnover uses a proxy:
# MAGIC - **COGS** ≈ shipped units × (unit price × cost factor)
# MAGIC - average inventory ≈ average on_hand_units × (unit price × cost factor)

# COMMAND ----------
cost_factor = 0.62  # illustrative “COGS % of price”

spark.sql(f"""
CREATE OR REPLACE VIEW {cfg.table("kpi_inventory_daily")} AS
SELECT
  snapshot_date AS date,
  coalesce(plant_id, dc_id) AS location_id,
  CASE WHEN plant_id IS NOT NULL THEN 'plant' ELSE 'dc' END AS location_type,
  sku_id,
  on_hand_units,
  on_order_units,
  safety_stock_units,
  days_of_supply
FROM {cfg.table("inventory_positions")}
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {cfg.table("kpi_inventory_turns_weekly")} AS
WITH shipped AS (
  SELECT
    date_trunc('week', order_date) AS week,
    dc_id AS location_id,
    sku_id,
    sum(units_shipped * unit_price * {cost_factor}) AS cogs_proxy
  FROM {cfg.table("erp_orders")}
  WHERE order_status <> 'cancelled'
  GROUP BY 1,2,3
),
avg_inv AS (
  SELECT
    date_trunc('week', date) AS week,
    location_id,
    sku_id,
    avg(on_hand_units) AS avg_on_hand_units
  FROM {cfg.table("kpi_inventory_daily")}
  WHERE location_type = 'dc'
  GROUP BY 1,2,3
),
price AS (
  SELECT sku_id, max(unit_price) AS unit_price
  FROM {cfg.table("erp_orders")}
  GROUP BY 1
)
SELECT
  s.week,
  s.location_id,
  s.sku_id,
  s.cogs_proxy,
  a.avg_on_hand_units,
  (a.avg_on_hand_units * p.unit_price * {cost_factor}) AS avg_inventory_value_proxy,
  -- Weekly turns proxy (annualized): (weekly COGS * 52) / avg inventory
  (s.cogs_proxy * 52.0) / nullif((a.avg_on_hand_units * p.unit_price * {cost_factor}), 0) AS inventory_turnover_annualized
FROM shipped s
LEFT JOIN avg_inv a USING (week, location_id, sku_id)
LEFT JOIN price p USING (sku_id)
""")

display(spark.table(cfg.table("kpi_inventory_turns_weekly")).orderBy(F.desc("week")).limit(10))

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2.5 Sustainability KPIs

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE VIEW {cfg.table("kpi_energy_intensity_weekly")} AS
SELECT
  date_trunc('week', date) AS week,
  plant_id,
  sku_family,
  sum(energy_kwh) AS energy_kwh,
  sum(units_produced) AS units_produced,
  sum(energy_kwh) / nullif(sum(units_produced), 0) AS energy_kwh_per_unit
FROM {cfg.table("production_output")}
GROUP BY 1,2,3
""")

display(spark.table(cfg.table("kpi_energy_intensity_weekly")).orderBy(F.desc("week")).limit(10))

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2.6 Forecast accuracy (MAPE) – becomes available after `demand_forecast` exists
# MAGIC
# MAGIC This view will be empty until you run the forecasting notebook.

# COMMAND ----------
if not spark.catalog.tableExists(cfg.table("demand_forecast").replace("`", "")):
    # Create an empty placeholder table so this notebook can run before the forecasting step.
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

# COMMAND ----------
# MAGIC %md
# MAGIC ### 2.7 “Gold” summary view for control tower

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE VIEW {cfg.table("control_tower_weekly")} AS
WITH otif AS (
  SELECT week, region, dc_id, otif_rate
  FROM {cfg.table("kpi_otif_weekly")}
),
freight AS (
  SELECT
    week,
    dc_id,
    plant_id,
    sum(freight_cost_usd) AS freight_cost_usd,
    sum(total_weight_tons) AS total_weight_tons,
    sum(co2_kg) AS co2_kg,
    sum(freight_cost_usd) / nullif(sum(total_weight_tons), 0) AS freight_cost_per_ton,
    sum(co2_kg) / nullif(sum(total_weight_tons), 0) AS co2_kg_per_ton
  FROM {cfg.table("kpi_freight_weekly")}
  GROUP BY 1,2,3
),
premium AS (
  SELECT week, dc_id, plant_id, premium_freight_pct
  FROM {cfg.table("kpi_premium_freight_weekly")}
),
energy AS (
  SELECT week, plant_id, sku_family, energy_kwh_per_unit
  FROM {cfg.table("kpi_energy_intensity_weekly")}
)
SELECT
  f.week,
  o.region,
  f.plant_id,
  f.dc_id,
  o.otif_rate,
  f.freight_cost_per_ton,
  p.premium_freight_pct,
  f.co2_kg_per_ton,
  e.sku_family,
  e.energy_kwh_per_unit
FROM freight f
LEFT JOIN otif o ON f.week = o.week AND f.dc_id = o.dc_id
LEFT JOIN premium p ON f.week = p.week AND f.dc_id = p.dc_id AND f.plant_id = p.plant_id
LEFT JOIN energy e ON f.week = e.week AND f.plant_id = e.plant_id
""")

display(spark.table(cfg.table("control_tower_weekly")).orderBy(F.desc("week")).limit(10))

# COMMAND ----------
# MAGIC %md
# MAGIC ### Optional: Delta Live Tables (DLT) pattern (snippet)
# MAGIC
# MAGIC For a live demo, it’s often enough to show that these **SQL views are DBSQL-ready**.
# MAGIC
# MAGIC If you want a DLT storyline, you can migrate the `CREATE VIEW` statements above into a DLT pipeline notebook using `CREATE LIVE TABLE/VIEW`.

