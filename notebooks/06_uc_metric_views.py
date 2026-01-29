# Databricks notebook source
# MAGIC %md
# MAGIC ## 6) Unity Catalog Metric Views — governed KPIs (one-time setup)
# MAGIC
# MAGIC This notebook creates **Unity Catalog metric views** (defined in YAML, registered as UC views)
# MAGIC to standardize key supply chain KPIs for reuse across:
# MAGIC - Databricks SQL / AI/BI Dashboards
# MAGIC - Genie spaces
# MAGIC - Streamlit (via SQL Warehouse)
# MAGIC
# MAGIC Metric views are created using SQL with `WITH METRICS LANGUAGE YAML` (see Databricks docs).
# MAGIC
# MAGIC ### What we create
# MAGIC - `mv_control_tower_kpis` — OTIF, freight $/ton, premium freight %, CO₂ kg/ton, energy kWh/unit
# MAGIC - `mv_forecast_accuracy` — MAPE (post-forecast KPI)
# MAGIC - `mv_late_delivery_risk` — late-risk probability + observed late rate
# MAGIC
# MAGIC ### Prereqs / run order
# MAGIC - Run DLT first (creates `control_tower_weekly`, etc.)
# MAGIC - Run `04_post_forecast_kpis.py` before the forecast metric view (`kpi_mape_weekly`)
# MAGIC - Run either:
# MAGIC   - DLT scoring (creates `order_late_risk_scored`), or
# MAGIC   - Notebook 5 (creates `order_late_risk_scored_ml`)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 6.0 Parameters
# MAGIC
# MAGIC Metric views are created in the configured catalog + schema.

# COMMAND ----------
dbutils.widgets.text("catalog", "welch")
dbutils.widgets.text("schema", "demand_planning_demo")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()

FQ = lambda name: f"`{CATALOG}`.`{SCHEMA}`.`{name}`"

# COMMAND ----------
# MAGIC %md
# MAGIC ### 6.1 Create metric view: Control tower KPIs (weekly)
# MAGIC
# MAGIC **Source**: `control_tower_weekly` (Gold)
# MAGIC
# MAGIC **Typical queries (Databricks SQL)**:
# MAGIC
# MAGIC ```sql
# MAGIC -- OTIF trend by region (last 13 weeks)
# MAGIC SELECT
# MAGIC   week,
# MAGIC   region,
# MAGIC   MEASURE(`OTIF`) AS otif
# MAGIC FROM welch.demand_planning_demo.mv_control_tower_kpis
# MAGIC WHERE week >= date_sub(current_date(), 91)
# MAGIC GROUP BY week, region
# MAGIC ORDER BY week, region;
# MAGIC ```

# COMMAND ----------
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

# COMMAND ----------
# MAGIC %md
# MAGIC ### 6.2 Create metric view: Forecast accuracy (MAPE)
# MAGIC
# MAGIC **Source**: `kpi_mape_weekly` (created by `04_post_forecast_kpis.py`)
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   sku_family,
# MAGIC   region,
# MAGIC   MEASURE(`MAPE`) AS mape
# MAGIC FROM welch.demand_planning_demo.mv_forecast_accuracy
# MAGIC WHERE week >= date_sub(current_date(), 91)
# MAGIC GROUP BY sku_family, region
# MAGIC ORDER BY mape DESC;
# MAGIC ```

# COMMAND ----------
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

# COMMAND ----------
# MAGIC %md
# MAGIC ### 6.3 Create metric view: Late delivery risk (ML-in-loop)
# MAGIC
# MAGIC We standardize the *consumer-facing* KPIs:
# MAGIC - average late-risk probability
# MAGIC - observed late rate (from `actual_late`)
# MAGIC - order count
# MAGIC
# MAGIC **Source note**
# MAGIC - Prefer `order_late_risk_scored_ml` (Notebook 5 output; avoids colliding with DLT)
# MAGIC - If you want the DLT-owned table instead, change the `source:` below to `order_late_risk_scored`

# COMMAND ----------
spark.sql(
    f"""
CREATE OR REPLACE VIEW {FQ("mv_late_delivery_risk")}
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Late delivery risk KPIs based on scored orders (Notebook 5 output by default)."
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

# COMMAND ----------
# MAGIC %md
# MAGIC ### 6.4 Quick verification (optional)
# MAGIC
# MAGIC In Databricks SQL, try a simple query like:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   order_week,
# MAGIC   customer_region,
# MAGIC   MEASURE(`Avg late-risk prob`) AS avg_risk
# MAGIC FROM welch.demand_planning_demo.mv_late_delivery_risk
# MAGIC WHERE order_date >= date_sub(current_date(), 28)
# MAGIC GROUP BY order_week, customer_region
# MAGIC ORDER BY avg_risk DESC;
# MAGIC ```

# COMMAND ----------
display(spark.sql(f"SHOW VIEWS IN `{CATALOG}`.`{SCHEMA}`"))

