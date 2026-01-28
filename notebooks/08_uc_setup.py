# Databricks notebook source
# MAGIC %md
# MAGIC ## 8) Bundle setup (UC catalog + schema)
# MAGIC
# MAGIC This notebook is designed to run as a **Databricks Job task** from the Asset Bundle.
# MAGIC
# MAGIC It performs best-effort setup:
# MAGIC - Create Unity Catalog **catalog** (if privileges allow)
# MAGIC - Create Unity Catalog **schema** (idempotent)

# COMMAND ----------
dbutils.widgets.text("catalog", "welch")
dbutils.widgets.text("schema", "demand_planning_demo")

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()

print("Target:", catalog, schema)

# COMMAND ----------
if catalog.lower() != "hive_metastore":
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
        print(f"Catalog ensured: {catalog}")
    except Exception as e:
        print("WARN: could not create catalog (may already exist or insufficient privileges):", e)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
print(f"Schema ensured: {catalog}.{schema}")

