# Databricks notebook source
# MAGIC %md
# MAGIC ## 1) UC Setup (catalog + schema)
# MAGIC
# MAGIC Designed to run as a **Databricks Job task** from the Asset Bundle.
# MAGIC
# MAGIC Best-effort setup:
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
    # Try to create catalog (requires elevated privileges)
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
        print(f"Catalog ensured: {catalog}")
    except Exception as e:
        print("WARN: could not create catalog (may already exist or insufficient privileges):", e)
    
    # CRITICAL: Switch to the Unity Catalog before creating schema
    spark.sql(f"USE CATALOG `{catalog}`")
    print(f"Switched to catalog: {catalog}")
    
    # Now create schema (single-part name since we're in the catalog context)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema}`")
    print(f"Schema ensured: {catalog}.{schema}")
else:
    # Hive metastore - use two-part name
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    print(f"Schema ensured: {catalog}.{schema}")

