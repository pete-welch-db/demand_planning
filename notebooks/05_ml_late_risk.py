# Databricks notebook source
# MAGIC %md
# MAGIC ## 5) ML in the loop — Late Delivery Risk
# MAGIC
# MAGIC Trains a simple late-delivery risk model from `silver_erp_orders` (DLT output),
# MAGIC registers it in MLflow, and writes `order_late_risk_scored`.

# COMMAND ----------
# MAGIC %run ./07_ml_in_loop_late_risk

