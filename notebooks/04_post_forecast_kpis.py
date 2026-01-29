from __future__ import annotations

# Databricks notebook source
# MAGIC %md
# MAGIC ## 4) Post-Forecast KPI Refresh
# MAGIC
# MAGIC Creates only post-forecast analytics (e.g., MAPE) after `demand_forecast*` tables are written.

# COMMAND ----------
# MAGIC %run ./02_kpis_and_views

