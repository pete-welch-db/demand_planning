# Databricks notebook source
# MAGIC %md
# MAGIC ## 3) Forecast Weekly Demand (MLflow) — after DLT Gold exists
# MAGIC
# MAGIC Builds a scalable weekly forecaster by `(sku_family, region)` and writes:
# MAGIC - `demand_forecast_all` (backtest + future for multiple models)
# MAGIC - `demand_forecast` (selected backtest forecast used for MAPE joins)
# MAGIC - `demand_forecast_future` (future horizon forecast)

# COMMAND ----------
# MAGIC %run ./03_forecasting_mlflow

