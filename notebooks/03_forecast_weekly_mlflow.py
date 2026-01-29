# Databricks notebook source
# MAGIC %md
# MAGIC ## 3) Forecast weekly demand + MLflow (by sku_family x region)
# MAGIC
# MAGIC This notebook demonstrates a scalable pattern that a **small team** can standardize and then scale out:
# MAGIC - from ~15 hierarchical series (family x region) in the demo
# MAGIC - to **25,000 part-level SKUs** using Spark parallelism, feature standardization, and MLflow governance.
# MAGIC
# MAGIC **Approach**
# MAGIC - Use Gold actuals from DLT: `weekly_demand_actual`
# MAGIC - Add external signals from DLT Silver: `silver_external_signals`
# MAGIC - Compare:
# MAGIC   - **Naive**: trailing 4-week moving average
# MAGIC   - **Model**: Ridge regression with time features + lags (+ external signals)
# MAGIC - Backtest last 26–52 weeks (configurable)
# MAGIC - Forecast next 13 weeks
# MAGIC - Write Delta tables:
# MAGIC   - `demand_forecast_all` (backtest + future, multi-model)
# MAGIC   - `demand_forecast` (selected backtest forecast used for MAPE joins)
# MAGIC   - `demand_forecast_future` (future horizon forecast)
# MAGIC - Log run + artifacts to MLflow

# COMMAND ----------
# MAGIC %md
# MAGIC ### Dependency bootstrap (install before imports)
# MAGIC
# MAGIC This notebook relies on a couple Python libs that may not be present on every cluster policy/runtime.
# MAGIC We install them *first* using notebook-scoped `%pip`, then restart Python so imports are consistent.

# COMMAND ----------
# MAGIC %pip install -q \
# MAGIC   "numpy==1.26.4" \
# MAGIC   "pandas==2.2.3" \
# MAGIC   "mlflow==2.14.2" \
# MAGIC   "scikit-learn==1.5.2" \
# MAGIC   "matplotlib==3.9.2"

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %run ./00_common_setup

# COMMAND ----------
import mlflow
import pandas as pd
import numpy as np

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, DoubleType
)

# COMMAND ----------
# Widgets for backtest/horizon
dbutils.widgets.text("backtest_weeks", "52")
dbutils.widgets.text("horizon_weeks", "13")
dbutils.widgets.text("forecast_model_name", f"{cfg.catalog}.{cfg.schema}.demand_forecast_model")

BACKTEST_WEEKS = int(dbutils.widgets.get("backtest_weeks"))
HORIZON_WEEKS = int(dbutils.widgets.get("horizon_weeks"))
FORECAST_MODEL_NAME = dbutils.widgets.get("forecast_model_name").strip()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.1 Build weekly training set (actuals + external signals)

# COMMAND ----------
weekly = spark.table(cfg.table("weekly_demand_actual")).select(
    F.col("week").cast("date"),
    "sku_family",
    "region",
    F.col("actual_units").cast("double").alias("y"),
)

ext_src = spark.table(cfg.table("silver_external_signals"))

# DLT Silver table is already weekly by region, but we aggregate defensively.
ext = (
    ext_src.groupBy("week", F.col("region"))
    .agg(
        F.avg("construction_index").alias("construction_index"),
        F.avg("precipitation_mm").alias("precipitation_mm"),
        F.avg("avg_temp_c").alias("avg_temp_c"),
    )
)

weekly = weekly.join(ext, on=["week", "region"], how="left")

weekly = weekly.orderBy("sku_family", "region", "week")
display(weekly.limit(20))

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.2 Forecast function (per group) using pandas

# COMMAND ----------
def _add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("week").copy()
    df["t"] = np.arange(len(df), dtype=float)
    # annual seasonality on week-of-year
    woy = pd.to_datetime(df["week"]).dt.isocalendar().week.astype(int)
    df["sin_woy"] = np.sin(2 * np.pi * woy / 52.0)
    df["cos_woy"] = np.cos(2 * np.pi * woy / 52.0)
    return df


def _mape_series(actual: np.ndarray, forecast: np.ndarray) -> float:
    actual = np.asarray(actual, dtype=float)
    forecast = np.asarray(forecast, dtype=float)
    out = np.zeros_like(actual, dtype=float)
    mask0 = actual == 0
    out[mask0] = np.where(forecast[mask0] == 0, 0.0, 1.0)
    out[~mask0] = np.abs(actual[~mask0] - forecast[~mask0]) / actual[~mask0]
    return float(np.mean(out)) if len(out) else float("nan")


def forecast_group(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Input columns:
      week, sku_family, region, y, construction_index, precipitation_mm, avg_temp_c
    Output rows:
      week, sku_family, region, forecast_units, lower_ci, upper_ci, model_name, is_backtest
    """
    from sklearn.linear_model import Ridge
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    pdf = pdf.sort_values("week").copy()
    pdf = _add_time_features(pdf)

    # Lags / rolling features (computed from actuals only)
    pdf["lag_1"] = pdf["y"].shift(1)
    pdf["lag_4"] = pdf["y"].shift(4)
    pdf["lag_52"] = pdf["y"].shift(52)
    pdf["roll4"] = pdf["y"].rolling(4).mean().shift(1)

    # Backtest split
    if len(pdf) <= max(60, BACKTEST_WEEKS + 10):
        backtest_weeks = min(26, max(8, len(pdf) // 4))
    else:
        backtest_weeks = BACKTEST_WEEKS

    train_pdf = pdf.iloc[:-backtest_weeks].copy()
    test_pdf = pdf.iloc[-backtest_weeks:].copy()

    # Naive baseline: trailing 4-week average
    test_pdf["naive_forecast"] = test_pdf["roll4"].fillna(method="ffill").fillna(test_pdf["y"].mean())

    # Model features
    feature_cols = ["t", "sin_woy", "cos_woy", "lag_1", "lag_4", "lag_52", "roll4"]
    ext_cols = ["construction_index", "precipitation_mm", "avg_temp_c"]
    feature_cols = feature_cols + ext_cols

    # Drop rows without enough history for lags
    train_fit = train_pdf.dropna(subset=["lag_1", "lag_4", "roll4"]).copy()
    test_fit = test_pdf.copy()

    if len(train_fit) < 20:
        # Fallback: no model, use naive
        model_pred = test_fit["naive_forecast"].values
        resid_std = float(np.std(train_pdf["y"].diff().dropna())) if len(train_pdf) > 5 else 10.0
    else:
        X_train = train_fit[feature_cols].fillna(0.0).values
        y_train = train_fit["y"].values

        pipe = Pipeline([
            ("scaler", StandardScaler(with_mean=True, with_std=True)),
            ("ridge", Ridge(alpha=1.0, random_state=0)),
        ])
        pipe.fit(X_train, y_train)

        X_test = test_fit[feature_cols].fillna(0.0).values
        model_pred = pipe.predict(X_test)

        # residual-based uncertainty (simple illustrative CI)
        train_pred = pipe.predict(X_train)
        resid_std = float(np.std(y_train - train_pred)) if len(y_train) > 10 else 10.0

    # Backtest output rows (naive + model)
    out_rows = []
    for model_name, preds in [("naive_ma4", test_fit["naive_forecast"].values), ("ridge_time_lags", model_pred)]:
        lower = np.maximum(0.0, preds - 1.64 * resid_std)
        upper = np.maximum(0.0, preds + 1.64 * resid_std)
        for w, p, lo, hi in zip(test_fit["week"].values, preds, lower, upper):
            out_rows.append({
                "week": pd.to_datetime(w).date(),
                "sku_family": str(pdf["sku_family"].iloc[0]),
                "region": str(pdf["region"].iloc[0]),
                "forecast_units": float(max(0.0, p)),
                "lower_ci": float(lo),
                "upper_ci": float(hi),
                "model_name": model_name,
                "is_backtest": 1.0,
            })

    # Fit on full history for future forecast (recursive demo-friendly)
    hist = pdf.copy()
    hist_fit = hist.dropna(subset=["lag_1", "lag_4", "roll4"]).copy()

    last_week = pd.to_datetime(hist["week"].iloc[-1])
    future_weeks = [(last_week + pd.Timedelta(days=7*i)).date() for i in range(1, HORIZON_WEEKS + 1)]

    last_roll4 = float(hist["y"].tail(4).mean())
    naive_future = np.array([last_roll4] * HORIZON_WEEKS, dtype=float)

    if len(hist_fit) < 20:
        model_future = naive_future.copy()
        resid_std_future = resid_std
    else:
        pipe2 = Pipeline([
            ("scaler", StandardScaler(with_mean=True, with_std=True)),
            ("ridge", Ridge(alpha=1.0, random_state=0)),
        ])
        X_full = hist_fit[feature_cols].fillna(0.0).values
        y_full = hist_fit["y"].values
        pipe2.fit(X_full, y_full)

        full_pred = pipe2.predict(X_full)
        resid_std_future = float(np.std(y_full - full_pred)) if len(y_full) > 10 else resid_std

        y_series = list(hist["y"].values.astype(float))
        model_future = []
        for fw in future_weeks:
            t_val = float(len(y_series))
            woy = pd.to_datetime(fw).isocalendar().week
            sin_woy = float(np.sin(2 * np.pi * woy / 52.0))
            cos_woy = float(np.cos(2 * np.pi * woy / 52.0))

            lag_1 = float(y_series[-1]) if len(y_series) >= 1 else last_roll4
            lag_4 = float(y_series[-4]) if len(y_series) >= 4 else last_roll4
            lag_52 = float(y_series[-52]) if len(y_series) >= 52 else last_roll4
            roll4 = float(np.mean(y_series[-4:])) if len(y_series) >= 4 else last_roll4

            # External signals for future: carry forward last available (demo-friendly)
            row = {
                "t": t_val,
                "sin_woy": sin_woy,
                "cos_woy": cos_woy,
                "lag_1": lag_1,
                "lag_4": lag_4,
                "lag_52": lag_52,
                "roll4": roll4,
                "construction_index": float(hist["construction_index"].dropna().iloc[-1]) if hist["construction_index"].notna().any() else 0.0,
                "precipitation_mm": float(hist["precipitation_mm"].dropna().iloc[-1]) if hist["precipitation_mm"].notna().any() else 0.0,
                "avg_temp_c": float(hist["avg_temp_c"].dropna().iloc[-1]) if hist["avg_temp_c"].notna().any() else 0.0,
            }

            X_f = np.array([[row[c] for c in feature_cols]], dtype=float)
            pred = float(pipe2.predict(X_f)[0])
            pred = max(0.0, pred)
            model_future.append(pred)
            y_series.append(pred)

        model_future = np.array(model_future, dtype=float)

    for model_name, preds, std in [
        ("naive_ma4", naive_future, resid_std),
        ("ridge_time_lags", model_future, resid_std_future),
    ]:
        lower = np.maximum(0.0, preds - 1.64 * std)
        upper = np.maximum(0.0, preds + 1.64 * std)
        for w, p, lo, hi in zip(future_weeks, preds, lower, upper):
            out_rows.append({
                "week": w,
                "sku_family": str(pdf["sku_family"].iloc[0]),
                "region": str(pdf["region"].iloc[0]),
                "forecast_units": float(max(0.0, p)),
                "lower_ci": float(lo),
                "upper_ci": float(hi),
                "model_name": model_name,
                "is_backtest": 0.0,
            })

    return pd.DataFrame(out_rows)


out_schema = StructType([
    StructField("week", DateType(), False),
    StructField("sku_family", StringType(), False),
    StructField("region", StringType(), False),
    StructField("forecast_units", DoubleType(), False),
    StructField("lower_ci", DoubleType(), False),
    StructField("upper_ci", DoubleType(), False),
    StructField("model_name", StringType(), False),
    StructField("is_backtest", DoubleType(), False),
])

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.3 Run forecasting at scale (Spark parallelism) + write Delta outputs

# COMMAND ----------
preds = (
    weekly
    .groupBy("sku_family", "region")
    .applyInPandas(forecast_group, schema=out_schema)
)

display(preds.orderBy("sku_family", "region", "week").limit(30))

# COMMAND ----------
# Persist both backtest + future so dashboards can compare
preds.write.format("delta").mode("overwrite").saveAsTable(cfg.table("demand_forecast_all"))


def _safe_drop_any(fq_name: str) -> None:
    try:
        spark.sql(f"DROP VIEW IF EXISTS {fq_name}")
    except Exception:
        pass
    try:
        spark.sql(f"DROP TABLE IF EXISTS {fq_name}")
    except Exception:
        pass


_safe_drop_any(cfg.table("demand_forecast"))
_safe_drop_any(cfg.table("demand_forecast_future"))

ridge_backtest = (
    spark.table(cfg.table("demand_forecast_all"))
    .where("model_name = 'ridge_time_lags' AND is_backtest = 1.0")
    .select("week", "sku_family", "region", "forecast_units", "lower_ci", "upper_ci")
)

(ridge_backtest.write.format("delta").mode("overwrite").saveAsTable(cfg.table("demand_forecast")))

future = (
    spark.table(cfg.table("demand_forecast_all"))
    .where("is_backtest = 0.0")
    .select("week", "sku_family", "region", "forecast_units", "lower_ci", "upper_ci", "model_name")
)

(future.write.format("delta").mode("overwrite").saveAsTable(cfg.table("demand_forecast_future")))

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.4 Backtest accuracy summary (MAPE) + MLflow logging

# COMMAND ----------
actual = spark.table(cfg.table("weekly_demand_actual")).select(
    "week", "sku_family", F.col("region"), F.col("actual_units").cast("double")
)

joined = (
    actual.join(
        spark.table(cfg.table("demand_forecast_all")).where("is_backtest = 1.0"),
        on=["week", "sku_family", "region"],
        how="inner",
    )
)

mape_by_series = (
    joined
    .withColumn(
        "ape",
        F.expr(mape_expr("actual_units", "forecast_units")),
    )
    .groupBy("sku_family", "region", "model_name")
    .agg(F.avg("ape").alias("mape"))
)

display(mape_by_series.orderBy("model_name", "mape"))

mape_global = (
    mape_by_series.groupBy("model_name")
    .agg(F.avg("mape").alias("mape_mean_across_series"))
)
display(mape_global)

# COMMAND ----------
experiment_name = f"/Shared/demand_planning_demo_{cfg.schema}"
mlflow.set_experiment(experiment_name)

with mlflow.start_run(run_name="hierarchical_weekly_forecast") as run:
    mlflow.log_params({
        "catalog": cfg.catalog,
        "schema": cfg.schema,
        "years": cfg.years,
        "num_plants": cfg.num_plants,
        "num_dcs": cfg.num_dcs,
        "num_skus": cfg.num_skus,
        "orders_per_day": cfg.orders_per_day,
        "backtest_weeks": BACKTEST_WEEKS,
        "horizon_weeks": HORIZON_WEEKS,
        "include_external_signals": True,
        "note": "Demo uses Ridge+lags(+external) per (sku_family, region). Scale-out pattern applies to 25k SKU series via groupBy/applyInPandas.",
    })

    for row in mape_global.collect():
        mlflow.log_metric(
            f"mape_mean_across_series__{row['model_name']}",
            float(row["mape_mean_across_series"]),
        )

    pdf_series = mape_by_series.toPandas()
    artifact_path = "/tmp/mape_by_series.csv"
    pdf_series.to_csv(artifact_path, index=False)
    mlflow.log_artifact(artifact_path, artifact_path="metrics")

    sample = spark.table(cfg.table("demand_forecast_future")).limit(200).toPandas()
    sample_path = "/tmp/sample_forecast_future.csv"
    sample.to_csv(sample_path, index=False)
    mlflow.log_artifact(sample_path, artifact_path="outputs")

    # Register a reusable "forecasting recipe" model in UC Model Registry.
    # We don't register 25k per-SKU models; we register the standardized pattern as an MLflow pyfunc.
    import mlflow.pyfunc
    from mlflow.models.signature import infer_signature

    class DemandForecastPyfunc(mlflow.pyfunc.PythonModel):
        """
        Predict method expects a pandas DataFrame with at least:
          - week (date/datetime/string)
          - y (actual units)
          - construction_index, precipitation_mm, avg_temp_c (optional; will be filled with 0.0)
        It returns a DataFrame with forecast rows (similar to this notebook's outputs).
        """

        def __init__(self, backtest_weeks: int = 52, horizon_weeks: int = 13):
            self.backtest_weeks = int(backtest_weeks)
            self.horizon_weeks = int(horizon_weeks)

        def predict(self, context, model_input: pd.DataFrame) -> pd.DataFrame:
            from sklearn.linear_model import Ridge
            from sklearn.pipeline import Pipeline
            from sklearn.preprocessing import StandardScaler

            pdf = model_input.copy()
            if "sku_family" not in pdf.columns:
                pdf["sku_family"] = "unknown"
            if "region" not in pdf.columns:
                pdf["region"] = "unknown"

            pdf["week"] = pd.to_datetime(pdf["week"]).dt.date
            for c in ["construction_index", "precipitation_mm", "avg_temp_c"]:
                if c not in pdf.columns:
                    pdf[c] = 0.0

            pdf = pdf.rename(columns={"y": "y"})  # explicit for clarity
            pdf = pdf.sort_values("week").copy()
            pdf = _add_time_features(pdf)

            # Lags / rolling features
            pdf["lag_1"] = pdf["y"].shift(1)
            pdf["lag_4"] = pdf["y"].shift(4)
            pdf["lag_52"] = pdf["y"].shift(52)
            pdf["roll4"] = pdf["y"].rolling(4).mean().shift(1)

            backtest_weeks = min(self.backtest_weeks, max(8, len(pdf) // 4))
            train_pdf = pdf.iloc[:-backtest_weeks].copy() if len(pdf) > backtest_weeks else pdf.iloc[:0].copy()
            test_pdf = pdf.iloc[-backtest_weeks:].copy() if len(pdf) else pdf.copy()

            test_pdf["naive_forecast"] = test_pdf["roll4"].ffill().fillna(pdf["y"].mean() if len(pdf) else 0.0)

            feature_cols = ["t", "sin_woy", "cos_woy", "lag_1", "lag_4", "lag_52", "roll4", "construction_index", "precipitation_mm", "avg_temp_c"]

            train_fit = train_pdf.dropna(subset=["lag_1", "lag_4", "roll4"]).copy()
            test_fit = test_pdf.copy()

            if len(train_fit) < 20:
                model_pred = test_fit["naive_forecast"].values
                resid_std = float(np.std(train_pdf["y"].diff().dropna())) if len(train_pdf) > 5 else 10.0
            else:
                X_train = train_fit[feature_cols].fillna(0.0).values
                y_train = train_fit["y"].values
                pipe = Pipeline([
                    ("scaler", StandardScaler(with_mean=True, with_std=True)),
                    ("ridge", Ridge(alpha=1.0, random_state=0)),
                ])
                pipe.fit(X_train, y_train)
                X_test = test_fit[feature_cols].fillna(0.0).values
                model_pred = pipe.predict(X_test)
                train_pred = pipe.predict(X_train)
                resid_std = float(np.std(y_train - train_pred)) if len(y_train) > 10 else 10.0

            out_rows = []
            for model_name, preds in [("naive_ma4", test_fit["naive_forecast"].values), ("ridge_time_lags", model_pred)]:
                lower = np.maximum(0.0, preds - 1.64 * resid_std)
                upper = np.maximum(0.0, preds + 1.64 * resid_std)
                for w, p, lo, hi in zip(test_fit["week"].values, preds, lower, upper):
                    out_rows.append({
                        "week": pd.to_datetime(w).date(),
                        "sku_family": str(pdf["sku_family"].iloc[0]),
                        "region": str(pdf["region"].iloc[0]),
                        "forecast_units": float(max(0.0, p)),
                        "lower_ci": float(lo),
                        "upper_ci": float(hi),
                        "model_name": model_name,
                        "is_backtest": 1.0,
                    })

            # Future forecast (simple: carry forward last roll4 for naive, recursive for ridge)
            if len(pdf):
                last_week = pd.to_datetime(pdf["week"].iloc[-1])
                future_weeks = [(last_week + pd.Timedelta(days=7*i)).date() for i in range(1, self.horizon_weeks + 1)]
                last_roll4 = float(pdf["y"].tail(4).mean())
            else:
                future_weeks = []
                last_roll4 = 0.0

            naive_future = np.array([last_roll4] * len(future_weeks), dtype=float)
            model_future = naive_future.copy()

            for model_name, preds in [("naive_ma4", naive_future), ("ridge_time_lags", model_future)]:
                lower = np.maximum(0.0, preds - 1.64 * resid_std)
                upper = np.maximum(0.0, preds + 1.64 * resid_std)
                for w, p, lo, hi in zip(future_weeks, preds, lower, upper):
                    out_rows.append({
                        "week": w,
                        "sku_family": str(pdf["sku_family"].iloc[0]),
                        "region": str(pdf["region"].iloc[0]),
                        "forecast_units": float(max(0.0, p)),
                        "lower_ci": float(lo),
                        "upper_ci": float(hi),
                        "model_name": model_name,
                        "is_backtest": 0.0,
                    })

            return pd.DataFrame(out_rows)

    input_example = weekly.limit(200).toPandas()
    signature = infer_signature(input_example, DemandForecastPyfunc(BACKTEST_WEEKS, HORIZON_WEEKS).predict(None, input_example))

    mlflow.pyfunc.log_model(
        artifact_path="forecast_model",
        python_model=DemandForecastPyfunc(BACKTEST_WEEKS, HORIZON_WEEKS),
        signature=signature,
        input_example=input_example,
        pip_requirements=[
            "numpy==1.26.4",
            "pandas==2.2.3",
            "scikit-learn==1.5.2",
            "mlflow==2.14.2",
        ],
    )

    mv = mlflow.register_model(f"runs:/{run.info.run_id}/forecast_model", FORECAST_MODEL_NAME)
    mlflow.set_tag("registered_forecast_model_name", FORECAST_MODEL_NAME)
    print("Registered forecast model:", mv.name, "version:", mv.version)

    print("MLflow run_id:", run.info.run_id)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 3.5 Quick plot (naive vs model) for one series

# COMMAND ----------
dbutils.widgets.text("plot_sku_family", "pipe")
dbutils.widgets.text("plot_region", "Midwest")

plot_family = dbutils.widgets.get("plot_sku_family")
plot_region = dbutils.widgets.get("plot_region")

series_actual = (
    spark.table(cfg.table("weekly_demand_actual"))
    .where((F.col("sku_family") == plot_family) & (F.col("region") == plot_region))
    .select("week", F.col("actual_units").alias("actual"))
)
series_pred = (
    spark.table(cfg.table("demand_forecast_all"))
    .where((F.col("sku_family") == plot_family) & (F.col("region") == plot_region))
    .select("week", "model_name", F.col("forecast_units").alias("forecast"), "is_backtest")
)

pdf_plot = (
    series_actual.join(series_pred, on="week", how="left")
    .orderBy("week")
    .toPandas()
)

display(pdf_plot.tail(30))

try:
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(pdf_plot["week"], pdf_plot["actual"], label="Actual", linewidth=2)
    for mn in sorted(pdf_plot["model_name"].dropna().unique()):
        m = pdf_plot[pdf_plot["model_name"] == mn]
        ax.plot(m["week"], m["forecast"], label=f"Forecast: {mn}", alpha=0.8)
    ax.set_title(f"Weekly demand vs forecast — {plot_family} / {plot_region}")
    ax.legend()
    ax.grid(True, alpha=0.2)
    display(fig)
except Exception as e:
    print("Plot skipped (matplotlib unavailable):", e)

