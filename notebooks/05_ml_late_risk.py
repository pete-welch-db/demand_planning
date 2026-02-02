# Databricks notebook source
# MAGIC %md
# MAGIC ## 5) ML in the loop — Late Delivery Risk
# MAGIC
# MAGIC Trains a late-delivery risk model from `silver_erp_orders` (DLT output),
# MAGIC registers it in MLflow/Unity Catalog, and writes `order_late_risk_scored_ml`.
# MAGIC
# MAGIC **Note**: Uses scikit-learn for serverless compute compatibility.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dependency bootstrap

# COMMAND ----------

# MAGIC %pip install -q \
# MAGIC   "numpy==1.26.4" \
# MAGIC   "pandas==2.2.3" \
# MAGIC   "mlflow[databricks]==2.14.2" \
# MAGIC   "scikit-learn==1.5.2" \
# MAGIC   "matplotlib==3.9.2"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./00_common_setup

# COMMAND ----------

import mlflow
import mlflow.sklearn
import mlflow.pyfunc
from mlflow.models.signature import infer_signature

import pandas as pd
import numpy as np
from pyspark.sql import functions as F

from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline as SkPipeline
from sklearn.metrics import roc_auc_score, accuracy_score

# COMMAND ----------

# Widgets
dbutils.widgets.text("train_lookback_days", "540")
dbutils.widgets.text("test_days", "90")
dbutils.widgets.text("model_name", f"{cfg.catalog}.{cfg.schema}.order_late_risk_model")

TRAIN_LOOKBACK_DAYS = int(dbutils.widgets.get("train_lookback_days"))
TEST_DAYS = int(dbutils.widgets.get("test_days"))
MODEL_NAME = dbutils.widgets.get("model_name")

print(f"Model name: {MODEL_NAME}")
print(f"Train lookback: {TRAIN_LOOKBACK_DAYS} days, Test: {TEST_DAYS} days")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Load Silver orders + build labeled dataset

# COMMAND ----------

silver_orders_table = f"{cfg.fq_schema}.silver_erp_orders"
orders = spark.table(silver_orders_table)

# Label: 1 if delivered after requested date (late). Drop cancelled/unknown deliveries.
orders = (
    orders.where("order_status <> 'cancelled'")
    .withColumn(
        "is_late",
        (
            F.col("actual_delivery_date").isNotNull()
            & (F.col("actual_delivery_date") > F.col("requested_delivery_date"))
        ).cast("int"),
    )
    .withColumn("order_week", F.date_trunc("week", F.col("order_date")).cast("date"))
)

# Time window split (most recent TEST_DAYS as test)
max_order_date = orders.agg(F.max("order_date").alias("d")).collect()[0]["d"]
cutoff_test = spark.sql(f"SELECT date_sub(to_date('{max_order_date}'), {TEST_DAYS}) AS d").collect()[0]["d"]
cutoff_train = spark.sql(f"SELECT date_sub(to_date('{max_order_date}'), {TRAIN_LOOKBACK_DAYS}) AS d").collect()[0]["d"]

base = orders.where((F.col("order_date") >= F.lit(cutoff_train)) & (F.col("order_date") <= F.lit(max_order_date)))

# External signals (weekly, by region) from DLT
ext_tbl = f"{cfg.fq_schema}.silver_external_signals"
ext = spark.table(ext_tbl).select("week", "region", "construction_index", "precipitation_mm", "avg_temp_c")
base = base.join(ext, (base.order_week == ext.week) & (base.customer_region == ext.region), "left").drop(ext.week).drop(ext.region)

# Features available at order time
feat = (
    base.select(
        "order_id",
        "order_date",
        "requested_delivery_date",
        "customer_region",
        "channel",
        "plant_id",
        "dc_id",
        "sku_family",
        "units_ordered",
        "unit_price",
        "construction_index",
        "precipitation_mm",
        "avg_temp_c",
        "is_late",
    )
    .withColumn("days_to_request", F.datediff("requested_delivery_date", "order_date").cast("double"))
    .withColumn("dow", F.dayofweek("order_date").cast("double"))
    .withColumn("woy", F.weekofyear("order_date").cast("double"))
    .withColumn("month", F.month("order_date").cast("double"))
)

train_spark = feat.where(F.col("order_date") < F.lit(cutoff_test))
test_spark = feat.where(F.col("order_date") >= F.lit(cutoff_test))

print(f"Train rows: {train_spark.count():,}")
print(f"Test rows: {test_spark.count():,}")

display(train_spark.select("order_date", "is_late").groupBy("is_late").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Convert to Pandas and train scikit-learn model

# COMMAND ----------

# Convert to pandas for sklearn
train_pdf = train_spark.toPandas()
test_pdf = test_spark.toPandas()

# Define feature columns
cat_cols = ["customer_region", "channel", "plant_id", "dc_id", "sku_family"]
num_cols = [
    "units_ordered",
    "unit_price",
    "days_to_request",
    "dow",
    "woy",
    "month",
    "construction_index",
    "precipitation_mm",
    "avg_temp_c",
]

# Fill nulls
for c in num_cols:
    train_pdf[c] = pd.to_numeric(train_pdf[c], errors="coerce").fillna(0.0)
    test_pdf[c] = pd.to_numeric(test_pdf[c], errors="coerce").fillna(0.0)
for c in cat_cols:
    train_pdf[c] = train_pdf[c].fillna("unknown").astype(str)
    test_pdf[c] = test_pdf[c].fillna("unknown").astype(str)

# Prepare X and y
X_train = train_pdf[cat_cols + num_cols]
y_train = train_pdf["is_late"].astype(int)
X_test = test_pdf[cat_cols + num_cols]
y_test = test_pdf["is_late"].astype(int)

print(f"X_train shape: {X_train.shape}, y_train late rate: {y_train.mean():.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Build sklearn pipeline and train

# COMMAND ----------

# Sklearn pipeline: OneHotEncoder for categoricals, StandardScaler for numerics
preprocessor = ColumnTransformer(
    transformers=[
        ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), cat_cols),
        ("num", StandardScaler(), num_cols),
    ]
)

pipeline = SkPipeline([
    ("preprocessor", preprocessor),
    ("classifier", LogisticRegression(max_iter=200, C=1.0, solver="lbfgs", random_state=42)),
])

# Train
pipeline.fit(X_train, y_train)

# Evaluate
y_pred_proba = pipeline.predict_proba(X_test)[:, 1]
y_pred = pipeline.predict(X_test)

auc = roc_auc_score(y_test, y_pred_proba)
acc = accuracy_score(y_test, y_pred)
base_rate = y_train.mean()

print(f"Test AUC: {auc:.4f}")
print(f"Test Accuracy: {acc:.4f}")
print(f"Train late rate (baseline): {base_rate:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4 Log to MLflow and register model in Unity Catalog

# COMMAND ----------

experiment_name = f"/Shared/demand_planning_demo_{cfg.schema}"
mlflow.set_experiment(experiment_name)

with mlflow.start_run(run_name="late_delivery_risk_sklearn") as run:
    # Log parameters
    mlflow.log_params({
        "catalog": cfg.catalog,
        "schema": cfg.schema,
        "train_lookback_days": TRAIN_LOOKBACK_DAYS,
        "test_days": TEST_DAYS,
        "model_type": "sklearn_logistic_regression",
        "max_iter": 200,
        "C": 1.0,
        "num_features": len(num_cols),
        "cat_features": len(cat_cols),
    })
    
    # Log metrics
    mlflow.log_metrics({
        "auc": auc,
        "accuracy": acc,
        "train_late_rate": base_rate,
    })
    
    # Create input example and signature
    input_example = X_test.head(5)
    signature = infer_signature(X_test, y_pred_proba)
    
    # Log model
    mlflow.sklearn.log_model(
        pipeline,
        artifact_path="model",
        input_example=input_example,
        signature=signature,
        pip_requirements=[
            "numpy==1.26.4",
            "pandas==2.2.3",
            "scikit-learn==1.5.2",
        ],
    )
    
    # Register model in Unity Catalog
    try:
        mv = mlflow.register_model(f"runs:/{run.info.run_id}/model", MODEL_NAME)
        print(f"Registered model: {mv.name} version: {mv.version}")
        mlflow.set_tag("registered_model_name", MODEL_NAME)
        mlflow.set_tag("registered_model_version", mv.version)
    except Exception as e:
        print(f"Warning: Could not register model to UC: {e}")
        mlflow.set_tag("registration_error", str(e)[:500])

print(f"MLflow run ID: {run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.5 Score recent orders into Gold table

# COMMAND ----------

# Score all recent orders (last 91 days)
recent_cutoff = spark.sql("SELECT date_sub(current_date(), 91) AS d").collect()[0]["d"]
to_score_spark = feat.where(F.col("order_date") >= F.lit(recent_cutoff))

# Convert to pandas
to_score_pdf = to_score_spark.toPandas()

# Prepare features (same preprocessing as training)
for c in num_cols:
    to_score_pdf[c] = pd.to_numeric(to_score_pdf[c], errors="coerce").fillna(0.0)
for c in cat_cols:
    to_score_pdf[c] = to_score_pdf[c].fillna("unknown").astype(str)

X_score = to_score_pdf[cat_cols + num_cols]

# Score
to_score_pdf["late_risk_prob"] = pipeline.predict_proba(X_score)[:, 1]
to_score_pdf["late_risk_flag"] = (to_score_pdf["late_risk_prob"] >= 0.5).astype(int)
to_score_pdf["actual_late"] = to_score_pdf["is_late"].astype(int)

# Select columns for output
output_cols = [
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
]
scored_pdf = to_score_pdf[output_cols]

print(f"Scored {len(scored_pdf):,} orders")
print(f"High risk (prob >= 0.5): {(scored_pdf['late_risk_prob'] >= 0.5).sum():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.6 Write scored table to Delta

# COMMAND ----------

SCORED_TABLE_NAME = "order_late_risk_scored_ml"

# Convert back to Spark and write
scored_spark = spark.createDataFrame(scored_pdf)

# Drop existing object if it exists (table/view)
try:
    spark.sql(f"DROP VIEW IF EXISTS {cfg.table(SCORED_TABLE_NAME)}")
except Exception:
    pass
try:
    spark.sql(f"DROP TABLE IF EXISTS {cfg.table(SCORED_TABLE_NAME)}")
except Exception:
    pass

# Write as Delta table
(scored_spark.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(cfg.table(SCORED_TABLE_NAME)))

print(f"Wrote {scored_spark.count():,} rows to {cfg.table(SCORED_TABLE_NAME)}")
display(spark.table(cfg.table(SCORED_TABLE_NAME)).orderBy(F.desc("late_risk_prob")).limit(20))
