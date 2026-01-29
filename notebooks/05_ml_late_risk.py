# Databricks notebook source
# MAGIC %md
# MAGIC ## 5) ML in the loop — Late Delivery Risk
# MAGIC
# MAGIC Trains a simple late-delivery risk model from `silver_erp_orders` (DLT output),
# MAGIC registers it in MLflow, and writes `order_late_risk_scored`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dependency bootstrap (install before imports)
# MAGIC
# MAGIC Install Python libs first, restart Python, then load common setup so `cfg` is available.

# COMMAND ----------

# DBTITLE 1,Cell 3
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
import mlflow.spark

from pyspark.sql import functions as F
from pyspark.ml.functions import vector_to_array

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler

# COMMAND ----------

# Widgets
dbutils.widgets.text("train_lookback_days", "540")
dbutils.widgets.text("test_days", "90")
dbutils.widgets.text("model_name", f"{cfg.catalog}.{cfg.schema}.order_late_risk_model")

TRAIN_LOOKBACK_DAYS = int(dbutils.widgets.get("train_lookback_days"))
TEST_DAYS = int(dbutils.widgets.get("test_days"))
MODEL_NAME = dbutils.widgets.get("model_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Load Silver orders (from DLT pipeline) + build labeled dataset

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

train = feat.where(F.col("order_date") < F.lit(cutoff_test))
test = feat.where(F.col("order_date") >= F.lit(cutoff_test))

display(train.select("order_date", "is_late").groupBy("is_late").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Train a simple baseline model (Spark Logistic Regression) + register in MLflow

# COMMAND ----------

# DBTITLE 1,Cell 11
# Convert to pandas for sklearn-based training (Spark ML feature transformers not available in Spark Connect)
import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder as SklearnOneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression as SklearnLogisticRegression
from sklearn.metrics import roc_auc_score, accuracy_score

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

# Convert to pandas
train_pd = train.select(cat_cols + num_cols + ["is_late"]).toPandas()
test_pd = test.select(cat_cols + num_cols + ["is_late"]).toPandas()

# Handle missing values
for col in num_cols:
    train_pd[col] = train_pd[col].fillna(0.0)
    test_pd[col] = test_pd[col].fillna(0.0)

for col in cat_cols:
    train_pd[col] = train_pd[col].fillna("unknown").astype(str)
    test_pd[col] = test_pd[col].fillna("unknown").astype(str)

# Prepare features and labels
X_train = train_pd[cat_cols + num_cols]
y_train = train_pd["is_late"]
X_test = test_pd[cat_cols + num_cols]
y_test = test_pd["is_late"]

# Build preprocessing pipeline
preprocessor = ColumnTransformer(
    transformers=[
        ("cat", SklearnOneHotEncoder(handle_unknown="ignore", sparse_output=False), cat_cols),
        ("num", "passthrough", num_cols),
    ]
)

experiment_name = f"/Shared/demand_planning_demo_{cfg.schema}"
mlflow.set_experiment(experiment_name)

with mlflow.start_run(run_name="late_delivery_risk_logreg") as run:
    # Fit preprocessor and transform data
    X_train_transformed = preprocessor.fit_transform(X_train)
    X_test_transformed = preprocessor.transform(X_test)

    # Train logistic regression
    lr = SklearnLogisticRegression(max_iter=50, C=1.0 / 0.05, penalty="l2", random_state=42)
    lr.fit(X_train_transformed, y_train)

    # Evaluate
    y_pred_proba = lr.predict_proba(X_test_transformed)[:, 1]
    y_pred = lr.predict(X_test_transformed)

    auc = roc_auc_score(y_test, y_pred_proba)
    acc = accuracy_score(y_test, y_pred)
    base_rate = float(y_train.mean())

    mlflow.log_params(
        {
            "catalog": cfg.catalog,
            "schema": cfg.schema,
            "train_lookback_days": TRAIN_LOOKBACK_DAYS,
            "test_days": TEST_DAYS,
            "model_type": "sklearn_logistic_regression",
        }
    )
    mlflow.log_metrics({"auc": auc, "accuracy@0.5": acc, "train_late_rate": base_rate})

    # Log sklearn model with preprocessing pipeline
    from sklearn.pipeline import Pipeline as SklearnPipeline

    model = SklearnPipeline([("preprocessor", preprocessor), ("classifier", lr)])

    # Create signature
    from mlflow.models.signature import infer_signature

    signature = infer_signature(X_train, pd.DataFrame({"prediction": y_pred, "probability": y_pred_proba}))

    mlflow.sklearn.log_model(
        model,
        artifact_path="model_sklearn",
        signature=signature,
        input_example=X_train.head(5),
    )

    mv = mlflow.register_model(f"runs:/{run.info.run_id}/model_sklearn", MODEL_NAME)
    mlflow.set_tag("registered_model_name", MODEL_NAME)
    mlflow.set_tag("registered_model_flavor", "sklearn")
    print(f"Registered sklearn model: {mv.name} version: {mv.version}")
    print(f"Test AUC: {auc:.4f}, Accuracy: {acc:.4f}, Train late rate: {base_rate:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Score into a Gold table used by the app

# COMMAND ----------

# DBTITLE 1,Cell 13
recent_cutoff = spark.sql("SELECT date_sub(current_date(), 91) AS d").collect()[0]["d"]
to_score = feat.where(F.col("order_date") >= F.lit(recent_cutoff))

# Convert to pandas for sklearn model scoring
to_score_pd = to_score.select(
    "order_id",
    "order_date",
    "customer_region",
    "channel",
    "dc_id",
    "plant_id",
    "sku_family",
    "is_late",
    *num_cols
).toPandas()

# Handle missing values (same as training)
for col in num_cols:
    to_score_pd[col] = to_score_pd[col].fillna(0.0)

for col in cat_cols:
    to_score_pd[col] = to_score_pd[col].fillna("unknown").astype(str)

# Prepare features
X_score = to_score_pd[cat_cols + num_cols]

# Score with sklearn model
to_score_pd["late_risk_flag"] = model.predict(X_score)
to_score_pd["late_risk_prob"] = model.predict_proba(X_score)[:, 1]
to_score_pd["actual_late"] = to_score_pd["is_late"].astype(int)

# Select final columns and convert back to Spark
scored_pd = to_score_pd[[
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
]]

scored = spark.createDataFrame(scored_pd)

# Drop existing table if it exists (may be a materialized view)
spark.sql(f"DROP TABLE IF EXISTS {cfg.table('order_late_risk_scored')}")

(scored.write.format("delta").mode("overwrite").saveAsTable(cfg.table("order_late_risk_scored")))
display(spark.table(cfg.table("order_late_risk_scored")).orderBy(F.desc("late_risk_prob")).limit(20))
