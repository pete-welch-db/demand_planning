# Databricks notebook source
# MAGIC %md
# MAGIC ## 5) ML in the loop — Late Delivery Risk
# MAGIC
# MAGIC Trains a simple late-delivery risk model from `silver_erp_orders` (DLT output),
# MAGIC registers it in MLflow, and writes `order_late_risk_scored_ml` (to avoid colliding with the DLT-owned `order_late_risk_scored` table).

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

# Native-first: Spark ML Pipeline (scales; no toPandas)
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

# Fill nulls consistently
filled_train = train
filled_test = test
for c in num_cols:
    filled_train = filled_train.withColumn(c, F.coalesce(F.col(c).cast("double"), F.lit(0.0)))
    filled_test = filled_test.withColumn(c, F.coalesce(F.col(c).cast("double"), F.lit(0.0)))
for c in cat_cols:
    filled_train = filled_train.withColumn(c, F.coalesce(F.col(c).cast("string"), F.lit("unknown")))
    filled_test = filled_test.withColumn(c, F.coalesce(F.col(c).cast("string"), F.lit("unknown")))

indexers = [StringIndexer(inputCol=c, outputCol=f"{c}__idx", handleInvalid="keep") for c in cat_cols]
encoder = OneHotEncoder(
    inputCols=[f"{c}__idx" for c in cat_cols],
    outputCols=[f"{c}__ohe" for c in cat_cols],
    handleInvalid="keep",
)
assembler = VectorAssembler(
    inputCols=[f"{c}__ohe" for c in cat_cols] + num_cols,
    outputCol="features",
    handleInvalid="keep",
)
lr = LogisticRegression(featuresCol="features", labelCol="is_late", maxIter=50, regParam=0.05, elasticNetParam=0.0)
model_pipeline = Pipeline(stages=[*indexers, encoder, assembler, lr])

experiment_name = f"/Shared/demand_planning_demo_{cfg.schema}"
mlflow.set_experiment(experiment_name)

with mlflow.start_run(run_name="late_delivery_risk_logreg_spark") as run:
    fitted = model_pipeline.fit(filled_train)

    scored_test = fitted.transform(filled_test)
    y_prob = vector_to_array(F.col("probability")).getItem(1)
    scored_test = scored_test.withColumn("p_late", y_prob)

    evaluator = BinaryClassificationEvaluator(labelCol="is_late", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    auc = float(evaluator.evaluate(scored_test))

    acc = float(
        scored_test.select((F.col("prediction") == F.col("is_late")).cast("double").alias("ok"))
        .agg(F.avg("ok").alias("acc"))
        .collect()[0]["acc"]
    )

    base_rate = float(filled_train.agg(F.avg(F.col("is_late").cast("double")).alias("br")).collect()[0]["br"])

    mlflow.log_params(
        {
            "catalog": cfg.catalog,
            "schema": cfg.schema,
            "train_lookback_days": TRAIN_LOOKBACK_DAYS,
            "test_days": TEST_DAYS,
            "model_type": "spark_logistic_regression_pipeline",
            "reg_param": 0.05,
            "max_iter": 50,
        }
    )
    mlflow.log_metrics({"auc": auc, "accuracy@0.5": acc, "train_late_rate": base_rate})

    # Log + register model in UC Model Registry.
    # In some runtimes (notably Spark Connect / serverless), Spark model logging can fail due to artifact FS restrictions.
    # We keep the job resilient by falling back to a lightweight pyfunc heuristic model if needed.
    try:
        mlflow.spark.log_model(fitted, artifact_path="model_spark")
        mv = mlflow.register_model(f"runs:/{run.info.run_id}/model_spark", MODEL_NAME)
        mlflow.set_tag("registered_model_name", MODEL_NAME)
        mlflow.set_tag("registered_model_flavor", "spark")
        print(f"Registered Spark model: {mv.name} version: {mv.version}")
    except Exception as e:
        mlflow.set_tag("registered_model_name", MODEL_NAME)
        mlflow.set_tag("registered_model_flavor", "pyfunc_heuristic_fallback")
        mlflow.set_tag("spark_model_log_error", (str(e)[:500] if e else "unknown"))

        import mlflow.pyfunc
        from mlflow.models.signature import infer_signature
        import pandas as pd
        import numpy as np

        class LateRiskHeuristicPyfunc(mlflow.pyfunc.PythonModel):
            def predict(self, context, model_input):
                df = model_input.copy()
                for c, default in [
                    ("days_to_request", 999.0),
                    ("channel", "unknown"),
                    ("units_ordered", 0.0),
                    ("woy", 0.0),
                ]:
                    if c not in df.columns:
                        df[c] = default

                days = pd.to_numeric(df["days_to_request"], errors="coerce").fillna(999.0).astype(float)
                units = pd.to_numeric(df["units_ordered"], errors="coerce").fillna(0.0).astype(float)
                woy = pd.to_numeric(df["woy"], errors="coerce").fillna(0.0).astype(float)
                ch = df["channel"].astype(str)

                winter = ((woy >= 48) | (woy <= 8)).astype(float)
                is_contractor_dot = ch.isin(["contractor", "DOT"]).astype(float)
                short_lead = (days <= 5).astype(float)
                big_units = (units >= 50).astype(float)

                prob = 0.12 + 0.18 * short_lead + 0.08 * is_contractor_dot + 0.10 * big_units + 0.06 * winter
                prob = np.clip(prob, 0.01, 0.99)
                return pd.DataFrame({"late_risk_prob": prob.astype(float)})

        input_example = pd.DataFrame(
            [{"days_to_request": 7, "channel": "stocking", "units_ordered": 12, "woy": 10}]
        )
        signature = infer_signature(input_example, LateRiskHeuristicPyfunc().predict(None, input_example))

        mlflow.pyfunc.log_model(
            artifact_path="model_pyfunc",
            python_model=LateRiskHeuristicPyfunc(),
            input_example=input_example,
            signature=signature,
            pip_requirements=[
                "numpy==1.26.4",
                "pandas==2.2.3",
                "mlflow[databricks]==2.14.2",
            ],
        )
        mv = mlflow.register_model(f"runs:/{run.info.run_id}/model_pyfunc", MODEL_NAME)
        print(f"Registered pyfunc fallback model: {mv.name} version: {mv.version}")
    print(f"Test AUC: {auc:.4f}, Accuracy: {acc:.4f}, Train late rate: {base_rate:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Score into a Gold table used by the app

# COMMAND ----------

# DBTITLE 1,Cell 13
recent_cutoff = spark.sql("SELECT date_sub(current_date(), 91) AS d").collect()[0]["d"]
to_score = feat.where(F.col("order_date") >= F.lit(recent_cutoff))

to_score_filled = to_score
for c in num_cols:
    to_score_filled = to_score_filled.withColumn(c, F.coalesce(F.col(c).cast("double"), F.lit(0.0)))
for c in cat_cols:
    to_score_filled = to_score_filled.withColumn(c, F.coalesce(F.col(c).cast("string"), F.lit("unknown")))

scored = (
    fitted.transform(to_score_filled)
    .withColumn("late_risk_prob", vector_to_array(F.col("probability")).getItem(1))
    .withColumn("late_risk_flag", (F.col("late_risk_prob") >= F.lit(0.5)).cast("int"))
    .withColumn("actual_late", F.col("is_late").cast("int"))
    .select(
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
    )
)

SCORED_TABLE_NAME = "order_late_risk_scored_ml"

# Drop existing object if it exists (table/view); avoid collisions with the DLT-owned `order_late_risk_scored`.
try:
    spark.sql(f"DROP VIEW IF EXISTS {cfg.table(SCORED_TABLE_NAME)}")
except Exception:
    pass
try:
    spark.sql(f"DROP TABLE IF EXISTS {cfg.table(SCORED_TABLE_NAME)}")
except Exception:
    pass

(scored.write.format("delta").mode("overwrite").saveAsTable(cfg.table(SCORED_TABLE_NAME)))
display(spark.table(cfg.table(SCORED_TABLE_NAME)).orderBy(F.desc("late_risk_prob")).limit(20))
