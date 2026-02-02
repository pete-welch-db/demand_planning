# Databricks notebook source
# MAGIC %md
# MAGIC ## 5) ML in the loop — Late Delivery Risk
# MAGIC
# MAGIC Trains a Spark ML late-delivery risk model from `silver_erp_orders` (DLT output),
# MAGIC registers it in MLflow/Unity Catalog, and writes `order_late_risk_scored_ml`.
# MAGIC
# MAGIC **Note**: This notebook uses Spark ML Pipeline (native Databricks) and requires
# MAGIC a cluster with ML Runtime (not serverless).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dependency bootstrap

# COMMAND ----------

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

print(f"Model: {MODEL_NAME}")
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

train = feat.where(F.col("order_date") < F.lit(cutoff_test))
test = feat.where(F.col("order_date") >= F.lit(cutoff_test))

print(f"Train rows: {train.count():,}")
print(f"Test rows: {test.count():,}")

display(train.select("order_date", "is_late").groupBy("is_late").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Train Spark ML Pipeline + register in MLflow

# COMMAND ----------

# Feature columns
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

# Build Spark ML Pipeline
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

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 MLflow experiment + model registration

# COMMAND ----------

experiment_name = f"/Shared/demand_planning_demo_{cfg.schema}"
mlflow.set_experiment(experiment_name)

with mlflow.start_run(run_name="late_delivery_risk_spark_ml") as run:
    # Train
    fitted = model_pipeline.fit(filled_train)

    # Score test set
    scored_test = fitted.transform(filled_test)
    y_prob = vector_to_array(F.col("probability")).getItem(1)
    scored_test = scored_test.withColumn("p_late", y_prob)

    # Evaluate
    evaluator = BinaryClassificationEvaluator(labelCol="is_late", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    auc = float(evaluator.evaluate(scored_test))

    acc = float(
        scored_test.select((F.col("prediction") == F.col("is_late")).cast("double").alias("ok"))
        .agg(F.avg("ok").alias("acc"))
        .collect()[0]["acc"]
    )

    base_rate = float(filled_train.agg(F.avg(F.col("is_late").cast("double")).alias("br")).collect()[0]["br"])

    # Log parameters
    mlflow.log_params({
        "catalog": cfg.catalog,
        "schema": cfg.schema,
        "train_lookback_days": TRAIN_LOOKBACK_DAYS,
        "test_days": TEST_DAYS,
        "model_type": "spark_logistic_regression_pipeline",
        "reg_param": 0.05,
        "max_iter": 50,
    })

    # Log metrics
    mlflow.log_metrics({"auc": auc, "accuracy": acc, "train_late_rate": base_rate})

    # Log model artifacts
    try:
        # Log MAPE by late status as table
        late_stats = scored_test.groupBy("is_late").agg(
            F.count("*").alias("count"),
            F.avg("p_late").alias("avg_prob")
        ).toPandas()
        mlflow.log_table(late_stats, artifact_file="metrics/late_stats.json")
    except Exception as e:
        print(f"Warning: Could not log table artifact: {e}")

    # Log and register model in Unity Catalog
    mlflow.spark.log_model(fitted, artifact_path="model")
    mv = mlflow.register_model(f"runs:/{run.info.run_id}/model", MODEL_NAME)
    mlflow.set_tag("registered_model_name", MODEL_NAME)
    mlflow.set_tag("registered_model_version", mv.version)
    
    print(f"Registered Spark ML model: {mv.name} version: {mv.version}")
    print(f"Test AUC: {auc:.4f}, Accuracy: {acc:.4f}, Train late rate: {base_rate:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4 Score recent orders into Gold table

# COMMAND ----------

recent_cutoff = spark.sql("SELECT date_sub(current_date(), 91) AS d").collect()[0]["d"]
to_score = feat.where(F.col("order_date") >= F.lit(recent_cutoff))

# Fill nulls
to_score_filled = to_score
for c in num_cols:
    to_score_filled = to_score_filled.withColumn(c, F.coalesce(F.col(c).cast("double"), F.lit(0.0)))
for c in cat_cols:
    to_score_filled = to_score_filled.withColumn(c, F.coalesce(F.col(c).cast("string"), F.lit("unknown")))

# Score
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

print(f"Scored {scored.count():,} orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.5 Write scored table to Delta

# COMMAND ----------

SCORED_TABLE_NAME = "order_late_risk_scored_ml"

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
(scored.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(cfg.table(SCORED_TABLE_NAME)))

print(f"Wrote to {cfg.table(SCORED_TABLE_NAME)}")
display(spark.table(cfg.table(SCORED_TABLE_NAME)).orderBy(F.desc("late_risk_prob")).limit(20))
