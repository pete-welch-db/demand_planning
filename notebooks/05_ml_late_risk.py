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
import mlflow.spark

from pyspark.sql import functions as F

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

# Optional external signals (weekly, by region)
ext_tbl = f"{cfg.fq_schema}.silver_external_signals"
if spark.catalog.tableExists(ext_tbl.replace("`", "")):
    ext = spark.table(ext_tbl).select("week", "region", "construction_index", "precipitation_mm", "avg_temp_c")
    base = base.join(ext, (base.order_week == ext.week) & (base.customer_region == ext.region), "left").drop(ext.week).drop(ext.region)
else:
    base = (
        base.withColumn("construction_index", F.lit(None).cast("double"))
            .withColumn("precipitation_mm", F.lit(None).cast("double"))
            .withColumn("avg_temp_c", F.lit(None).cast("double"))
    )

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

indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in cat_cols]
encoder = OneHotEncoder(inputCols=[f"{c}_idx" for c in cat_cols], outputCols=[f"{c}_oh" for c in cat_cols])

assembler = VectorAssembler(
    inputCols=[f"{c}_oh" for c in cat_cols] + num_cols,
    outputCol="features",
    handleInvalid="keep",
)

lr = LogisticRegression(featuresCol="features", labelCol="is_late", maxIter=50, regParam=0.05, elasticNetParam=0.0)
pipe = Pipeline(stages=indexers + [encoder, assembler, lr])

experiment_name = f"/Shared/demand_planning_demo_{cfg.schema}"
mlflow.set_experiment(experiment_name)

with mlflow.start_run(run_name="late_delivery_risk_logreg") as run:
    model = pipe.fit(train)
    pred = model.transform(test)

    evaluator = BinaryClassificationEvaluator(labelCol="is_late", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    auc = evaluator.evaluate(pred)

    base_rate = float(train.agg(F.avg("is_late").alias("p")).collect()[0]["p"])
    acc = float(
        pred.withColumn("correct", (F.col("prediction").cast("int") == F.col("is_late")).cast("int"))
        .agg(F.avg("correct").alias("a"))
        .collect()[0]["a"]
    )

    mlflow.log_params(
        {
            "catalog": cfg.catalog,
            "schema": cfg.schema,
            "train_lookback_days": TRAIN_LOOKBACK_DAYS,
            "test_days": TEST_DAYS,
            "model_type": "spark_logistic_regression",
        }
    )
    mlflow.log_metrics({"auc": auc, "accuracy@0.5": acc, "train_late_rate": base_rate})

    mlflow.spark.log_model(model, artifact_path="model")

    mv = mlflow.register_model(f"runs:/{run.info.run_id}/model", MODEL_NAME)
    mlflow.set_tag("registered_model_name", MODEL_NAME)
    print("Registered model:", mv.name, "version:", mv.version)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5.3 Score into a Gold table used by the app

# COMMAND ----------
recent_cutoff = spark.sql("SELECT date_sub(current_date(), 91) AS d").collect()[0]["d"]
to_score = feat.where(F.col("order_date") >= F.lit(recent_cutoff))

scored = model.transform(to_score).select(
    "order_id",
    "order_date",
    "customer_region",
    "channel",
    "dc_id",
    "plant_id",
    "sku_family",
    "units_ordered",
    "days_to_request",
    F.col("probability").getItem(1).alias("late_risk_prob"),
    F.col("prediction").cast("int").alias("late_risk_flag"),
    F.col("is_late").cast("int").alias("actual_late"),
)

(scored.write.format("delta").mode("overwrite").saveAsTable(cfg.table("order_late_risk_scored")))
display(spark.table(cfg.table("order_late_risk_scored")).orderBy(F.desc("late_risk_prob")).limit(20))

