"""
04_churn_prediction.py — Churn Prediction (Random Forest, Spark MLlib)

Label: user active trong 80% đầu timeline nhưng KHÔNG active trong 20% cuối = churned (1)
Input : /music/batch/clean
Output: /music/batch/churn_predictions, /music/batch/churn_summary
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline
from datetime import timedelta

HDFS       = "hdfs://namenode:9000"
CLEAN_PATH = f"{HDFS}/music/batch/clean"
OUT_PATH   = f"{HDFS}/music/batch/churn_predictions"
SUM_PATH   = f"{HDFS}/music/batch/churn_summary"

spark = SparkSession.builder \
    .appName("Music Churn Prediction") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(CLEAN_PATH)

# ── Time split: 80% đầu để tính features, 20% cuối để gán label ──
bounds = df.agg(F.min("event_ts").alias("mn"), F.max("event_ts").alias("mx")).collect()[0]
min_ts, max_ts = bounds["mn"], bounds["mx"]
span_sec = (max_ts - min_ts).total_seconds()
cutoff   = min_ts + timedelta(seconds=span_sec * 0.8)

df_early = df.filter(F.col("event_ts") <= cutoff)
df_late  = df.filter(F.col("event_ts") >  cutoff)

# ── Feature engineering (từ period đầu) ──────────────────────────
features = df_early.groupBy("userId").agg(
    F.count("*").alias("total_plays"),
    F.countDistinct("artist").alias("unique_artists"),
    F.countDistinct("song").alias("unique_songs"),
    F.countDistinct("sessionId").alias("total_sessions"),
    F.round(F.avg("duration"), 1).alias("avg_duration"),
    F.countDistinct(F.to_date("event_ts")).alias("active_days"),
    F.datediff(F.max("event_ts"), F.min("event_ts")).alias("days_span"),
    F.first("level").alias("level"),
    # Tần suất nghe trung bình mỗi ngày
    F.round(F.count("*") / (F.datediff(F.max("event_ts"), F.min("event_ts")) + 1), 2)
     .alias("plays_per_day"),
)

# ── Label: engagement-based churn ────────────────────────────────
# Thử time-split trước; nếu churn rate quá thấp (<5%) → dùng
# bottom-quartile engagement làm proxy churn (phù hợp với EventSim
# vì simulator chạy liên tục nên tất cả user đều active cả timeline)
early_users   = df_early.select("userId").distinct()
late_users    = df_late.select("userId").distinct()
time_churned  = early_users.join(late_users, "userId", "left_anti") \
                            .withColumn("label", F.lit(1))
time_retained = early_users.join(late_users, "userId", "inner") \
                            .withColumn("label", F.lit(0))
labeled_time  = time_churned.union(time_retained)

dataset_time  = features.join(labeled_time, "userId").na.fill(0)
churn_rate    = dataset_time.agg(F.avg("label")).collect()[0][0]
print(f"Time-split churn rate: {churn_rate:.1%}")

if churn_rate < 0.05:
    print("Churn rate quá thấp — dùng engagement-based labeling")
    # Bottom 25% plays_per_day = churned
    ppd_25 = features.approxQuantile("plays_per_day", [0.25], 0.01)[0]
    ad_25  = features.approxQuantile("active_days",   [0.25], 0.01)[0]
    labeled = features.withColumn(
        "label",
        F.when(
            (F.col("plays_per_day") <= ppd_25) & (F.col("active_days") <= ad_25), 1
        ).otherwise(0)
    ).select("userId", "label")
else:
    labeled = labeled_time

dataset   = features.join(labeled, "userId").na.fill(0)
churn_rate = dataset.agg(F.avg("label")).collect()[0][0]
print(f"Final churn rate: {churn_rate:.1%}")

# ── Train / Test split ────────────────────────────────────────────
train, test = dataset.randomSplit([0.8, 0.2], seed=42)

indexer   = StringIndexer(inputCol="level", outputCol="level_idx")
assembler = VectorAssembler(
    inputCols=["total_plays", "unique_artists", "unique_songs", "total_sessions",
               "avg_duration", "active_days", "days_span", "plays_per_day", "level_idx"],
    outputCol="features_raw",
)
scaler = StandardScaler(inputCol="features_raw", outputCol="features",
                        withMean=True, withStd=True)
rf = RandomForestClassifier(
    numTrees=100, maxDepth=6, seed=42,
    featuresCol="features", labelCol="label", probabilityCol="probability",
)

pipeline = Pipeline(stages=[indexer, assembler, scaler, rf])
model    = pipeline.fit(train)

# ── Evaluate ──────────────────────────────────────────────────────
preds    = model.transform(test)
evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
auc = evaluator.evaluate(preds)
print(f"AUC-ROC: {auc:.4f}")

# ── Predict toàn bộ users ─────────────────────────────────────────
all_preds = model.transform(dataset)

extract_prob = F.udf(lambda v: float(v[1]))
result = all_preds.withColumn("churn_probability", extract_prob(F.col("probability"))) \
    .withColumn(
        "risk_level",
        F.when(F.col("churn_probability") >= 0.7, "High")
         .when(F.col("churn_probability") >= 0.4, "Medium")
         .otherwise("Low")
    ).select(
        "userId", "label", "churn_probability", "risk_level",
        "total_plays", "unique_artists", "active_days",
        "plays_per_day", "avg_duration", "level",
    )

result.write.mode("overwrite").parquet(OUT_PATH)

# ── Summary metadata ──────────────────────────────────────────────
risk_counts = result.groupBy("risk_level").agg(F.count("*").alias("user_count"))
risk_counts.show()

summary = result.agg(
    F.count("*").alias("total_users"),
    F.sum(F.when(F.col("risk_level") == "High",   1).otherwise(0)).alias("high_risk"),
    F.sum(F.when(F.col("risk_level") == "Medium", 1).otherwise(0)).alias("medium_risk"),
    F.sum(F.when(F.col("risk_level") == "Low",    1).otherwise(0)).alias("low_risk"),
    F.round(F.avg("churn_probability") * 100, 1).alias("avg_churn_pct"),
).withColumn("auc_roc", F.lit(round(auc, 4))) \
 .withColumn("churn_rate_pct", F.lit(round(churn_rate * 100, 1)))

summary.write.mode("overwrite").parquet(SUM_PATH)

print(f"Churn prediction done — AUC: {auc:.4f} → {OUT_PATH}")
spark.stop()
