"""
03_kmeans.py — User Segmentation (KMeans, Spark MLlib)
Input : /music/batch/clean
Output: /music/batch/user_segments, /music/batch/segment_profiles
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline

HDFS         = "hdfs://namenode:9000"
CLEAN_PATH   = f"{HDFS}/music/batch/clean"
SEG_PATH     = f"{HDFS}/music/batch/user_segments"
PROFILE_PATH = f"{HDFS}/music/batch/segment_profiles"
K            = 4

spark = SparkSession.builder \
    .appName("Music KMeans Segmentation") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(CLEAN_PATH)

user_features = df.groupBy("userId").agg(
    F.count("*").alias("total_plays"),
    F.countDistinct("artist").alias("unique_artists"),
    F.countDistinct("song").alias("unique_songs"),
    F.countDistinct("sessionId").alias("total_sessions"),
    F.round(F.avg("duration"), 1).alias("avg_duration"),
    F.countDistinct(F.to_date("event_ts")).alias("active_days"),
    F.first("level").alias("level"),
)

indexer   = StringIndexer(inputCol="level", outputCol="level_idx")
assembler = VectorAssembler(
    inputCols=["total_plays", "unique_artists", "unique_songs",
               "total_sessions", "avg_duration", "active_days", "level_idx"],
    outputCol="features_raw",
)
scaler = StandardScaler(inputCol="features_raw", outputCol="features",
                        withMean=True, withStd=True)
kmeans = KMeans(k=K, seed=42, featuresCol="features", predictionCol="segment")

model     = Pipeline(stages=[indexer, assembler, scaler, kmeans]).fit(user_features)
segmented = model.transform(user_features)

# Gán nhãn: sắp xếp theo avg_plays giảm dần → Power … At-Risk
LABELS = ["Power Users", "Regular Listeners", "Casual Listeners", "At-Risk Users"]
stats  = segmented.groupBy("segment") \
                  .agg(F.avg("total_plays").alias("avg_plays")) \
                  .orderBy(F.desc("avg_plays")).collect()
label_rows = [(row["segment"], LABELS[i] if i < len(LABELS) else f"Segment {i}")
              for i, row in enumerate(stats)]
label_df   = spark.createDataFrame(label_rows, ["segment", "segment_label"])
segmented  = segmented.join(label_df, on="segment")

# Lưu user-level segments
segmented.select(
    "userId", "segment", "segment_label",
    "total_plays", "unique_artists", "unique_songs",
    "total_sessions", "avg_duration", "active_days", "level",
).write.mode("overwrite").parquet(SEG_PATH)

# Lưu segment profiles (cho chart)
profiles = segmented.groupBy("segment_label").agg(
    F.count("*").alias("user_count"),
    F.round(F.avg("total_plays"),   0).alias("avg_plays"),
    F.round(F.avg("unique_artists"),0).alias("avg_artists"),
    F.round(F.avg("active_days"),   0).alias("avg_active_days"),
    F.round(F.avg("avg_duration"),  0).alias("avg_duration_sec"),
    F.round(
        F.sum(F.when(F.col("level") == "paid", 1).otherwise(0)) / F.count("*") * 100, 1
    ).alias("paid_pct"),
).orderBy(F.desc("avg_plays"))
profiles.show(truncate=False)
profiles.write.mode("overwrite").parquet(PROFILE_PATH)

print(f"KMeans done — {K} segments → {SEG_PATH}")
spark.stop()
