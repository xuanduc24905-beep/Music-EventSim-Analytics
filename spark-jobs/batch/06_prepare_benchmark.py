"""
06_prepare_benchmark.py — Chuẩn bị dữ liệu cho benchmark.
Export dữ liệu từ Parquet sang JSON để MapReduce có thể đọc được.
"""
from pyspark.sql import SparkSession

HDFS = "hdfs://namenode:9000"
CLEAN_PATH = f"{HDFS}/music/batch/clean"
BENCHMARK_DATA = f"{HDFS}/music/benchmark/data.json"

spark = SparkSession.builder \
    .appName("Benchmark Data Prep") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

df = spark.read.parquet(CLEAN_PATH)

# Gom về 1 file JSON để MapReduce chạy cho "cực" (thể hiện rõ sự chậm chạp của Single Reducer nếu cần)
# Hoặc để nhiều file để MapReduce chạy song song (YARN). Ở đây ta để mặc định.
print(f"Exporting {df.count()} records to JSON...")
df.write.mode("overwrite").json(BENCHMARK_DATA)

print(f"Data prepared at {BENCHMARK_DATA}")
spark.stop()
