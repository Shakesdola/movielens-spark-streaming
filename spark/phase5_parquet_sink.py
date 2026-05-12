"""
Phase 5: Save stream output to Parquet files.
- Raw parsed ratings    → /output/raw_ratings/
- Windowed genre trends → /output/windowed_trends/
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, explode, avg, count, window,
    round as spark_round, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

MOVIE_SCHEMA = StructType([
    StructField("movieId", StringType()),
    StructField("title",   StringType()),
    StructField("genres",  ArrayType(StringType())),
])
RATING_SCHEMA = StructType([
    StructField("userId",    StringType()),
    StructField("movie",     MOVIE_SCHEMA),
    StructField("rating",    StringType()),
    StructField("timestamp", StringType()),
])

spark = (
    SparkSession.builder
    .appName("MovieLens-Phase5-Parquet")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

parsed = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "ratings")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
    .select(from_json(col("value").cast("string"), RATING_SCHEMA).alias("r"))
    .select(
        col("r.userId").alias("userId"),
        col("r.movie.movieId").alias("movieId"),
        col("r.movie.title").alias("title"),
        col("r.movie.genres").alias("genres"),
        col("r.rating").cast("double").alias("rating"),
        current_timestamp().alias("event_time"),
    )
)

q1 = (
    parsed.writeStream
    .format("parquet")
    .option("path", "/output/raw_ratings")
    .option("checkpointLocation", "/checkpoints/parquet_raw")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .start()
)

windowed = (
    parsed
    .withWatermark("event_time", "30 seconds")
    .select(explode("genres").alias("genre"), "rating", "event_time")
    .groupBy(
        window(col("event_time"), "10 minutes", "2 minutes"),
        col("genre"),
    )
    .agg(
        spark_round(avg("rating"), 3).alias("avg_rating"),
        count("rating").alias("rating_count"),
    )
)

q2 = (
    windowed.writeStream
    .format("parquet")
    .option("path", "/output/windowed_trends")
    .option("checkpointLocation", "/checkpoints/parquet_windows")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .start()
)

print("=== Phase 5: Writing to Parquet (runs for 5 minutes then stops) ===")

import time
time.sleep(1800)
q1.stop()
q2.stop()
print("=== Done. Check /output/ for Parquet files. ===")