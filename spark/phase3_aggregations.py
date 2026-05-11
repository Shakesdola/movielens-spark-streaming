"""
Phase 3: Stateful running aggregations.
- Running average rating per movie (min 3 ratings)
- Rating volume + average per genre
Prints both tables to console on every micro-batch.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, explode, avg, count, round as spark_round
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
    .appName("MovieLens-Phase3-Aggregations")
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
    .load()
    .select(from_json(col("value").cast("string"), RATING_SCHEMA).alias("r"))
    .select(
        col("r.movie.movieId").alias("movieId"),
        col("r.movie.title").alias("title"),
        col("r.movie.genres").alias("genres"),
        col("r.rating").cast("double").alias("rating"),
    )
)

# ── Aggregation 1: running average per movie ──────────────────────────────
movie_stats = (
    parsed
    .groupBy("movieId", "title")
    .agg(
        spark_round(avg("rating"), 3).alias("avg_rating"),
        count("rating").alias("num_ratings"),
    )
    .filter(col("num_ratings") >= 3)
    .orderBy(col("avg_rating").desc())
)

# ── Aggregation 2: volume + average per genre ─────────────────────────────
genre_stats = (
    parsed
    .select(explode("genres").alias("genre"), "rating")
    .groupBy("genre")
    .agg(
        spark_round(avg("rating"), 3).alias("avg_rating"),
        count("rating").alias("num_ratings"),
    )
    .orderBy(col("num_ratings").desc())
)

q1 = (
    movie_stats.writeStream
    .format("console")
    .option("truncate", False)
    .option("numRows", 10)
    .outputMode("complete")
    .option("checkpointLocation", "/checkpoints/movies")
    .start()
)

q2 = (
    genre_stats.writeStream
    .format("console")
    .option("truncate", False)
    .option("numRows", 15)
    .outputMode("complete")
    .option("checkpointLocation", "/checkpoints/genres")
    .start()
)

print("=== Phase 3: Aggregations running (Ctrl+C to stop) ===")
spark.streams.awaitAnyTermination()
