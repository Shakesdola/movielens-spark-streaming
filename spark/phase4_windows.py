"""
Phase 4: Sliding window trend detection.
Window: 10 minutes, sliding every 2 minutes.
Detects which genres have the highest activity in recent windows.
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
    .appName("MovieLens-Phase4-Windows")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "Europe/Zurich")  # #5/17 added: Change the default time zone from UTC to Swiss timezone
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
        col("r.movie.title").alias("title"),
        col("r.movie.genres").alias("genres"),
        col("r.rating").cast("double").alias("rating"),
        current_timestamp().alias("event_time"),
    )
)

# Sliding window: 10-minute window, slides every 2 minutes
# Watermark tolerates up to 5 minutes of late data
windowed = (
    parsed
    .withWatermark("event_time", "5 minutes")
    .select(explode("genres").alias("genre"), "rating", "event_time")
    .groupBy(
        window(col("event_time"), "10 minutes", "2 minutes").alias("time_window"),
        col("genre"),
    )
    .agg(
        spark_round(avg("rating"), 3).alias("avg_rating"), #5/17 added alias
        count("rating").alias("rating_count"),
    )
    .select(  #5/17 added
        col("time_window.start").alias("window_start"),
        col("time_window.end").alias("window_end"),
        col("genre"),
        col("avg_rating"),
        col("rating_count"),
    )
)

query = (
    windowed.writeStream
    .format("console")
    .option("truncate", False)
    .option("numRows", 10)
    .outputMode("update")
    .option("checkpointLocation", "/checkpoints/windows")
    .trigger(processingTime='10 seconds')   # trigger defines when should spark runs a micro-batch
    .start()
)

print("=== Phase 4: Windowed trend detection running (Ctrl+C to stop) ===")
query.awaitTermination()