"""
Phase 2: Spark Structured Streaming — read Kafka and print parsed records.
Goal: confirm Spark can decode the JSON stream.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
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
    .appName("MovieLens-Phase2-Console")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "ratings")
    .option("startingOffsets", "latest")
    .load()
)

parsed = (
    raw
    .select(from_json(col("value").cast("string"), RATING_SCHEMA).alias("r"))
    .select(
        col("r.userId").alias("userId"),
        col("r.movie.movieId").alias("movieId"),
        col("r.movie.title").alias("title"),
        col("r.movie.genres").alias("genres"),
        col("r.rating").cast("double").alias("rating"),
        col("r.timestamp").cast("long").alias("timestamp"),
    )
)

query = (
    parsed.writeStream
    .format("console")
    .option("truncate", False)
    .option("numRows", 10)
    .outputMode("append")
    .start()
)

print("=== Phase 2: Spark running — waiting for micro-batches (Ctrl+C to stop) ===")
query.awaitTermination()
