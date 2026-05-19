"""
als_streaming.py

Compares:
  - Batch ALS collaborative filtering model
  - Real-time Spark streaming windowed genre trends

Goal:
  ALS learns LONG-TERM user preferences from all ratings.
  Streaming detects SHORT-TERM genre activity in sliding windows.

Data sources:
  output/raw_ratings/      → raw streaming events collected by Spark
  output/windowed_trends/  → true streaming window aggregations

Outputs:
  analysis_output/als_stream/
"""

import os
import glob
import numpy as np
import pandas as pd

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, avg, count,
    round as spark_round,
)

from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

# ─────────────────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────────────────
RAW_DIR = "output/raw_ratings"
WIN_DIR = "output/windowed_trends"

OUT_DIR = "analysis_output/als_stream"
os.makedirs(OUT_DIR, exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────
# Spark session
# ─────────────────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("ALS-vs-Streaming-Trends")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print(" ALS vs Streaming Window Trends")
print("=" * 70)

# ─────────────────────────────────────────────────────────────────────────
# Load raw ratings
# ─────────────────────────────────────────────────────────────────────────
print("\n[1] Loading raw ratings...")

raw_files = [
    f for f in glob.glob(f"{RAW_DIR}/*")
    if f.endswith(".parquet")
]

if not raw_files:
    raise Exception("No parquet files found in output/raw_ratings")

df = spark.read.parquet(*raw_files)

df.cache()

print(f"    Records loaded: {df.count():,}")

# ─────────────────────────────────────────────────────────────────────────
# Prepare ALS dataset
# ─────────────────────────────────────────────────────────────────────────
print("\n[2] Preparing ALS training data...")

user_indexer = StringIndexer(
    inputCol="userId",
    outputCol="userIndex"
)

movie_indexer = StringIndexer(
    inputCol="movieId",
    outputCol="movieIndex"
)

pipeline = Pipeline(stages=[
    user_indexer,
    movie_indexer,
])

index_model = pipeline.fit(df)

indexed = index_model.transform(df)

indexed = indexed.select(
    col("userIndex").cast("int").alias("userIndex"),
    col("movieIndex").cast("int").alias("movieIndex"),
    col("rating").cast("float"),
    col("genres"),
    col("title"),
)

train, test = indexed.randomSplit([0.8, 0.2], seed=42)

print(f"    Training rows: {train.count():,}")
print(f"    Test rows:     {test.count():,}")

# ─────────────────────────────────────────────────────────────────────────
# Train ALS
# ─────────────────────────────────────────────────────────────────────────
print("\n[3] Training ALS model...")

als = ALS(
    maxIter=10,
    rank=10,
    regParam=0.1,
    userCol="userIndex",
    itemCol="movieIndex",
    ratingCol="rating",
    coldStartStrategy="drop",
)

als_model = als.fit(train)

print("    ALS training complete.")

# ─────────────────────────────────────────────────────────────────────────
# Evaluate ALS
# ─────────────────────────────────────────────────────────────────────────
print("\n[4] Evaluating ALS predictions...")

predictions = als_model.transform(test)

evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol="rating",
    predictionCol="prediction",
)

rmse = evaluator.evaluate(predictions)

print(f"    RMSE: {rmse:.4f}")

# ─────────────────────────────────────────────────────────────────────────
# ALS genre preference profile
# LONG-TERM preference learned from all ratings
# ─────────────────────────────────────────────────────────────────────────
print("\n[5] Computing ALS genre preference profile...")

als_genre = (
    predictions
    .select(
        explode("genres").alias("genre"),
        col("prediction"),
    )
    .groupBy("genre")
    .agg(
        spark_round(avg("prediction"), 3).alias("als_predicted_avg"),
        count("prediction").alias("als_prediction_count"),
    )
    .orderBy(col("als_predicted_avg").desc())
)

als_genre_pd = als_genre.toPandas()

print(als_genre_pd.head(10).to_string(index=False))

# ─────────────────────────────────────────────────────────────────────────
# Load TRUE streaming window output
# ─────────────────────────────────────────────────────────────────────────
print("\n[6] Loading streaming window aggregations...")

win = pd.read_parquet(WIN_DIR)

print(f"    Window rows loaded: {len(win):,}")

# Parse window columns
win["window_start"] = pd.to_datetime(
    win["window"].apply(lambda w: w["start"]),
    unit="ns"
)

win["window_end"] = pd.to_datetime(
    win["window"].apply(lambda w: w["end"]),
    unit="ns"
)

print(
    f"    Window span: "
    f"{win['window_start'].min()} "
    f"→ "
    f"{win['window_end'].max()}"
)

# ─────────────────────────────────────────────────────────────────────────
# Latest streaming trend snapshot
# SHORT-TERM real-time trend
# ─────────────────────────────────────────────────────────────────────────
print("\n[7] Extracting latest streaming trend window...")

latest_window = sorted(win["window_end"].unique())[-1]

latest_stream = (
    win[win["window_end"] == latest_window]
    .copy()
)

latest_stream = latest_stream.sort_values(
    "rating_count",
    ascending=False
)

print("\nLatest streaming window:")
print(
    latest_stream[
        ["genre", "avg_rating", "rating_count"]
    ].head(10).to_string(index=False)
)

# ─────────────────────────────────────────────────────────────────────────
# Compare ALS vs Streaming
# ─────────────────────────────────────────────────────────────────────────
print("\n[8] Comparing ALS long-term preference vs streaming trends...")

compare = latest_stream.merge(
    als_genre_pd,
    on="genre",
    how="inner"
)

compare["avg_diff"] = (
        compare["avg_rating"]
        - compare["als_predicted_avg"]
).abs()

compare = compare.sort_values(
    "rating_count",
    ascending=False
)

print("\nGenre comparison:")
print(
    compare[
        [
            "genre",
            "avg_rating",
            "als_predicted_avg",
            "avg_diff",
            "rating_count",
        ]
    ].to_string(index=False)
)

# Correlation
corr = compare["avg_rating"].corr(
    compare["als_predicted_avg"]
)

print(f"\n    Correlation: {corr:.4f}")

# ─────────────────────────────────────────────────────────────────────────
# Chart 1
# Streaming vs ALS genre averages
# ─────────────────────────────────────────────────────────────────────────
print("\n[9] Generating charts...")

fig1 = go.Figure()

fig1.add_trace(go.Bar(
    name="Streaming Window Avg",
    x=compare["genre"],
    y=compare["avg_rating"],
))

fig1.add_trace(go.Bar(
    name="ALS Predicted Avg",
    x=compare["genre"],
    y=compare["als_predicted_avg"],
))

fig1.update_layout(
    template="plotly_dark",
    barmode="group",
    title=(
        "Streaming Window Trends vs ALS Genre Preferences"
    ),
    xaxis_title="Genre",
    yaxis_title="Average Rating",
    xaxis_tickangle=-35,
    height=550,
)

fig1.write_html(
    f"{OUT_DIR}/1_stream_vs_als_genres.html"
)

fig1.write_image(
    f"{OUT_DIR}/1_stream_vs_als_genres.png"
)

print("    Saved: 1_stream_vs_als_genres")

# ─────────────────────────────────────────────────────────────────────────
# Chart 2
# Scatter comparison
# ─────────────────────────────────────────────────────────────────────────
fig2 = px.scatter(
    compare,
    x="avg_rating",
    y="als_predicted_avg",
    size="rating_count",
    text="genre",
    title=(
        "ALS Long-Term Preference vs "
        "Streaming Short-Term Trend"
    ),
    labels={
        "avg_rating": "Streaming Window Avg",
        "als_predicted_avg": "ALS Predicted Avg",
    },
)

fig2.add_shape(
    type="line",
    x0=1,
    y0=1,
    x1=5,
    y1=5,
    line=dict(
        dash="dash",
        color="red",
    ),
)

fig2.update_traces(
    textposition="top center"
)

fig2.update_layout(
    template="plotly_dark",
    height=600,
)

fig2.write_html(
    f"{OUT_DIR}/2_scatter_stream_vs_als.html"
)

fig2.write_image(
    f"{OUT_DIR}/2_scatter_stream_vs_als.png"
)

print("    Saved: 2_scatter_stream_vs_als")

# ─────────────────────────────────────────────────────────────────────────
# Chart 3
# Window evolution over time
# ─────────────────────────────────────────────────────────────────────────
top_genres = (
    latest_stream
    .sort_values("rating_count", ascending=False)
    .head(6)["genre"]
    .tolist()
)

fig3 = go.Figure()

for genre in top_genres:

    gdf = (
        win[win["genre"] == genre]
        .sort_values("window_end")
    )

    fig3.add_trace(go.Scatter(
        x=gdf["window_end"],
        y=gdf["avg_rating"],
        mode="lines+markers",
        name=genre,
    ))

fig3.update_layout(
    template="plotly_dark",
    title=(
        "Streaming Genre Trends Over Time"
    ),
    xaxis_title="Window End Time",
    yaxis_title="Average Rating",
    height=600,
)

fig3.write_html(
    f"{OUT_DIR}/3_streaming_trends_over_time.html"
)

fig3.write_image(
    f"{OUT_DIR}/3_streaming_trends_over_time.png"
)

print("    Saved: 3_streaming_trends_over_time")

# ─────────────────────────────────────────────────────────────────────────
# Chart 4
# Summary table
# ─────────────────────────────────────────────────────────────────────────
summary = pd.DataFrame({
    "Metric": [
        "Total ratings",
        "ALS RMSE",
        "Genres analysed",
        "Streaming windows",
        "Correlation (ALS vs stream)",
        "Best streaming genre",
        "Best ALS genre",
    ],
    "Value": [
        f"{df.count():,}",
        f"{rmse:.4f}",
        len(compare),
        win["window_end"].nunique(),
        f"{corr:.4f}",
        latest_stream.sort_values(
            "avg_rating",
            ascending=False
        ).iloc[0]["genre"],
        als_genre_pd.sort_values(
            "als_predicted_avg",
            ascending=False
        ).iloc[0]["genre"],
    ]
})

fig4 = go.Figure(data=[go.Table(
    header=dict(
        values=list(summary.columns),
    ),
    cells=dict(
        values=[
            summary["Metric"],
            summary["Value"],
        ]
    )
)])

fig4.update_layout(
    template="plotly_dark",
    title="ALS vs Streaming Summary",
    height=450,
)

fig4.write_html(
    f"{OUT_DIR}/4_summary_table.html"
)

fig4.write_image(
    f"{OUT_DIR}/4_summary_table.png"
)

print("    Saved: 4_summary_table")

# ─────────────────────────────────────────────────────────────────────────
# Final interpretation
# ─────────────────────────────────────────────────────────────────────────
print(f"""

======================================================================
 FINAL ANALYSIS
======================================================================

ALS MODEL
---------
- Learns long-term latent user preferences
- Trained offline on the full ratings dataset
- Uses matrix factorization collaborative filtering
- RMSE: {rmse:.4f}

STREAMING WINDOWS
-----------------
- Detect short-term genre activity in real time
- Updated incrementally every micro-batch
- Uses 10-minute sliding windows
- Captures trending genres immediately

KEY DIFFERENCE
--------------
ALS:
  Predictive / long-term / personalized

Streaming:
  Reactive / short-term / trend-oriented

INTERPRETATION
--------------
If ALS and streaming agree strongly for a genre:
  → Long-term preference aligns with current trend

If streaming spikes but ALS does not:
  → Temporary popularity surge

If ALS is high but streaming is low:
  → Stable long-term preference but not currently trending

Correlation between systems: {corr:.4f}

Charts saved to:
  {OUT_DIR}

======================================================================

""")

spark.stop()