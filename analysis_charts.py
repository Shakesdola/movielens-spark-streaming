"""
analysis_charts.py
Reads the Parquet output from Phase 5 and generates charts for the report.
Run this locally after Phase 5 completes:
    pip install pandas pyarrow plotly kaleido
    python analysis_charts.py
"""

import os
import glob
import pandas as pd
import plotly.express as px
from plotly.subplots import make_subplots

RAW_DIR      = "output/raw_ratings"
WINDOWED_DIR = "output/windowed_trends"
OUT_DIR      = "output/charts"
os.makedirs(OUT_DIR, exist_ok=True)

# ── Load Parquet files ────────────────────────────────────────────────────
print("Loading Parquet files...")

raw_files      = [f for f in glob.glob(f"{RAW_DIR}/*") if f.endswith(".parquet") and not f.endswith(".crc")]
windowed_files = [f for f in glob.glob(f"{WINDOWED_DIR}/*") if f.endswith(".parquet") and not f.endswith(".crc")]

if not raw_files:
    print(f"No files found in {RAW_DIR} — make sure Phase 5 has run.")
    exit(1)

raw      = pd.concat([pd.read_parquet(f) for f in raw_files], ignore_index=True)
windowed = pd.concat([pd.read_parquet(f) for f in windowed_files], ignore_index=True) if windowed_files else pd.DataFrame()

print(f"  Raw ratings:     {len(raw):,} records")
print(f"  Windowed trends: {len(windowed):,} records")

# ── Explode genres ────────────────────────────────────────────────────────
raw_exploded = raw.explode("genres").rename(columns={"genres": "genre"})

# ── Chart 1: Rating distribution ──────────────────────────────────────────
fig1 = px.histogram(
    raw,
    x="rating",
    nbins=9,
    title="Rating Distribution — Live Stream Sample",
    labels={"rating": "Star Rating", "count": "Number of Ratings"},
    color_discrete_sequence=["#4C9BE8"],
)
fig1.update_layout(bargap=0.1)
fig1.write_image(f"{OUT_DIR}/01_rating_distribution.png")
fig1.write_html(f"{OUT_DIR}/01_rating_distribution.html")
print("  Chart 1 done: Rating distribution")

# ── Chart 2: Average rating per genre ─────────────────────────────────────
genre_avg = (
    raw_exploded.groupby("genre")["rating"]
    .agg(avg_rating="mean", count="count")
    .reset_index()
    .sort_values("avg_rating", ascending=False)
)

fig2 = px.bar(
    genre_avg,
    x="genre",
    y="avg_rating",
    color="avg_rating",
    color_continuous_scale="Blues",
    title="Average Rating by Genre — Running Aggregation",
    labels={"genre": "Genre", "avg_rating": "Average Rating"},
    text=genre_avg["avg_rating"].round(2),
)
fig2.update_traces(textposition="outside")
fig2.update_layout(xaxis_tickangle=-45, coloraxis_showscale=False)
fig2.write_image(f"{OUT_DIR}/02_avg_rating_by_genre.png")
fig2.write_html(f"{OUT_DIR}/02_avg_rating_by_genre.html")
print("  Chart 2 done: Avg rating by genre")

# ── Chart 3: Rating volume per genre ──────────────────────────────────────
genre_count = (
    raw_exploded.groupby("genre")["rating"]
    .count()
    .reset_index()
    .rename(columns={"rating": "count"})
    .sort_values("count", ascending=False)
)

fig3 = px.bar(
    genre_count,
    x="genre",
    y="count",
    title="Rating Volume by Genre — Live Stream",
    labels={"genre": "Genre", "count": "Number of Ratings"},
    color="count",
    color_continuous_scale="Teal",
)
fig3.update_layout(xaxis_tickangle=-45, coloraxis_showscale=False)
fig3.write_image(f"{OUT_DIR}/03_rating_volume_by_genre.png")
fig3.write_html(f"{OUT_DIR}/03_rating_volume_by_genre.html")
print("  Chart 3 done: Rating volume by genre")

# ── Chart 4: Event rate over time ─────────────────────────────────────────
raw["minute"] = pd.to_datetime(raw["event_time"]).dt.floor("30s")
ratings_over_time = raw.groupby("minute").size().reset_index(name="count")

fig4 = px.line(
    ratings_over_time,
    x="minute",
    y="count",
    title="Rating Event Rate Over Time (per 30 seconds)",
    labels={"minute": "Time", "count": "Ratings Received"},
    markers=True,
)
fig4.update_traces(line_color="#4C9BE8")
fig4.write_image(f"{OUT_DIR}/04_event_rate_over_time.png")
fig4.write_html(f"{OUT_DIR}/04_event_rate_over_time.html")
print("  Chart 4 done: Event rate over time")

# ── Chart 5: Windowed genre trends ────────────────────────────────────────
if not windowed.empty:
    windowed["window_start"] = pd.to_datetime(windowed["window"].apply(lambda x: x["start"]))

    top_genres = (
        windowed.groupby("genre")["rating_count"]
        .sum()
        .nlargest(8)
        .index.tolist()
    )
    windowed_top = windowed[windowed["genre"].isin(top_genres)]

    # Plan A: Sectional Diagram (a separate subdiagram for each type)
    # fig5 = px.scatter(
    #     windowed_top,
    #     x="window_start",
    #     y="rating_count",
    #     color="genre",
    #     facet_col="genre",      # 按类型分面
    #     facet_col_wrap=4,       # 每行4个
    #     title="Genre Activity in 10-Minute Sliding Windows",
    #     labels={"window_start": "Window Start", "rating_count": "Rating Count"},
    #     opacity=0.7,
    # )
    # fig5.update_traces(marker=dict(size=8))
    # fig5.show()
    # print("  Chart 5 done: Windowed genre trends")

    # Plan B: Scatter plot
    fig5 = px.scatter(
        windowed_top,
        x="window_start",
        y="rating_count",
        color="genre",
        title="Genre Activity in 10-Minute Sliding Windows",
        labels={"window_start": "Window Start", "rating_count": "Rating Count", "genre": "Genre"},
        opacity=0.7,
        size_max=8,
    )
    fig5.update_traces(marker=dict(size=8))
    fig5.write_image(f"{OUT_DIR}/05_windowed_genre_trends.png")
    fig5.write_html(f"{OUT_DIR}/05_windowed_genre_trends.html")
    print("  Chart 5 done: Windowed genre trends")

    # fig5 = px.scatter(
    #     windowed_top,
    #     x="window_start",
    #     y="rating_count",
    #     color="genre",
    #     title="Genre Activity in 10-Minute Sliding Windows",
    #     labels={"window_start": "Window Start", "rating_count": "Rating Count", "genre": "Genre"},
    #     markers=True,
    # )
    # fig5.write_image(f"{OUT_DIR}/05_windowed_genre_trends.png")
    # fig5.write_html(f"{OUT_DIR}/05_windowed_genre_trends.html")
    # print("  Chart 5 done: Windowed genre trends")
else:
    print("  Skipping Chart 5 — no windowed trend data")

# ── Summary ───────────────────────────────────────────────────────────────
print("\n=== Stream Summary ===")
print(f"  Total ratings processed : {len(raw):,}")
print(f"  Unique movies seen      : {raw['movieId'].nunique():,}")
print(f"  Unique users seen       : {raw['userId'].nunique():,}")
print(f"  Overall avg rating      : {raw['rating'].mean():.3f}")
print(f"  Rating std deviation    : {raw['rating'].std():.3f}")
print(f"  Time range              : {raw['event_time'].min()} -> {raw['event_time'].max()}")
print(f"\nCharts saved to: {OUT_DIR}/")