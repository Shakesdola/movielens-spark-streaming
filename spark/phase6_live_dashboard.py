"""
phase6_live_dashboard.py

Pipeline: Producer → Kafka → Spark Structured Streaming → memory sink → Dash dashboard

Charts:
  1. Rating Volume & Avg Score by Genre (bar)
  2. Top Rated Movies — global (all time) + recent 10-min sliding window
  3. Ratings per 10-second interval — rolling line chart (last 9 buckets)
  4. Distribution of Movie Average Ratings (histogram)

Open: http://localhost:8050
"""

import threading
import time
from collections import deque
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, explode, avg, count, window,
    round as spark_round, current_timestamp,
)
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import pandas as pd

# ─────────────────────────────────────────────────────────────────────────
# Schema
# ─────────────────────────────────────────────────────────────────────────
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

# ─────────────────────────────────────────────────────────────────────────
# Spark session
# ─────────────────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("MovieLens-Live-Dashboard")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────────────────────────────────────
# Kafka source
# ─────────────────────────────────────────────────────────────────────────
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

# ─────────────────────────────────────────────────────────────────────────
# Memory sink queries
# ─────────────────────────────────────────────────────────────────────────

# Q1: running stats per movie (global)
q1 = (
    parsed
    .groupBy("movieId", "title")
    .agg(
        spark_round(avg("rating"), 3).alias("avg_rating"),
        count("rating").alias("num_ratings"),
    )
    .writeStream
    .format("memory")
    .queryName("movie_stats")
    .outputMode("complete")
    .trigger(processingTime="10 seconds")
    .start()
)

# Q2: running stats per genre (global)
q2 = (
    parsed
    .select(explode("genres").alias("genre"), "rating")
    .groupBy("genre")
    .agg(
        spark_round(avg("rating"), 3).alias("avg_rating"),
        count("rating").alias("num_ratings"),
    )
    .writeStream
    .format("memory")
    .queryName("genre_stats")
    .outputMode("complete")
    .trigger(processingTime="10 seconds")
    .start()
)

# Q3: 10-min sliding window, 2-min slide — per movie
q3 = (
    parsed
    .withWatermark("event_time", "3 minutes")
    .groupBy(
        window(col("event_time"), "10 minutes", "2 minutes"),
        col("movieId"),
        col("title"),
    )
    .agg(
        spark_round(avg("rating"), 3).alias("avg_rating"),
        count("rating").alias("num_ratings"),
    )
    .writeStream
    .format("memory")
    .queryName("window_movie_stats")
    .outputMode("complete")
    .trigger(processingTime="5 seconds")
    .start()
)

# # Q4: running stats per user (global)
q4 = (
    parsed
    .groupBy("userId")
    .agg(
        count("rating").alias("num_ratings"),
        spark_round(avg("rating"), 3).alias("avg_rating"),
    )
    .writeStream
    .format("memory")
    .queryName("user_stats")
    .outputMode("complete")
    .trigger(processingTime="10 seconds")
    .start()
)

print("=== Spark streaming queries started. Launching dashboard... ===")

# ─────────────────────────────────────────────────────────────────────────
# Rolling line chart state
# Snapshots total rating count every 10s, stores the per-bucket delta
# ─────────────────────────────────────────────────────────────────────────
ROLLING_WINDOW  = 9
rolling_times   = deque(maxlen=ROLLING_WINDOW)
rolling_counts  = deque(maxlen=ROLLING_WINDOW)
rolling_lock    = threading.Lock()
last_total_seen = 0


def snapshot_loop():
    global last_total_seen
    while True:
        time.sleep(10)
        try:
            df = spark.sql(
                "SELECT sum(num_ratings) as total FROM movie_stats"
            ).toPandas()
            if df.empty or df["total"].iloc[0] is None:
                continue
            current_total = int(df["total"].iloc[0])
            delta = max(current_total - last_total_seen, 0)
            last_total_seen = current_total
            label = datetime.now().strftime("%H:%M:%S")
            with rolling_lock:
                rolling_times.append(label)
                rolling_counts.append(delta)
        except Exception as e:
            print(f"  [snapshot_loop] {e}")


threading.Thread(target=snapshot_loop, daemon=True).start()

# ─────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────
DARK = "#1e2130"
BG   = "#0f1117"
LAYOUT_BASE = dict(
    template="plotly_dark",
    paper_bgcolor=DARK,
    plot_bgcolor=DARK,
    margin=dict(t=50, b=40, l=40, r=20),
    font=dict(color="white"),
)
CARD_STYLE = {
    "backgroundColor": DARK, "borderRadius": "8px",
    "padding": "16px 24px", "flex": "1",
    "color": "white", "textAlign": "center",
}


def stat_card(label, value, color="#4C9BE8"):
    return html.Div([
        html.Div(str(value),
                 style={"fontSize": "2rem", "fontWeight": "bold", "color": color}),
        html.Div(label,
                 style={"fontSize": "0.85rem", "color": "#aaa"}),
    ], style=CARD_STYLE)


def empty_fig(msg="Waiting for Spark micro-batch..."):
    fig = go.Figure()
    fig.update_layout(**LAYOUT_BASE, title=msg)
    return fig


def wrap_title(title: str, max_chars: int = 35) -> str:
    """Break a long movie title with <br> so it wraps inside the chart."""
    if len(title) <= max_chars:
        return title
    idx = title.rfind(" ", 0, max_chars)
    if idx == -1:
        idx = max_chars
    return title[:idx] + "<br>" + wrap_title(title[idx:].strip(), max_chars)


# ─────────────────────────────────────────────────────────────────────────
# Dash layout
# ─────────────────────────────────────────────────────────────────────────
app = dash.Dash(__name__)
app.title = "MovieLens Live — Spark Memory Sink"

app.layout = html.Div(
    style={"backgroundColor": BG, "minHeight": "100vh",
           "padding": "20px", "fontFamily": "sans-serif"},
    children=[
        html.Div([
            html.H1("🎬 MovieLens Live Stream Dashboard",
                    style={"color": "#ffffff", "marginBottom": "4px"}),
            html.P(
                "Pipeline: Kafka → Spark Structured Streaming → Memory sink → Dashboard",
                style={"color": "#888", "marginTop": "0"},
            ),
        ]),

        # KPI row
        html.Div(id="stats-row",
                 style={"display": "flex", "gap": "16px", "marginBottom": "20px"}),

        # Row 1: genre bar + top movies
        html.Div([
            html.Div(dcc.Graph(id="genre-bar"),   style={"flex": "1"}),
            html.Div(dcc.Graph(id="top-movies"),   style={"flex": "1"}),
        ], style={"display": "flex", "gap": "16px", "marginBottom": "16px"}),

        # Row 2: rolling line chart + rating histogram
        html.Div([
            html.Div(dcc.Graph(id="rolling-line"), style={"flex": "1"}),
            html.Div(dcc.Graph(id="rating-hist"), style={"flex": "1"}),
        ], style={"display": "flex", "gap": "16px"}),

        dcc.Interval(id="interval", interval=10000, n_intervals=0),
    ]
)


# ─────────────────────────────────────────────────────────────────────────
# Callback
# ─────────────────────────────────────────────────────────────────────────
@app.callback(
    [Output("stats-row",    "children"),
     Output("genre-bar",    "figure"),
     Output("top-movies",   "figure"),
     Output("rolling-line", "figure"),
     Output("rating-hist",  "figure")],
    Input("interval", "n_intervals"),
)
def update(_n):
    # Query Spark memory tables
    try:
        df_movies = spark.sql("SELECT * FROM movie_stats").toPandas()
        df_genres = spark.sql("SELECT * FROM genre_stats").toPandas()
        df_window = spark.sql("SELECT * FROM window_movie_stats").toPandas()
        df_users  = spark.sql("SELECT * FROM user_stats").toPandas()
    except Exception as e:
        print(f"  SQL error: {e}")
        empty = empty_fig()
        return [stat_card("—", "—")] * 4, empty, empty, empty, empty

    if df_movies.empty:
        empty = empty_fig()
        return (
            [stat_card("Ratings Seen",  "—"),
             stat_card("Unique Movies", "—"),
             stat_card("Unique Users", "—"),
             stat_card("Avg Rating",    "—")],
            empty, empty, empty, empty,
        )

    # ── KPI ───────────────────────────────────────────────────────────────
    total_ratings = int(df_movies["num_ratings"].sum())
    avg_rating = (
            (df_movies["avg_rating"] * df_movies["num_ratings"]).sum()
            / df_movies["num_ratings"].sum()
    )
    stats = [
        stat_card("Ratings Processed", f"{total_ratings:,}",  "#4C9BE8"),
        stat_card("Unique Movies",      len(df_movies),         "#48bb78"),
        stat_card("Unique Users",      len(df_users),         "#ed8936"),
        stat_card("Avg Rating",         f"{avg_rating:.2f} ⭐", "#9f7aea"),
    ]

    # ── Chart 1: genre bar ────────────────────────────────────────────────
    top_genres = df_genres.sort_values("num_ratings", ascending=False).head(12)
    fig_genre = px.bar(
        top_genres, x="genre", y="num_ratings", color="avg_rating",
        color_continuous_scale="Blues",
        title="Rating Volume & Avg Score by Genre",
        labels={"genre": "Genre", "num_ratings": "Ratings", "avg_rating": "Avg ⭐"},
    )
    fig_genre.update_layout(**LAYOUT_BASE)
    fig_genre.update_xaxes(tickangle=-35)


    # ── Chart 2: recent top movies — windowed only ────────────────────
    window_top = pd.DataFrame()
    if not df_window.empty:
        try:
            df_window["window_end"] = df_window["window"].apply(
                lambda w: w["end"] if isinstance(w, dict) else str(w)
            )

            windows = sorted(df_window["window_end"].unique())

            if len(windows) >= 2:
                latest_stable = windows[-2]
            else:
                latest_stable = windows[-1]

            window_top = (
                df_window[
                    (df_window["window_end"] == latest_stable) &
                    (df_window["num_ratings"] >= 2)
                    ]

                .sort_values("avg_rating", ascending=False)
                .head(10)
                .copy()
            )
            window_top["label"] = window_top["title"].apply(wrap_title)
        except Exception as e:
            print(f"  window top error: {e}")

    if window_top.empty:
        fig_top = empty_fig("Waiting for windowed data... (need ~10 min)")
    else:
        fig_top = go.Figure()
        fig_top.add_trace(go.Bar(
            x=window_top["avg_rating"],
            y=window_top["label"],
            orientation="h",
            marker=dict(
                color=window_top["avg_rating"],
                colorscale="Emrld", cmin=1, cmax=5,
            ),
            hovertemplate="%{y}<br>Avg rating: %{x:.2f} ⭐<extra></extra>",
            showlegend=False,
        ))
        fig_top.update_layout(
            template="plotly_dark",
            paper_bgcolor=DARK,
            plot_bgcolor=DARK,
            font=dict(color="white"),
            margin=dict(t=50, b=40, l=20, r=20),
            title_text="Recent Top Movies (last 10-min window)",
        )
        fig_top.update_xaxes(range=[0, 5.3], title_text="Avg Rating")
        fig_top.update_yaxes(
            autorange="reversed",
            automargin=True,
            tickfont=dict(size=10),
        )

    # ── Chart 3: rolling line — ratings per 10-second bucket ─────────────
    with rolling_lock:
        times  = list(rolling_times)
        counts = list(rolling_counts)

    if len(times) < 2:
        fig_line = empty_fig("Collecting data... (need ~20s to start)")
    else:
        df_line = pd.DataFrame({"time": times, "count": counts})
        fig_line = go.Figure()
        fig_line.add_trace(go.Scatter(
            x=df_line["time"],
            y=df_line["count"],
            mode="lines+markers",
            line=dict(color="#4C9BE8", width=2.5),
            marker=dict(size=8, color="#4C9BE8"),
            fill="tozeroy",
            fillcolor="rgba(76,155,232,0.15)",
            hovertemplate="%{x}<br>%{y} ratings<extra></extra>",
        ))
        fig_line.update_layout(
            **LAYOUT_BASE,
            title="Ratings Received per 10-Second Interval",
            xaxis=dict(title="Time", tickangle=-30, fixedrange=True),
            yaxis=dict(title="Ratings Count", rangemode="tozero"),
        )

    # ── Chart 4: distribution of movie average ratings ────────────────────
    fig_hist = go.Figure()
    fig_hist.add_trace(go.Histogram(
        x=df_movies["avg_rating"],
        xbins=dict(start=0.5, end=5.5, size=1.0),  # 5个区间: 0.5-1.5, 1.5-2.5, 2.5-3.5, 3.5-4.5, 4.5-5.5
        marker_color="#4C9BE8",
        hovertemplate="Ratings: %{x}<br>Count: %{y}<extra></extra>",
    ))
    fig_hist.update_layout(
        **LAYOUT_BASE,
        title="Distribution of Movie Average Ratings",
        xaxis_title="Avg Rating",
        yaxis_title="Count",
        bargap=0.1,
    )
    fig_hist.update_xaxes(tickmode='array', tickvals=[1, 2, 3, 4, 5])
    #
    # fig_hist = px.histogram(
    #     df_movies, x="avg_rating", nbins=5,
    #     title="Distribution of Movie Average Ratings",
    #     color_discrete_sequence=["#4C9BE8"],
    #     labels={"avg_rating": "Avg Rating"},
    # )
    # fig_hist.update_layout(**LAYOUT_BASE, bargap=0.1)



    return stats, fig_genre, fig_top, fig_line, fig_hist


# ─────────────────────────────────────────────────────────────────────────
# Run Dash in background thread — Spark stays alive in main thread
# ─────────────────────────────────────────────────────────────────────────
def run_dash():
    print("🎬 Dashboard at http://localhost:8050")
    app.run(debug=False, host="0.0.0.0", port=8050, use_reloader=False)


threading.Thread(target=run_dash, daemon=True).start()
spark.streams.awaitAnyTermination()