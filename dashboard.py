"""
dashboard.py
Live dashboard that reads from Spark parquet output and updates every 5 seconds.
Shows real-time genre trends, rating distributions, and event rates.

Run this locally (outside Docker):
    pip install dash plotly pandas pyarrow
    python dashboard.py
Then open: http://localhost:8050

Requires phase5_parquet_sink.py to be running (writes to ./output/raw_ratings/).
"""

import glob
import os
import threading
import time

import pandas as pd

import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ── Config ────────────────────────────────────────────────────────────────
RAW_DIR     = "output/raw_ratings"   # Spark parquet output (mounted from Docker)
MAX_RECORDS = 2000                  # keep last N ratings in memory

# ── Shared in-memory store ────────────────────────────────────────────────
records = []          # list of dicts
lock    = threading.Lock()

# ── Background Spark parquet polling thread ───────────────────────────────
def spark_thread():
    print(f"Watching Spark output at: {RAW_DIR}")
    while True:
        try:
            parquet_files = glob.glob(os.path.join(RAW_DIR, "*.parquet")) + \
                            glob.glob(os.path.join(RAW_DIR, "*.snappy.parquet"))
            if parquet_files:
                df = pd.concat(
                    [pd.read_parquet(f) for f in parquet_files],
                    ignore_index=True,
                )
                df = df.sort_values("event_time").tail(MAX_RECORDS)
                new_records = df.to_dict("records")
                with lock:
                    records[:] = new_records
                print(f"  Loaded {len(new_records)} records from {len(parquet_files)} file(s)")
            else:
                print(f"  No parquet files yet in {RAW_DIR}, waiting...")
        except Exception as e:
            print(f"Parquet read error: {e}")
        time.sleep(5)

threading.Thread(target=spark_thread, daemon=True).start()

# ── Dash app ──────────────────────────────────────────────────────────────
app = dash.Dash(__name__)
app.title = "MovieLens Live Dashboard"

app.layout = html.Div(
    style={"backgroundColor": "#0f1117", "minHeight": "100vh", "padding": "20px", "fontFamily": "sans-serif"},
    children=[
        # Header
        html.Div([
            html.H1("🎬 MovieLens Live Stream Dashboard",
                    style={"color": "#ffffff", "marginBottom": "4px"}),
            html.P("Real-time movie rating analytics via Apache Spark Structured Streaming",
                   style={"color": "#888", "marginTop": "0"}),
        ]),

        # Stats row
        html.Div(id="stats-row", style={"display": "flex", "gap": "16px", "marginBottom": "20px"}),

        # Charts row 1
        html.Div([
            html.Div(dcc.Graph(id="genre-bar"),   style={"flex": "1"}),
            html.Div(dcc.Graph(id="rating-hist"), style={"flex": "1"}),
        ], style={"display": "flex", "gap": "16px", "marginBottom": "16px"}),

        # Charts row 2
        html.Div([
            html.Div(dcc.Graph(id="event-rate"),  style={"flex": "1"}),
            html.Div(dcc.Graph(id="top-movies"),  style={"flex": "1"}),
        ], style={"display": "flex", "gap": "16px"}),

        # Auto-refresh interval
        dcc.Interval(id="interval", interval=5000, n_intervals=0),
    ]
)

DARK = "#1e2130"
CARD = {"backgroundColor": DARK, "borderRadius": "8px", "padding": "16px 24px",
        "flex": "1", "color": "white", "textAlign": "center"}

def stat_card(label, value, color="#4C9BE8"):
    return html.Div([
        html.Div(str(value), style={"fontSize": "2rem", "fontWeight": "bold", "color": color}),
        html.Div(label,      style={"fontSize": "0.85rem", "color": "#aaa"}),
    ], style=CARD)

@app.callback(
    [Output("stats-row",  "children"),
     Output("genre-bar",  "figure"),
     Output("rating-hist","figure"),
     Output("event-rate", "figure"),
     Output("top-movies", "figure")],
    Input("interval", "n_intervals"),
)
def update(n):
    with lock:
        snapshot = list(records)

    if not snapshot:
        empty = go.Figure()
        empty.update_layout(template="plotly_dark", paper_bgcolor=DARK, plot_bgcolor=DARK,
                            title="Waiting for data...")
        return [stat_card("Ratings", 0), stat_card("Movies", 0),
                stat_card("Users", 0), stat_card("Avg Rating", "-")], \
               empty, empty, empty, empty

    df = pd.DataFrame(snapshot)
    exploded = df.explode("genres").rename(columns={"genres": "genre"})

    # ── Stats ────────────────────────────────────────────────────────────
    stats = [
        stat_card("Ratings Seen",    len(df),                          "#4C9BE8"),
        stat_card("Unique Movies",   df["movieId"].nunique(),           "#48bb78"),
        stat_card("Unique Users",    df["userId"].nunique(),            "#ed8936"),
        stat_card("Avg Rating",      f"{df['rating'].mean():.2f} ⭐",   "#9f7aea"),
    ]

    layout = dict(template="plotly_dark", paper_bgcolor=DARK, plot_bgcolor=DARK,
                  margin=dict(t=40, b=40, l=40, r=20), font=dict(color="white"))

    # ── Genre bar ────────────────────────────────────────────────────────
    genre_stats = (
        exploded.groupby("genre")["rating"]
        .agg(avg_rating="mean", count="count")
        .reset_index()
        .sort_values("count", ascending=False)
        .head(12)
    )
    fig_genre = px.bar(genre_stats, x="genre", y="count", color="avg_rating",
                       color_continuous_scale="Blues",
                       title="Rating Volume & Avg Score by Genre",
                       labels={"count": "Ratings", "avg_rating": "Avg ⭐"})
    fig_genre.update_layout(**layout)
    fig_genre.update_xaxes(tickangle=-35)

    # ── Rating histogram ─────────────────────────────────────────────────
    fig_hist = px.histogram(df, x="rating", nbins=9,
                            title="Rating Distribution",
                            color_discrete_sequence=["#4C9BE8"])
    fig_hist.update_layout(**layout, bargap=0.1)

    # ── Event rate over time ─────────────────────────────────────────────
    df["bucket"] = df["event_time"].dt.floor("10s")
    rate = df.groupby("bucket").size().reset_index(name="count").tail(30)
    fig_rate = px.line(rate, x="bucket", y="count", markers=True,
                       title="Event Rate (ratings per 10 seconds)",
                       labels={"bucket": "Time", "count": "Ratings"})
    fig_rate.update_traces(line_color="#4C9BE8")
    fig_rate.update_layout(**layout)

    # ── Top movies ───────────────────────────────────────────────────────
    top = (
        df.groupby("title")["rating"]
        .agg(avg="mean", count="count")
        .reset_index()
        .query("count >= 1")
        .sort_values("avg", ascending=False)
        .head(10)
    )
    fig_top = px.bar(top, x="avg", y="title", orientation="h",
                     title="Top Rated Movies (min 2 ratings)",
                     color="avg", color_continuous_scale="Greens",
                     labels={"avg": "Avg Rating", "title": ""})
    fig_top.update_layout(**layout, yaxis={"autorange": "reversed"},
                          coloraxis_showscale=False)

    return stats, fig_genre, fig_hist, fig_rate, fig_top


if __name__ == "__main__":
    print("Starting dashboard at http://localhost:8050")
    app.run(debug=False, host="0.0.0.0", port=8050)