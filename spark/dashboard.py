"""
dashboard.py
Live dashboard that reads Parquet files written by phase5_parquet_sink.py (Spark).

Full pipeline:
    Producer → Kafka → Spark (phase5_parquet_sink) → Parquet files → this dashboard

Run inside Docker via docker-compose.yml (dashboard service).
Open: http://localhost:8050
"""

import glob
import os
import time
import threading

import pandas as pd
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go

# ── Paths (must match phase5_parquet_sink.py output paths) ────────────────
RAW_DIR      = "../output/raw_ratings"  # written by Spark q1
WINDOWED_DIR = "../output/windowed_trends"  # written by Spark q2

# ── Shared in-memory cache — refreshed from Parquet every few seconds ─────
cache = {"raw": pd.DataFrame(), "windowed": pd.DataFrame()}
cache_lock = threading.Lock()


def read_parquet_dir(path: str) -> pd.DataFrame:
    """Read all Parquet files in a directory into one DataFrame."""
    files = glob.glob(f"{path}/**/*.parquet", recursive=True) + \
            glob.glob(f"{path}/*.parquet")
    if not files:
        return pd.DataFrame()
    try:
        return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    except Exception as e:
        print(f"  Warning: could not read {path}: {e}")
        return pd.DataFrame()


def refresh_cache():
    """Background thread: re-reads Parquet files every 15 seconds."""
    while True:
        raw      = read_parquet_dir(RAW_DIR)
        windowed = read_parquet_dir(WINDOWED_DIR)
        with cache_lock:
            cache["raw"]      = raw
            cache["windowed"] = windowed
        n_raw = len(raw)
        n_win = len(windowed)
        print(f"  [cache refresh] raw={n_raw} rows, windowed={n_win} rows")
        time.sleep(15)


threading.Thread(target=refresh_cache, daemon=True).start()

# ── Dash app ──────────────────────────────────────────────────────────────
app = dash.Dash(__name__)
app.title = "MovieLens Live Dashboard (Spark → Parquet)"

DARK = "#1e2130"
BG   = "#0f1117"

CARD_STYLE = {
    "backgroundColor": DARK,
    "borderRadius": "8px",
    "padding": "16px 24px",
    "flex": "1",
    "color": "white",
    "textAlign": "center",
}

LAYOUT_BASE = dict(
    template="plotly_dark",
    paper_bgcolor=DARK,
    plot_bgcolor=DARK,
    margin=dict(t=45, b=40, l=40, r=20),
    font=dict(color="white"),
)


def stat_card(label, value, color="#4C9BE8"):
    return html.Div([
        html.Div(str(value), style={"fontSize": "2rem", "fontWeight": "bold", "color": color}),
        html.Div(label,      style={"fontSize": "0.85rem", "color": "#aaa"}),
    ], style=CARD_STYLE)


def empty_fig(msg="Waiting for Spark to write data..."):
    fig = go.Figure()
    fig.update_layout(**LAYOUT_BASE, title=msg)
    return fig


app.layout = html.Div(
    style={"backgroundColor": BG, "minHeight": "100vh",
           "padding": "20px", "fontFamily": "sans-serif"},
    children=[
        # ── Header ──────────────────────────────────────────────────────
        html.Div([
            html.H1("🎬 MovieLens Live Stream Dashboard",
                    style={"color": "#ffffff", "marginBottom": "4px"}),
            html.P(
                "Pipeline: Kafka Producer → Apache Spark Structured Streaming "
                "→ Parquet files → this dashboard",
                style={"color": "#888", "marginTop": "0"}
            ),
        ]),

        # ── KPI stat cards ───────────────────────────────────────────────
        html.Div(id="stats-row",
                 style={"display": "flex", "gap": "16px", "marginBottom": "20px"}),

        # ── Row 1: genre bar + rating histogram ──────────────────────────
        html.Div([
            html.Div(dcc.Graph(id="genre-bar"),    style={"flex": "1"}),
            html.Div(dcc.Graph(id="rating-hist"),  style={"flex": "1"}),
        ], style={"display": "flex", "gap": "16px", "marginBottom": "16px"}),

        # ── Row 2: windowed genre trends + top movies ────────────────────
        html.Div([
            html.Div(dcc.Graph(id="windowed-trends"), style={"flex": "1"}),
            html.Div(dcc.Graph(id="top-movies"),      style={"flex": "1"}),
        ], style={"display": "flex", "gap": "16px"}),

        # ── Auto-refresh every 15 seconds ────────────────────────────────
        dcc.Interval(id="interval", interval=15000, n_intervals=0),
    ]
)


@app.callback(
    [Output("stats-row",       "children"),
     Output("genre-bar",       "figure"),
     Output("rating-hist",     "figure"),
     Output("windowed-trends", "figure"),
     Output("top-movies",      "figure")],
    Input("interval", "n_intervals"),
)
def update(_n):
    with cache_lock:
        df  = cache["raw"].copy()
        win = cache["windowed"].copy()

    # ── Nothing yet ──────────────────────────────────────────────────────
    if df.empty:
        empty = empty_fig()
        placeholder = [
            stat_card("Ratings", "—"),
            stat_card("Movies",  "—"),
            stat_card("Users",   "—"),
            stat_card("Avg ⭐",  "—"),
        ]
        return placeholder, empty, empty, empty, empty

    # ── KPI cards ────────────────────────────────────────────────────────
    stats = [
        stat_card("Ratings Processed", f"{len(df):,}",                    "#4C9BE8"),
        stat_card("Unique Movies",      df["movieId"].nunique(),            "#48bb78"),
        stat_card("Unique Users",       df["userId"].nunique(),             "#ed8936"),
        stat_card("Avg Rating",         f"{df['rating'].mean():.2f} ⭐",    "#9f7aea"),
    ]

    # ── Explode genres for per-genre charts ──────────────────────────────
    exploded = df.explode("genres").rename(columns={"genres": "genre"})
    exploded  = exploded[exploded["genre"].notna() & (exploded["genre"] != "")]

    # ── Chart 1: rating volume + avg score by genre ───────────────────────
    genre_stats = (
        exploded.groupby("genre")["rating"]
        .agg(avg_rating="mean", count="count")
        .reset_index()
        .sort_values("count", ascending=False)
        .head(12)
    )
    fig_genre = px.bar(
        genre_stats, x="genre", y="count", color="avg_rating",
        color_continuous_scale="Blues",
        title="Rating Volume & Avg Score by Genre  (Spark → Parquet)",
        labels={"count": "Ratings", "avg_rating": "Avg ⭐"},
    )
    fig_genre.update_layout(**LAYOUT_BASE)
    fig_genre.update_xaxes(tickangle=-35)

    # ── Chart 2: rating distribution histogram ────────────────────────────
    fig_hist = px.histogram(
        df, x="rating", nbins=9,
        title="Rating Distribution  (Spark → Parquet)",
        color_discrete_sequence=["#4C9BE8"],
    )
    fig_hist.update_layout(**LAYOUT_BASE, bargap=0.1)

    # ── Chart 3: windowed genre trends from Spark windows ─────────────────
    if not win.empty and "genre" in win.columns and "rating_count" in win.columns:
        # Keep only the most recent window per genre
        if "window" in win.columns:
            win["window_end"] = win["window"].apply(
                lambda w: w["end"] if isinstance(w, dict) else str(w)
            )
            latest_window = win["window_end"].max()
            win_latest = win[win["window_end"] == latest_window]
        else:
            win_latest = win

        win_latest = win_latest.sort_values("rating_count", ascending=False).head(12)

        fig_win = px.bar(
            win_latest, x="genre", y="rating_count", color="avg_rating",
            color_continuous_scale="Oranges",
            title="Recent Genre Trends — Spark Sliding Window",
            labels={"rating_count": "Ratings in Window", "avg_rating": "Avg ⭐"},
        )
        fig_win.update_layout(**LAYOUT_BASE)
        fig_win.update_xaxes(tickangle=-35)
    else:
        fig_win = empty_fig("Waiting for Spark windowed data...")

    # ── Chart 4: top rated movies ─────────────────────────────────────────
    top = (
        df.groupby("title")["rating"]
        .agg(avg="mean", count="count")
        .reset_index()
        .query("count >= 3")
        .sort_values("avg", ascending=False)
        .head(10)
    )
    if top.empty:
        fig_top = empty_fig("Not enough ratings yet (need ≥3 per movie)...")
    else:
        fig_top = px.bar(
            top, x="avg", y="title", orientation="h",
            title="Top Rated Movies — min 3 ratings  (Spark → Parquet)",
            color="avg", color_continuous_scale="Greens",
            labels={"avg": "Avg Rating", "title": ""},
        )
        fig_top.update_layout(
            **LAYOUT_BASE,
            yaxis={"autorange": "reversed"},
            coloraxis_showscale=False,
        )

    return stats, fig_genre, fig_hist, fig_win, fig_top


if __name__ == "__main__":
    print("🎬 Dashboard starting at http://localhost:8050")
    print("   Reading from Spark Parquet output at /output/")
    app.run(debug=False, host="0.0.0.0", port=8050)