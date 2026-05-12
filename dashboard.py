"""
dashboard.py
Live dashboard that reads from Kafka and updates every 5 seconds.
Shows real-time genre trends, rating distributions, and event rates.

Run this locally (outside Docker):
    pip install dash plotly kafka-python pandas
    python dashboard.py
Then open: http://localhost:8050

OR run inside Docker by adding a dashboard service to docker-compose.yml.
"""

from collections import defaultdict
from datetime import datetime
import threading
import json
import time

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import pandas as pd

import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ── Config ────────────────────────────────────────────────────────────────
BOOTSTRAP    = "localhost:9094"   # change to kafka:9092 if running inside Docker
TOPIC        = "ratings"
MAX_RECORDS  = 2000               # keep last N ratings in memory

# ── Shared in-memory store ────────────────────────────────────────────────
records = []          # list of dicts
lock    = threading.Lock()

# ── Background Kafka consumer thread ─────────────────────────────────────
def kafka_thread():
    print("Connecting to Kafka...")
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=[BOOTSTRAP],
                auto_offset_reset="latest",
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                consumer_timeout_ms=1000,
            )
            print("✅ Connected to Kafka — consuming ratings...")
            break
        except NoBrokersAvailable:
            print("  Kafka not ready, retrying in 3s...")
            time.sleep(3)

    while True:
        try:
            for msg in consumer:
                d = msg.value
                record = {
                    "userId":    d.get("userId", "?"),
                    "movieId":   d["movie"]["movieId"],
                    "title":     d["movie"]["title"],
                    "genres":    d["movie"]["genres"],
                    "rating":    float(d["rating"]),
                    "event_time": datetime.now(),
                }
                with lock:
                    records.append(record)
                    if len(records) > MAX_RECORDS:
                        records.pop(0)
        except Exception as e:
            print(f"Kafka error: {e}, reconnecting...")
            time.sleep(3)

threading.Thread(target=kafka_thread, daemon=True).start()

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
            html.P("Real-time movie rating analytics via Apache Kafka + Spark Structured Streaming",
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
        .query("count >= 2")
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