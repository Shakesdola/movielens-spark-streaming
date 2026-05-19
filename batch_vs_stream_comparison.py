"""
batch_vs_stream_comparison.py

Compares batch recomputation vs Spark Structured Streaming output
at the GENRE level (the only windowed aggregation available).

What this shows:
  - Batch:  compute genre stats once over ALL raw_ratings (offline)
  - Stream: use the windowed_trends written by Spark incrementally

Data sources:
  output/raw_ratings/     — every rating event Spark received
  output/windowed_trends/ — genre aggregations per sliding window

Run locally (no Docker needed):
  pip install pandas pyarrow matplotlib seaborn
  python batch_vs_stream_comparison.py
"""

import os
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

RAW_DIR   = "output/raw_ratings"
WIN_DIR   = "output/windowed_trends"
OUT_DIR   = "analysis_output/batch_comparison"
os.makedirs(OUT_DIR, exist_ok=True)

plt.rcParams.update({
    "figure.facecolor": "#0f1117",
    "axes.facecolor":   "#1e2130",
    "axes.edgecolor":   "#444",
    "axes.labelcolor":  "white",
    "xtick.color":      "white",
    "ytick.color":      "white",
    "text.color":       "white",
    "grid.color":       "#333",
    "grid.linestyle":   "--",
    "grid.alpha":       0.4,
    "legend.facecolor": "#1e2130",
    "legend.edgecolor": "#555",
    "font.size":        10,
})
CB = "#4C9BE8"   # batch blue
CS = "#48bb78"   # stream green
CA = "#ed8936"   # accent orange

print("=" * 60)
print("  Batch vs Stream Comparison — Genre Level")
print("=" * 60)

# ─────────────────────────────────────────────────────────────────────────
# Step 1: Load raw ratings (the streaming output from Spark)
# ─────────────────────────────────────────────────────────────────────────
print("\n[1] Loading raw ratings (written incrementally by Spark)...")
raw = pd.read_parquet(RAW_DIR, engine="pyarrow")
raw["event_time"] = pd.to_datetime(raw["event_time"])
raw_exploded = (
    raw.explode("genres")
    .rename(columns={"genres": "genre"})
)
raw_exploded = raw_exploded[
    raw_exploded["genre"].notna() & (raw_exploded["genre"] != "")
    ]
print(f"    {len(raw):,} rating events across "
      f"{raw['event_time'].min().strftime('%H:%M:%S')} → "
      f"{raw['event_time'].max().strftime('%H:%M:%S')}")
print(f"    {raw['movieId'].nunique():,} unique movies, "
      f"{raw['userId'].nunique():,} unique users")

# ─────────────────────────────────────────────────────────────────────────
# Step 2: BATCH — recompute genre stats over ALL raw data at once
# ─────────────────────────────────────────────────────────────────────────
print("\n[2] BATCH — recomputing genre stats over full dataset...")
batch = (
    raw_exploded
    .groupby("genre")["rating"]
    .agg(
        batch_avg   ="mean",
        batch_count ="count",
    )
    .reset_index()
    .sort_values("batch_count", ascending=False)
)
print(f"    {len(batch)} genres found")
print(f"    Overall avg rating (batch): {raw['rating'].mean():.4f}")

# ─────────────────────────────────────────────────────────────────────────
# Step 3: STREAM — read Spark's windowed genre aggregations
# ─────────────────────────────────────────────────────────────────────────
print("\n[3] STREAM — reading Spark windowed genre aggregations...")
win = pd.read_parquet(WIN_DIR, engine="pyarrow")
win["window_start"] = pd.to_datetime(
    win["window"].apply(lambda w: w["start"]), unit="ns"
)
win["window_end"] = pd.to_datetime(
    win["window"].apply(lambda w: w["end"]), unit="ns"
)
n_windows = win["window_end"].nunique()
print(f"    {len(win):,} rows across {n_windows} sliding windows")
print(f"    Window span: {win['window_start'].min().strftime('%H:%M')} → "
      f"{win['window_end'].max().strftime('%H:%M')}")

# Accumulate stream results: weighted average across all windows
# This is the stream's "final answer" after processing all data
stream_final = (
    win.groupby("genre")
    .apply(lambda g: pd.Series({
        "stream_avg":   (g["avg_rating"] * g["rating_count"]).sum()
                        / g["rating_count"].sum(),
        "stream_count": g["rating_count"].sum(),
    }))
    .reset_index()
)

# ─────────────────────────────────────────────────────────────────────────
# Step 4: Compare batch vs stream final answers
# ─────────────────────────────────────────────────────────────────────────
print("\n[4] Comparing batch vs stream final answers...")
compare = batch.merge(stream_final, on="genre", how="inner")
compare["diff"] = (compare["batch_avg"] - compare["stream_avg"]).abs()
compare["count_diff"] = (compare["batch_count"] - compare["stream_count"]).abs()

print(f"\n{'Genre':<14} {'Batch Avg':>9} {'Stream Avg':>10} "
      f"{'Diff':>7}  {'Batch #':>8} {'Stream #':>9}")
print("-" * 65)
for _, r in compare.sort_values("batch_count", ascending=False).iterrows():
    flag = "✓" if r["diff"] < 0.01 else f"Δ{r['diff']:.3f}"
    print(f"  {r['genre']:<13} {r['batch_avg']:>9.3f} {r['stream_avg']:>10.3f} "
          f"  {flag:>7}  {int(r['batch_count']):>8,} {int(r['stream_count']):>9,}")

max_diff = compare["diff"].max()
mean_diff = compare["diff"].mean()
print(f"\n  Max difference:  {max_diff:.4f}")
print(f"  Mean difference: {mean_diff:.4f}")
if mean_diff < 0.05:
    print("  ✅ Stream converges closely to batch results.")
else:
    print("  ⚠️  Notable difference — stream window may not cover all data.")

# ─────────────────────────────────────────────────────────────────────────
# Step 5: Stream convergence over time
# How does each genre's avg rating evolve window by window?
# ─────────────────────────────────────────────────────────────────────────
print("\n[5] Computing stream convergence over time...")

# For each window, compute the cumulative weighted average up to that point
# This shows how streaming results converge toward the batch answer
windows_sorted = sorted(win["window_end"].unique())
convergence_records = []

for genre in compare["genre"].tolist():
    running_sum   = 0.0
    running_count = 0
    for w_end in windows_sorted:
        chunk = win[(win["genre"] == genre) & (win["window_end"] == w_end)]
        if chunk.empty:
            continue
        r_count = chunk["rating_count"].values[0]
        r_avg   = chunk["avg_rating"].values[0]
        running_sum   += r_avg * r_count
        running_count += r_count
        cumulative_avg = running_sum / running_count
        convergence_records.append({
            "genre":          genre,
            "window_end":     w_end,
            "cumulative_avg": cumulative_avg,
            "cumulative_count": running_count,
        })

conv_df = pd.DataFrame(convergence_records)

# ─────────────────────────────────────────────────────────────────────────
# Charts
# ─────────────────────────────────────────────────────────────────────────
print("\n[6] Generating charts...")

# ── Chart 1: Batch vs Stream final avg rating per genre ───────────────────
fig, ax = plt.subplots(figsize=(14, 6))
genres   = compare.sort_values("batch_count", ascending=False)["genre"].tolist()
x        = np.arange(len(genres))
w        = 0.35
b_avgs   = compare.set_index("genre")["batch_avg"].reindex(genres)
s_avgs   = compare.set_index("genre")["stream_avg"].reindex(genres)

bars1 = ax.bar(x - w/2, b_avgs, w, label="Batch (offline, one-shot)",
               color=CB, alpha=0.85)
bars2 = ax.bar(x + w/2, s_avgs, w, label="Stream (Spark, incremental)",
               color=CS, alpha=0.85)

ax.set_xticks(x)
ax.set_xticklabels(genres, rotation=35, ha="right", fontsize=9)
ax.set_ylabel("Average Rating")
ax.set_ylim(0, 5.5)
ax.set_title("Final Average Rating per Genre — Batch vs Stream", color="white")
ax.legend()
ax.grid(axis="y")

# Mark differences
for i, (b, s) in enumerate(zip(b_avgs, s_avgs)):
    diff = abs(b - s)
    if diff > 0.01:
        ax.annotate(f"Δ{diff:.2f}",
                    xy=(i, max(b, s) + 0.1),
                    ha="center", fontsize=7, color=CA)
    else:
        ax.annotate("✓", xy=(i, max(b, s) + 0.05),
                    ha="center", fontsize=9, color=CS)

plt.tight_layout()
plt.savefig(f"{OUT_DIR}/1_genre_avg_batch_vs_stream.png",
            dpi=150, bbox_inches="tight")
plt.close()
print("    Saved: 1_genre_avg_batch_vs_stream.png")

# ── Chart 2: Rating volume per genre ─────────────────────────────────────
fig, ax = plt.subplots(figsize=(14, 5))
b_counts = compare.set_index("genre")["batch_count"].reindex(genres)
s_counts = compare.set_index("genre")["stream_count"].reindex(genres)

ax.bar(x - w/2, b_counts, w, label="Batch",  color=CB, alpha=0.85)
ax.bar(x + w/2, s_counts, w, label="Stream", color=CS, alpha=0.85)
ax.set_xticks(x)
ax.set_xticklabels(genres, rotation=35, ha="right", fontsize=9)
ax.set_ylabel("Number of Ratings")
ax.set_title("Rating Count per Genre — Batch vs Stream\n"
             "(stream count = sum across all sliding windows — may differ due to overlapping windows)",
             color="white")
ax.legend()
ax.grid(axis="y")
plt.tight_layout()
plt.savefig(f"{OUT_DIR}/2_genre_count_batch_vs_stream.png",
            dpi=150, bbox_inches="tight")
plt.close()
print("    Saved: 2_genre_count_batch_vs_stream.png")

# ── Chart 3: Scatter — batch avg vs stream avg per genre ──────────────────
fig, ax = plt.subplots(figsize=(8, 7))
sc = ax.scatter(
    compare["batch_avg"], compare["stream_avg"],
    c=compare["batch_count"], cmap="Blues",
    s=120, edgecolors="white", linewidths=0.7, zorder=3,
)
for _, r in compare.iterrows():
    ax.annotate(r["genre"],
                (r["batch_avg"], r["stream_avg"]),
                textcoords="offset points", xytext=(6, 3),
                fontsize=8, color="white")
ax.plot([2.5, 5], [2.5, 5], color=CA, linestyle="--",
        linewidth=1.5, label="Perfect agreement (y=x)")
cb = plt.colorbar(sc, ax=ax)
cb.set_label("# ratings (batch)", color="white")
cb.ax.yaxis.set_tick_params(color="white")
plt.setp(cb.ax.yaxis.get_ticklabels(), color="white")
ax.set_xlabel("Batch avg rating")
ax.set_ylabel("Stream avg rating")
ax.set_title("Genre Avg Rating: Batch vs Stream\n"
             "(dots near the diagonal = strong agreement)", color="white")
ax.legend()
ax.grid(True)

corr = compare["batch_avg"].corr(compare["stream_avg"])
ax.text(0.05, 0.93, f"Pearson r = {corr:.4f}",
        transform=ax.transAxes, fontsize=11, color="white",
        bbox=dict(boxstyle="round", facecolor="#2d3748", alpha=0.8))
plt.tight_layout()
plt.savefig(f"{OUT_DIR}/3_genre_scatter_batch_vs_stream.png",
            dpi=150, bbox_inches="tight")
plt.close()
print("    Saved: 3_genre_scatter_batch_vs_stream.png")

# ── Chart 4: Convergence of top 6 genres over time ───────────────────────
top6_genres = (compare.sort_values("batch_count", ascending=False)
               .head(6)["genre"].tolist())
batch_refs  = compare.set_index("genre")["batch_avg"]

fig, axes = plt.subplots(2, 3, figsize=(16, 9))
fig.suptitle("Stream Convergence Toward Batch Answer — Top 6 Genres\n"
             "(dashed line = batch answer; solid line = stream running average)",
             fontsize=12, color="white")

for ax, genre in zip(axes.flat, top6_genres):
    gdf = conv_df[conv_df["genre"] == genre].sort_values("window_end")
    batch_val = batch_refs[genre]

    ax.plot(range(len(gdf)), gdf["cumulative_avg"],
            color=CS, linewidth=2.5, marker="o", markersize=5,
            label="Stream (cumulative avg)")
    ax.axhline(batch_val, color=CA, linestyle="--",
               linewidth=1.8, label=f"Batch = {batch_val:.3f}")
    ax.fill_between(range(len(gdf)), gdf["cumulative_avg"], batch_val,
                    alpha=0.1, color=CA)

    final_diff = abs(gdf["cumulative_avg"].iloc[-1] - batch_val)
    ax.set_title(f"{genre}  (final Δ={final_diff:.3f})", color="white", fontsize=10)
    ax.set_xlabel("Window #")
    ax.set_ylabel("Avg Rating")
    ax.set_ylim(2.5, 5.0)
    ax.legend(fontsize=7)
    ax.grid(True)

plt.tight_layout()
plt.savefig(f"{OUT_DIR}/4_stream_convergence_by_genre.png",
            dpi=150, bbox_inches="tight")
plt.close()
print("    Saved: 4_stream_convergence_by_genre.png")


# ── Chart 5: Summary table ────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(13, 5))
fig.suptitle("Batch vs Stream — Summary Comparison", fontsize=13, color="white")
ax.axis("off")

best_batch_genre  = compare.sort_values("batch_avg",  ascending=False).iloc[0]["genre"]
best_stream_genre = compare.sort_values("stream_avg", ascending=False).iloc[0]["genre"]
corr = compare["batch_avg"].corr(compare["stream_avg"])

rows = [
    ["Total ratings processed",
     f"{len(raw):,}",
     f"{len(raw):,} (same data, written by Spark)"],
    ["Unique genres analysed",
     f"{len(batch)}",
     f"{len(stream_final)}"],
    ["Overall avg rating",
     f"{raw['rating'].mean():.4f}",
     "updated after each micro-batch"],
    ["Highest avg genre",
     best_batch_genre,
     best_stream_genre],
    ["Avg rating correlation (batch vs stream)",
     "—",
     f"Pearson r = {corr:.4f}"],
    ["Max genre avg difference",
     "—",
     f"{max_diff:.4f}  ({'✓ negligible' if max_diff < 0.05 else '⚠ notable'})"],
    ["Sliding windows",
     "N/A — single pass",
     f"{n_windows} windows, results visible after each one"],
    ["When results are available",
     "Only after ALL data is loaded",
     "After every 30-second micro-batch"],
]

table = ax.table(
    cellText=rows,
    colLabels=["Metric", "Batch", "Stream (Spark)"],
    loc="center", cellLoc="left",
)
table.auto_set_font_size(False)
table.set_fontsize(9)
table.scale(1, 2.5)
for (r, c), cell in table.get_celld().items():
    if r == 0:
        cell.set_facecolor("#2d3748")
        cell.set_text_props(color="white", fontweight="bold")
    elif c == 1:
        cell.set_facecolor("#1a2744")
        cell.set_text_props(color=CB)
    elif c == 2:
        cell.set_facecolor("#1a2e1a")
        cell.set_text_props(color=CS)
    else:
        cell.set_facecolor("#1e2130")
        cell.set_text_props(color="white")
    cell.set_edgecolor("#444")
plt.tight_layout()
plt.savefig(f"{OUT_DIR}/5_summary_table.png",
            dpi=150, bbox_inches="tight")
plt.close()
print("    Saved: 6_summary_table.png")

print(f"\n✅ Done. Charts saved to ./{OUT_DIR}/")
