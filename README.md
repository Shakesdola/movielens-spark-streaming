# Real-time Analysis of Movie Ratings with Apache Spark Structured Streaming

**Team:** Daniela Shahini, Jiaxin Li  
**Course:** Distributed and Scalable Systems  
**Dataset:** MovieLens ratings stream via Apache Kafka

---

## Overview

This project implements an end-to-end streaming analytics pipeline that ingests live movie ratings from a Kafka broker and processes them incrementally using Apache Spark Structured Streaming. Rather than treating the MovieLens dataset as a static dump, we observe ratings as a continuous stream — closer to how production recommendation systems operate.

The pipeline progresses through four phases, from basic stream ingestion to windowed trend detection.

---

## Architecture

```
MovieLens Server (get.awesomedata.stream:9093)
        │
        ▼
┌───────────────────┐
│   Producer        │  Python script — pulls live ratings, pushes to Kafka
│   (producer.py)   │
└────────┬──────────┘
         │  topic: ratings
         ▼
┌───────────────────┐
│   Kafka Broker    │  Apache Kafka 3.7.0 (KRaft mode, no Zookeeper)
│   kafka:9092      │
└────────┬──────────┘
         │
         ▼
┌───────────────────┐
│   Spark Job       │  PySpark Structured Streaming 3.5.1
│   (phase*.py)     │  Stateful aggregations, sliding windows
└───────────────────┘
```

All components run in Docker containers via Docker Compose.

---

## Repository Structure

```
movielens-spark-streaming/
├── docker-compose.yml          # Defines kafka, producer, spark services
├── checkpoints/                # Spark streaming checkpoints (auto-created)
│   ├── movies/
│   ├── genres/
│   └── windows/
└── spark/
    ├── Dockerfile              # Image for both producer and spark containers
    ├── requirements.txt        # Python dependencies
    ├── producer.py             # Kafka producer — streams ratings into topic
    ├── phase1_kafka_test.py    # Plain Kafka consumer — confirms stream works
    ├── phase2_spark_console.py # Spark reads and parses JSON stream
    ├── phase3_aggregations.py  # Running avg per movie and genre (stateful)
    ├── phase4_windows.py       # Sliding window trend detection
    └── ml-latest-small/        # Static MovieLens metadata (for enrichment)
```

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (Windows/Mac) or Docker Engine (Linux)
- Docker Compose v2 (included with Docker Desktop)
- At least 4 GB RAM allocated to Docker
- Internet access (producer connects to `get.awesomedata.stream:9093`)

---

## Quick Start

### 1. Clone the repository

```bash
git clone <your-repo-url>
cd movielens-spark-streaming
```

### 2. Choose which phase to run

Open `docker-compose.yml` and find the last line of the `spark` service. Change the filename to the phase you want:

```yaml
# Change this line to switch phases:
command: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /app/phase4_windows.py
```

| Phase | File | What it does |
|-------|------|--------------|
| 1 | `phase1_kafka_test.py` | Plain Kafka consumer — confirms stream is live |
| 2 | `phase2_spark_console.py` | Spark parses JSON and prints raw records |
| 3 | `phase3_aggregations.py` | Running averages per movie and genre |
| 4 | `phase4_windows.py` | Sliding window trend detection |

### 3. Start the pipeline

```bash
docker compose up -d
```

### 4. Watch the output

```bash
docker compose logs -f spark
```

### 5. Stop the pipeline

```bash
docker compose down
```

---

## Phase Details

### Phase 1 — Kafka Connection Test (`phase1_kafka_test.py`)

Confirms the local Kafka broker is receiving ratings from the MovieLens stream. Prints 20 raw messages then exits.

Expected output:
```
✅ Connected to Kafka on attempt 1
  [  1] User  414 rated 'Bad Boys (1995)'  →  2.0 ⭐  | genres: ['Action', 'Comedy']
  [  2] User  489 rated 'Descendants, The (2011)'  →  3.5 ⭐  | genres: ['Comedy', 'Drama']
✅ Phase 1 PASSED — Kafka stream is working perfectly.
```

### Phase 2 — Spark JSON Parsing (`phase2_spark_console.py`)

Spark reads from Kafka and decodes each JSON message into structured columns: `userId`, `movieId`, `title`, `genres`, `rating`, `timestamp`. Prints one record per micro-batch to console.

### Phase 3 — Stateful Aggregations (`phase3_aggregations.py`)

Two running aggregations updated on every micro-batch:

- **Movie stats** — running average rating and count per movie (shown only for movies with 3+ ratings)
- **Genre stats** — rating volume and average score per genre across all time

Output mode: `complete` (full table reprinted each batch).

### Phase 4 — Sliding Window Trend Detection (`phase4_windows.py`)

Detects which genres have the highest activity in recent time windows using:

- Window size: 10 minutes
- Slide interval: 2 minutes  
- Watermark: 5 minutes (tolerates late-arriving data)
- Event time: processing time (`current_timestamp`) — used because the source timestamps are historical

Output mode: `update` (only changed windows are printed per batch).

Example output:
```
+------------------------------------------+---------+----------+------------+
|window                                    |genre    |avg_rating|rating_count|
+------------------------------------------+---------+----------+------------+
|{2026-05-11 15:48:00, 2026-05-11 15:58:00}|Drama    |4.25      |12          |
|{2026-05-11 15:48:00, 2026-05-11 15:58:00}|Comedy   |3.6       |8           |
|{2026-05-11 15:48:00, 2026-05-11 15:58:00}|Sci-Fi   |3.429     |7           |
+------------------------------------------+---------+----------+------------+
```

---

## Configuration

### Switching phases

Edit the `command` line in `docker-compose.yml` under the `spark` service. After changing, restart:

```bash
docker compose down
docker compose up -d
```

If switching to Phase 4 from another phase, clear the checkpoint first:

```bash
# Windows (PowerShell)
Remove-Item -Recurse -Force checkpoints\windows

# Mac/Linux
rm -rf checkpoints/windows
```

### Kafka topic

The producer connects to `get.awesomedata.stream:9093` on topic `ratings` and republishes messages to the local broker on `kafka:9092`, topic `ratings`.

### Spark configuration

| Setting | Value |
|---------|-------|
| Spark version | 3.5.1 |
| Kafka package | `spark-sql-kafka-0-10_2.12:3.5.1` |
| Shuffle partitions | 4 |
| Log level | WARN |

---

## Troubleshooting

**Containers exit immediately**  
Check logs: `docker compose logs spark`  
Most common cause: Kafka not ready yet. Wait 30 seconds and retry.

**Empty batches in Phase 3 (movie table)**  
The movie table requires 3+ ratings for the same movie. This takes a few minutes to accumulate — normal behaviour.

**Empty batches in Phase 4**  
If using event-time timestamps from the source data, Spark's watermark drops them as late. The fix is already applied in the current `phase4_windows.py` — it uses `current_timestamp()` instead.

**`failOnDataLoss` error**  
Kafka aged out messages between restarts. Already handled in Phase 4 with `.option("failOnDataLoss", "false")`.

**`grep` not found on Windows**  
Use PowerShell equivalent: `docker compose logs spark | Select-String "ERROR"`

---

## Dependencies

Listed in `spark/requirements.txt`. Key packages:

- `pyspark==3.5.1`
- `kafka-python`

The Spark Kafka connector is loaded at runtime via `--packages` in the `spark-submit` command — no manual JAR download needed.

---

## Notes on Streaming vs Batch

A one-shot batch analysis on the same data would compute identical aggregate statistics, but could not:

- Detect genre trends within short time windows as they emerge
- React to sudden spikes in rating volume for a specific genre
- Maintain continuously updated running averages without reprocessing history

The windowed aggregations in Phase 4 are the clearest demonstration of what streaming adds over batch: the ability to ask "what is trending *right now*" rather than "what was popular overall".