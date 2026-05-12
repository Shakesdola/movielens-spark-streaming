# MovieLens Spark Streaming — Local Setup Guide

## Folder structure

After setup your project looks like this:

```
movielens-streaming/
├── docker-compose.yml
├── checkpoints/            ← created manually, Spark writes state here
└── spark/
    ├── Dockerfile
    ├── requirements.txt
    ├── producer.py         ← generates fake ratings → Kafka
    ├── phase1_kafka_test.py
    ├── phase2_spark_console.py
    ├── phase3_aggregations.py
    └── phase4_windows.py
```

---

## Step 1 — Create the folders (PowerShell)

```powershell
mkdir movielens-streaming
cd movielens-streaming
mkdir spark
mkdir checkpoints
```

Then copy all files into the folders as shown above.

---

## Step 2 — Build Docker images (first time only, ~5 min)

```powershell
docker compose build
```

---

## Step 3 — Run Phase 1 (plain Kafka consumer test)

The default command in docker-compose.yml is already set to phase1.

```powershell
docker compose up
```

**What you should see:**

```
movielens_producer  | ✅ Connected to Kafka on attempt 1
movielens_producer  |   [1] Sent → User  342  4.0⭐  'Matrix, The (1999)'
movielens_producer  |   [2] Sent → User  342  5.0⭐  'Dark Knight, The (2008)'

movielens_spark     | ✅ Connected to Kafka on attempt 1
movielens_spark     |   [ 1] User  342 rated 'Matrix, The (1999)'  →  4.0 ⭐
movielens_spark     |   [ 2] User  342 rated 'Dark Knight, The (2008)'  →  5.0 ⭐
...
movielens_spark     | ✅ Phase 1 PASSED — Kafka stream is working perfectly.
```

Stop with Ctrl+C.

---

## Step 4 — Switch to Phase 2 (Spark reads the stream)

Open docker-compose.yml and change the last line of the spark service:

```yaml
    # Change this:
    command: python /app/phase1_kafka_test.py

    # To this:
    command: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /app/phase2_spark_console.py
```

Then:

```powershell
docker compose up
```

NOTE: The first time spark-submit runs, it downloads the Kafka JAR (~50MB).
This takes 1-2 minutes. After that it is cached inside the container.

---

## Step 5 — Switch to Phase 3 (running aggregations)

Wipe checkpoints first (important!):

```powershell
# PowerShell
Remove-Item -Recurse -Force checkpoints\*
```

Change docker-compose.yml command to:

```yaml
    command: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /app/phase3_aggregations.py
```

```powershell
docker compose up
```

---

## Step 6 — Switch to Phase 4 (sliding windows)

```powershell
Remove-Item -Recurse -Force checkpoints\*
```

Change command to:

```yaml
    command: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /app/phase4_windows.py
```

```powershell
docker compose up
```

---

## Useful commands

```powershell
# Stop everything
docker compose down

# Rebuild after changing requirements.txt or Dockerfile
docker compose build --no-cache

# See logs of one container only
docker compose logs -f spark
docker compose logs -f producer

# Wipe checkpoints between phases
Remove-Item -Recurse -Force checkpoints\*
```

---

## Troubleshooting

**"Kafka not ready" keeps looping** — Kafka takes ~20 seconds to start.
The producer and consumer both retry automatically. Just wait.

**Spark produces no output** — It uses startingOffsets=latest,
so it only sees messages arriving after Spark starts. Wait 30 seconds.

**"No space left on device" inside Docker** — Go to Docker Desktop →
Settings → Resources → Disk image size and increase it.

**Out of memory** — Go to Docker Desktop → Settings → Resources →
Memory and set it to at least 4GB.

**Checkpoint error when switching phases** — Always run
Remove-Item -Recurse -Force checkpoints\* before switching phases.
