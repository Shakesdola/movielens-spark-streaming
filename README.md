# Real-time Analysis of Movie Ratings with Apache Spark Structured Streaming

**Team:** Daniela Shahini, Jiaxin Li  
**Course:** Distributed and Scalable Systems  
**Dataset:** MovieLens ratings stream via Apache Kafka

---

## Overview

This project implements an end-to-end streaming analytics pipeline that processes movie ratings incrementally using Apache Spark Structured Streaming. The original data source was the Awesome Data Stream server at get.awesomedata.stream:9093, which provides the MovieLens dataset as a live Kafka stream. As the server was unavailable during development, we downloaded the MovieLens ml-latest-small dataset directly and reconstructed the stream by replaying ratings in timestamp order through a custom Kafka producer. Each rating is emitted as an individual JSON message, replicating the format and behavior of the original live stream.

The processing is genuinely incremental: Spark processes each rating as it arrives without access to the full history. The live stream already includes embedded movie metadata (title and genres) in every message, making a separate join with static files unnecessary.

The pipeline was validated in two deployment modes: a local Docker Compose setup and a distributed AWS EC2 cluster with four separate nodes.

---

## Architecture

### Local Deployment

```
MovieLens Server (get.awesomedata.stream:9093)
        |
        v
Producer (producer.py)       pulls live ratings, pushes to Kafka
        |
        v  topic: ratings
Kafka Broker                 Apache Kafka 3.7.0, KRaft mode, no Zookeeper
        |
        v
Spark Job (phase*.py)        PySpark Structured Streaming 3.5.1
                             stateful aggregations, sliding windows
```

For local development, all components run in Docker containers via Docker Compose. For production deployment, the pipeline runs across four separate AWS EC2 instances in a distributed configuration.

### AWS Distributed Deployment

```
MovieLens Server (get.awesomedata.stream:9093)
        |
        v
EC2 #4  Producer Node        54.91.224.221
        producer.py running standalone, feeds Kafka over port 9094
        |
        v
EC2 #1  Kafka Node           184.73.143.173
        Kafka broker only, buffers the live ratings stream
        |
        v
EC2 #2  Spark Master         54.166.149.1
        spark://172.31.32.124:7077, coordinates job execution
        |
        v
EC2 #3  Spark Worker         54.224.25.237
        172.31.37.165:39997, executes stream processing tasks
```

All EC2 instances run Ubuntu 24.04 LTS. The producer and broker are intentionally separated onto different machines to allow independent scaling of ingestion and message buffering. The Spark cluster runs in standalone mode.

---

## Repository Structure

```
movielens-spark-streaming/
    docker-compose.yml              defines kafka, producer, spark services
    checkpoints/                    Spark streaming checkpoints, auto-created
        movies/
        genres/
        windows/
    output/                         Parquet output files, auto-created by Phase 5
        raw_ratings/
        windowed_trends/
    spark/
        Dockerfile                  image for producer and spark containers
        requirements.txt            Python dependencies
        producer.py                 connects to MovieLens server, feeds Kafka
        phase1_kafka_test.py        plain Kafka consumer, confirms stream works
        phase2_spark_console.py     Spark reads and parses the JSON stream
        phase3_aggregations.py      running averages per movie and genre
        phase4_windows.py           sliding window trend detection
        phase5_parquet_sink.py      saves raw ratings and windowed trends to Parquet
        ml-latest-small/            static MovieLens metadata
    analysis_charts.py              reads Parquet output and generates charts
    batch_comparison.py             compares streaming vs batch aggregations
    batch_als.py                    trains Spark ALS and compares with streaming averages
    dashboard.py                    live Plotly Dash dashboard, updates every 5 seconds
```

---

## Prerequisites

Docker Desktop on Windows or Mac, or Docker Engine on Linux.For the analysis scripts, Python 3.x with pandas, pyarrow, plotly, kaleido, dash, and kafka-python installed locally.

---

## Quick Start (Local)

Clone the repository and enter the folder:

```bash
git clone <your-repo-url>
cd movielens-spark-streaming
```

Create the required folders:

```powershell
mkdir checkpoints
mkdir output
```

Choose which phase to run by editing the last line of the spark service in docker-compose.yml:

```yaml
command: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /app/phase4_windows.py
```

Start the pipeline:

```bash
docker compose up -d
```

Watch the output:

```bash
docker compose logs -f spark
```

Stop the pipeline:

```bash
docker compose down
```

---

## Phases

### Phase 1 — Kafka Connection Test

File: phase1_kafka_test.py

Confirms the local Kafka broker is receiving ratings. Connects directly to Kafka using kafka-python, prints 20 raw messages, and exits. No Spark involved.

To run, set the docker-compose.yml command to:

```yaml
command: python /app/phase1_kafka_test.py
```

Expected output:

```
Connected to Kafka on attempt 1
  [  1] User  414 rated 'Bad Boys (1995)'  2.0 stars  genres: ['Action', 'Comedy', 'Crime']
  [  2] User  489 rated 'Descendants, The (2011)'  3.5 stars  genres: ['Comedy', 'Drama']
Phase 1 PASSED, Kafka stream is working perfectly.
```

### Phase 2 — Spark JSON Parsing

File: phase2_spark_console.py

Spark connects to Kafka and decodes each JSON message into structured columns: userId, movieId, title, genres, rating, timestamp. Prints one record per micro-batch to the console. Batch numbers increment continuously, confirming the pipeline processes a live stream rather than a static file.

```yaml
command: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /app/phase2_spark_console.py
```

### Phase 3 — Stateful Aggregations

File: phase3_aggregations.py

Two running aggregations updated on every micro-batch without reprocessing history:

Movie stats: running average rating and count per movie, shown only when a movie has 3 or more ratings.

Genre stats: rating volume and average score per genre across all time.

Output mode is complete, meaning the full table is reprinted on each batch.

```yaml
command: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /app/phase3_aggregations.py
```

Clear checkpoints before switching to this phase:

```powershell
Remove-Item -Recurse -Force checkpoints\*
```

### Phase 4 — Sliding Window Trend Detection

File: phase4_windows.py

Detects which genres have the highest activity in recent time windows.

Window size: 10 minutes. Slide interval: 2 minutes. Watermark: 30 seconds for late data tolerance. Event time uses current_timestamp because the source timestamps are historical replays.

Output mode is update, meaning only changed windows are printed per batch.

```yaml
command: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /app/phase4_windows.py
```

Example output:

```
Batch: 1
+------------------------------------------+---------+----------+------------+
|window                                    |genre    |avg_rating|rating_count|
+------------------------------------------+---------+----------+------------+
|{2026-05-11 15:48:00, 2026-05-11 15:58:00}|Drama    |4.25      |12          |
|{2026-05-11 15:48:00, 2026-05-11 15:58:00}|Comedy   |3.6       |8           |
|{2026-05-11 15:48:00, 2026-05-11 15:58:00}|Sci-Fi   |3.429     |7           |
+------------------------------------------+---------+----------+------------+
```

### Phase 5 — Parquet Output

File: phase5_parquet_sink.py

Writes raw ratings to output/raw_ratings/ and windowed genre trends to output/windowed_trends/ as Snappy-compressed Parquet files. Writes every 30 seconds and stops automatically after 5 minutes.

```yaml
command: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /app/phase5_parquet_sink.py
```

---

## Analysis Scripts

After running Phase 5, run these locally to generate charts and comparisons.

Install dependencies:

```powershell
pip install pandas pyarrow plotly kaleido dash kafka-python pyspark
```

Generate charts from Parquet output:

```powershell
python analysis_charts.py
```

This produces 5 charts in output/charts/ as both PNG and interactive HTML: rating distribution, average rating by genre, rating volume by genre, event rate over time, and windowed genre trends.

Compare streaming vs batch:

```powershell
python batch_comparison.py
```

This runs the same aggregations in batch mode on the collected Parquet data and confirms the final results are identical to the streaming output, while demonstrating what streaming uniquely adds.

Train ALS and compare with streaming averages:

```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jdk-17"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
python batch_als.py
```

Run the live dashboard:

```powershell
python dashboard.py
```

Open http://localhost:8050 in your browser. The dashboard connects directly to Kafka on localhost:9094 and updates every 5 seconds, showing genre volumes, rating distribution, event rate, and top rated movies.

---

## AWS Distributed Deployment

### Setup

Launch 4 EC2 instances on Ubuntu 24.04 LTS. Install Docker on all 4:

```bash
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker ubuntu
sudo apt-get install -y docker-compose
```

### EC2 #1 — Kafka Node

Copy the project and start Kafka only:

```bash
cd ~/movielens-spark-streaming
docker-compose up -d kafka
```

The docker-compose.yml must expose port 9094 externally with a separate EXTERNAL listener so other EC2 instances can connect:

```yaml
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,EXTERNAL://0.0.0.0:9094,CONTROLLER://0.0.0.0:9093
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,EXTERNAL://<kafka-private-ip>:9094
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
```

### EC2 #2 — Spark Master

```bash
sudo docker run -d \
  --name spark-master \
  --network host \
  apache/spark:3.5.1 \
  /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
  --host <master-private-ip> \
  --port 7077 \
  --webui-port 8080
```

### EC2 #3 — Spark Worker

```bash
sudo docker run -d \
  --name spark-worker \
  --network host \
  apache/spark:3.5.1 \
  /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
  --host <worker-private-ip> \
  spark://<master-private-ip>:7077
```

### EC2 #4 — Producer Node

Copy producer.py and the ml-latest-small folder. Update the BOOTSTRAP address to the Kafka private IP on port 9094, then run:

```bash
pip3 install kafka-python --break-system-packages
python3 producer.py
```

### Submit the Spark Job

From the Master EC2:

```bash
sudo docker run --rm \
  --network host \
  --user root \
  -v ~/phase4_windows.py:/phase4_windows.py \
  apache/spark:3.5.1 \
  /opt/spark/bin/spark-submit \
  --master spark://<master-private-ip>:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /phase4_windows.py
```

Monitor the cluster at http://<master-public-ip>:8080. The UI shows connected workers, running applications, cores and memory in use.

### Security Group Rules

Open these inbound ports on your EC2 security group:

```
22     SSH
9092   Kafka internal
9094   Kafka external
7077   Spark Master
8080   Spark Master UI
8081   Spark Worker UI
4040   Spark Application UI
```

Also add an All Traffic rule sourced from the security group itself so all EC2 instances in the group can communicate freely.

---

## Spark Configuration

| Setting | Value |
|---------|-------|
| Spark version | 3.5.1 |
| Kafka connector | spark-sql-kafka-0-10_2.12:3.5.1 |
| Shuffle partitions | 4 |
| Log level | WARN |
| Checkpoint location | /checkpoints/ |
| Output location | /output/ |

---

## Troubleshooting

**Containers exit immediately.** Check logs with docker compose logs spark. Most common cause is Kafka not ready yet. Wait 30 seconds and retry.

**Empty batches in Phase 3 movie table.** The movie table requires 3 or more ratings for the same movie. This takes a few minutes to accumulate and is normal behaviour.

**Empty batches in Phase 4.** The current phase4_windows.py uses current_timestamp instead of event-time from the source, so windows fill immediately.

**failOnDataLoss error.** Kafka aged out messages between restarts. Already handled in Phase 4 and Phase 5 with .option("failOnDataLoss", "false").

**Checkpoint conflict error when restarting.** Always clear checkpoints before restarting a phase: Remove-Item -Recurse -Force checkpoints\* on Windows or rm -rf checkpoints/* on Linux.

**grep not found on Windows.** Use the PowerShell equivalent: docker compose logs spark | Select-String "ERROR"

**Java not found when running batch_als.py.** Set JAVA_HOME before running: $env:JAVA_HOME = "C:\Program Files\Java\jdk-17" and $env:PATH = "$env:JAVA_HOME\bin;$env:PATH"

**Kafka not reachable from AWS Master.** Make sure the Kafka EC2 exposes port 9094 with an EXTERNAL listener and the phase4_windows.py bootstrap address points to the Kafka private IP on port 9094.

---

## Streaming vs Batch

A one-shot batch analysis on the same data produces identical final aggregate statistics, but cannot detect genre trends within short time windows as they emerge, react to sudden spikes in rating volume, or maintain continuously updated running averages without reprocessing history. The batch comparison script confirms the final numbers converge exactly, while the windowed aggregations in Phase 4 demonstrate what streaming uniquely adds: the ability to ask what is trending right now rather than what was popular overall.

ALS matrix factorization was evaluated on the collected Parquet data. With 573 ratings the RMSE was 3.93, dropping to 2.83 with 3,400 ratings, compared to the published MovieLens benchmark of approximately 0.87 on 100,000 ratings. This confirms that streaming windows do not accumulate sufficient data density for meaningful matrix factorization, and that incremental aggregations are the appropriate tool for real-time genre-level analytics.