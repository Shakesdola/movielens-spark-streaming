"""
producer.py
Reads real MovieLens ratings.csv and movies.csv and publishes
each rating as a JSON message to the local Kafka topic 'ratings'.
The JSON format is identical to the Awesome Data Stream format.
"""
import csv
import json
import time
import random
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

TOPIC         = "ratings"
BOOTSTRAP     = "kafka:9092"
DELAY_SECONDS = 0.5   # one rating every 0.5 seconds — lower = faster

# Paths inside the container (mounted from your host)
RATINGS_FILE = "/app/ml-latest-small/ratings.csv"
MOVIES_FILE  = "/app/ml-latest-small/movies.csv"


def wait_for_kafka(retries=20, delay=5):
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[BOOTSTRAP],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"✅ Connected to Kafka on attempt {attempt}")
            return producer
        except NoBrokersAvailable:
            print(f"  Kafka not ready (attempt {attempt}/{retries}), retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after multiple retries.")


def load_movies(path: str) -> dict:
    """
    Returns a dict: movieId (str) -> {"title": str, "genres": [str]}
    movies.csv columns: movieId, title, genres  (genres pipe-separated)
    """
    movies = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            genres = row["genres"].split("|") if row["genres"] != "(no genres listed)" else []
            movies[row["movieId"]] = {
                "title":  row["title"],
                "genres": genres,
            }
    print(f"  Loaded {len(movies):,} movies from {path}")
    return movies


def load_ratings(path: str) -> list:
    """
    Returns a list of dicts: userId, movieId, rating, timestamp
    ratings.csv columns: userId, movieId, rating, timestamp
    """
    ratings = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ratings.append(row)
    print(f"  Loaded {len(ratings):,} ratings from {path}")
    return ratings


def main():
    print("=== MovieLens Producer (real dataset) starting... ===")

    # Check files exist and give a clear error if not
    for path in [RATINGS_FILE, MOVIES_FILE]:
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"\n File not found: {path}\n"
                f"Make sure your MovieLens folder is named 'ml-latest-small'\n"
                f"and is inside the 'spark/' folder of your project.\n"
                f"Expected structure:\n"
                f"  spark/\n"
                f"    ml-latest-small/\n"
                f"      ratings.csv\n"
                f"      movies.csv\n"
            )

    movies  = load_movies(MOVIES_FILE)
    ratings = load_ratings(RATINGS_FILE)

    # Shuffle so we don't stream in timestamp order (simulates live arrivals)
    random.seed(42)
    random.shuffle(ratings)
    print(f"  Shuffled ratings — streaming will begin now.\n")
    # OR
    # Sort by timestamp ascending — replays history in chronological order
    # ratings.sort(key=lambda r: int(r["timestamp"]))

    producer = wait_for_kafka()

    count = 0
    try:
        while True:
            # Loop back to the start when all ratings have been sent
            for row in ratings:
                movie_id = row["movieId"]

                # Skip ratings whose movieId has no matching movie metadata
                if movie_id not in movies:
                    continue

                movie = movies[movie_id]
                msg = {
                    "userId": row["userId"],
                    "movie": {
                        "movieId": movie_id,
                        "title":   movie["title"],
                        "genres":  movie["genres"],
                    },
                    "rating":    row["rating"],
                    "timestamp": row["timestamp"],
                }

                producer.send(TOPIC, value=msg)

                count += 1
                print(f"  [{count:>5}] User {row['userId']:>6}  "
                      f"{row['rating']}⭐  '{movie['title']}'")

                time.sleep(DELAY_SECONDS)

            print("\n🔁 All ratings sent — looping back to start...\n")

    except KeyboardInterrupt:
        print("\nProducer stopped.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()