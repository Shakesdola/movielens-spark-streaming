"""
Phase 1: Plain Kafka consumer — no Spark yet.
Goal: confirm we can receive messages from the local Kafka broker.
"""
import json
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

TOPIC         = "ratings"
BOOTSTRAP     = "kafka:9092"
GROUP_ID      = "phase1-test-group"


def wait_for_kafka(retries=20, delay=5):
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=[BOOTSTRAP],
                group_id=GROUP_ID,
                auto_offset_reset="latest",
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            )
            print(f"✅ Connected to Kafka on attempt {attempt}")
            return consumer
        except NoBrokersAvailable:
            print(f"  Kafka not ready (attempt {attempt}/{retries}), retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka.")


def main():
    print("=== Phase 1: Listening to local MovieLens stream (Ctrl+C to stop) ===\n")
    consumer = wait_for_kafka()

    count = 0
    try:
        for message in consumer:
            data = message.value
            count += 1
            print(f"  [{count:>3}] User {data['userId']:>4} rated "
                  f"'{data['movie']['title']}'  →  {data['rating']} ⭐"
                  f"  | genres: {data['movie']['genres']}")

            if count >= 20:
                print("\n✅ Phase 1 PASSED — Kafka stream is working perfectly.")
                break
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
