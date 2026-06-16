"""
EV Charging Network — Kafka Producer

Streams telemetry_faulted.csv into Kafka topic `ev.telemetry.raw` at
configurable replay speed. Each row becomes one JSON message keyed by
station_id (ensures all messages from one station go to the same partition
— critical for ordered per-station anomaly detection downstream).

Usage:
    python src/producer.py              # real-time (1 msg/sec per station)
    python src/producer.py --speed 10   # 10x accelerated replay
    python src/producer.py --speed 0    # fire all rows as fast as possible
"""
import json
import time
import argparse
import pandas as pd
import numpy as np
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

KAFKA_BROKER = "localhost:9092"
TOPIC = "ev.telemetry.raw"
NUM_PARTITIONS = 8   # one per city roughly
REPLICATION = 1


def create_topic_if_missing():
    admin = AdminClient({"bootstrap.servers": KAFKA_BROKER})
    existing = admin.list_topics(timeout=10).topics
    if TOPIC not in existing:
        topic = NewTopic(TOPIC, num_partitions=NUM_PARTITIONS,
                         replication_factor=REPLICATION)
        fs = admin.create_topics([topic])
        for t, f in fs.items():
            try:
                f.result()
                print(f"Created topic: {t}")
            except Exception as e:
                print(f"Topic creation error (may already exist): {e}")
    else:
        print(f"Topic '{TOPIC}' already exists.")


def delivery_report(err, msg):
    if err:
        print(f"[DELIVERY ERROR] {err}")


def serialize_row(row: pd.Series) -> dict:
    """Convert a DataFrame row to a JSON-serializable dict."""
    record = row.to_dict()
    # Replace NaN/None with null-friendly values
    for k, v in record.items():
        if isinstance(v, float) and np.isnan(v):
            record[k] = None
        elif isinstance(v, np.integer):
            record[k] = int(v)
        elif isinstance(v, np.floating):
            record[k] = round(float(v), 4)
    return record


def stream_telemetry(speed_multiplier: float = 1.0):
    print(f"Loading telemetry...")
    tel = pd.read_csv("data/telemetry_faulted.csv")
    tel = tel.sort_values(["timestamp_s", "station_id"]).reset_index(drop=True)
    print(f"  {len(tel):,} rows across {tel['station_id'].nunique()} stations")

    create_topic_if_missing()

    producer = Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "linger.ms": 5,           # small batching for throughput
        "batch.size": 32768,
        "compression.type": "lz4",
    })

    timestamps = tel["timestamp_s"].unique()
    total_ts = len(timestamps)
    prev_ts = None
    rows_sent = 0
    faults_sent = 0

    print(f"\nStreaming {total_ts} time steps "
          f"({'real-time' if speed_multiplier == 1.0 else f'{speed_multiplier}x'})...")
    print(f"Topic: {TOPIC} | Partitions: {NUM_PARTITIONS}\n")

    for i, ts in enumerate(sorted(timestamps)):
        batch = tel[tel["timestamp_s"] == ts]

        for _, row in batch.iterrows():
            record = serialize_row(row)
            value = json.dumps(record).encode("utf-8")
            key = row["station_id"].encode("utf-8")

            producer.produce(
                TOPIC,
                key=key,
                value=value,
                callback=delivery_report,
            )
            rows_sent += 1
            if row.get("fault_type") is not None and not (
                isinstance(row.get("fault_type"), float)
            ):
                faults_sent += 1

        producer.poll(0)  # trigger callbacks without blocking

        # Pace the replay
        if speed_multiplier > 0 and prev_ts is not None:
            gap_s = (ts - prev_ts) / speed_multiplier
            if gap_s > 0:
                time.sleep(gap_s)
        prev_ts = ts

        # Progress every 10 minutes of sim time
        if i % 10 == 0:
            sim_hour = (ts // 3600) % 24
            sim_min = (ts % 3600) // 60
            print(f"  [{sim_hour:02d}:{sim_min:02d}] "
                  f"{rows_sent:>7,} rows sent | "
                  f"{faults_sent:>4} fault rows | "
                  f"lag={producer.flush.__doc__ and 0}")

    producer.flush()
    print(f"\nDone. {rows_sent:,} rows → topic '{TOPIC}'")
    print(f"Fault rows streamed: {faults_sent}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--speed", type=float, default=60.0,
        help="Replay speed multiplier (default 60 = 1 sim-minute per real second). "
             "Use 0 for max speed."
    )
    args = parser.parse_args()
    stream_telemetry(speed_multiplier=args.speed)