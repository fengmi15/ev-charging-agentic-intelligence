"""
EV Charging Network — Stream Agent

Consumes ev.telemetry.raw, maintains a rolling 10-minute window of
readings per station, computes window-level statistics, and publishes
enriched records to ev.telemetry.windowed.

This is the first processing layer — it doesn't make decisions, it just
enriches raw records with enough context (rolling stats, rate-of-change,
window completeness) for the anomaly_agent to work from.

Key design: per-station state is kept in memory (dict keyed by station_id).
In production this would be Kafka Streams or Flink state — here we keep it
simple and in-process since we're single-node.
"""
import json
import time
from collections import deque
from confluent_kafka import Consumer, Producer, KafkaError

KAFKA_BROKER   = "localhost:9092"
INPUT_TOPIC    = "ev.telemetry.raw"
OUTPUT_TOPIC   = "ev.telemetry.windowed"
CONSUMER_GROUP = "stream-agent-group"
WINDOW_SIZE    = 10   # minutes (10 readings at 1/min)


def create_producer():
    return Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "linger.ms": 10,
        "compression.type": "lz4",
    })


def create_consumer():
    c = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    c.subscribe([INPUT_TOPIC])
    return c


class StationWindow:
    """
    Maintains a fixed-size deque of recent readings for one station.
    Computes rolling statistics on demand.
    """
    def __init__(self, station_id: str, window_size: int = WINDOW_SIZE):
        self.station_id   = station_id
        self.window_size  = window_size
        self.readings     = deque(maxlen=window_size)
        self.last_seen_ts = None

    def add(self, record: dict):
        self.readings.append(record)
        self.last_seen_ts = record.get("timestamp_s")

    def compute_stats(self) -> dict:
        if len(self.readings) < 2:
            return {}

        temps   = [r["connector_temp_c"] for r in self.readings
                   if r.get("connector_temp_c") is not None]
        powers  = [r["power_kw"] for r in self.readings
                   if r.get("power_kw") is not None]
        socs    = [r["soc_pct"] for r in self.readings
                   if r.get("soc_pct") is not None]
        grid    = [r["grid_load_pct"] for r in self.readings
                   if r.get("grid_load_pct") is not None]

        stats = {}

        if len(temps) >= 2:
            stats["temp_mean"]        = round(sum(temps) / len(temps), 2)
            stats["temp_max"]         = round(max(temps), 2)
            # Rate of change per minute (C/min)
            stats["temp_rate_c_min"]  = round(temps[-1] - temps[0], 3)

        if len(powers) >= 2:
            stats["power_mean_kw"]    = round(sum(powers) / len(powers), 3)
            stats["power_variance"]   = round(
                sum((p - stats["power_mean_kw"])**2 for p in powers) / len(powers), 4
            )

        if len(socs) >= 2:
            stats["soc_delta"]        = round(socs[-1] - socs[0], 3)
            stats["soc_current"]      = round(socs[-1], 2)

        if len(grid) >= 2:
            stats["grid_load_mean"]   = round(sum(grid) / len(grid), 2)
            stats["grid_load_max"]    = round(max(grid), 2)

        stats["window_size"]          = len(self.readings)
        stats["charging_count"]       = sum(
            1 for r in self.readings if r.get("status") == "CHARGING"
        )

        return stats


def run():
    consumer  = create_consumer()
    producer  = create_producer()
    windows   = {}   # station_id -> StationWindow
    processed = 0
    published = 0
    last_log  = time.time()

    print(f"Stream agent started.")
    print(f"  Input:  {INPUT_TOPIC}")
    print(f"  Output: {OUTPUT_TOPIC}")
    print(f"  Window: {WINDOW_SIZE} minutes per station\n")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[ERROR] {msg.error()}")
                continue

            try:
                record = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                print(f"[PARSE ERROR] {e}")
                continue

            station_id = record.get("station_id")
            if not station_id:
                continue

            # Update per-station window
            if station_id not in windows:
                windows[station_id] = StationWindow(station_id)
            win = windows[station_id]
            win.add(record)

            # Enrich record with window stats
            stats = win.compute_stats()
            enriched = {**record, **stats}

            # Publish to windowed topic
            producer.produce(
                OUTPUT_TOPIC,
                key=station_id.encode("utf-8"),
                value=json.dumps(enriched).encode("utf-8"),
            )
            producer.poll(0)
            processed += 1
            published += 1

            # Log every 30 real seconds
            if time.time() - last_log >= 30:
                print(
                    f"[stream_agent] processed={processed:,} | "
                    f"stations tracked={len(windows)} | "
                    f"published={published:,}"
                )
                last_log = time.time()

    except KeyboardInterrupt:
        print(f"\nStream agent stopped. Processed {processed:,} records.")
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    run()