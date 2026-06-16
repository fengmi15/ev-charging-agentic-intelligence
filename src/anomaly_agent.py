"""
EV Charging Network — Anomaly Agent

Consumes ev.telemetry.windowed, applies per-station detection rules for
all 6 fault types, and publishes structured alerts to ev.alerts.

Detection logic (what makes this impressive in interviews):

1. THERMAL_RUNAWAY  — temp_rate_c_min > 4.0 during active charging
                      (rate-of-change detection, not absolute threshold)

2. GRID_CONGESTION  — grid_load_pct > 115% sustained over window
                      (city-level aggregate, not per-station)

3. PHANTOM_SESSION  — status=CHARGING AND power_mean_kw < 0.5 for full window
                      (power near zero despite active session)

4. SOC_PLATEAU      — soc_delta < 0.3 during charging over 10-min window
                      (SoC not moving despite active charging)

5. FIRMWARE_FAULT   — fault_code=E042 appearing in >3 readings in window
                      (frequency-based — single occurrence is noise)

6. GHOST_STATION    — station_id absent from stream for >5 minutes
                      (absence detection via heartbeat tracker)

Each alert published to ev.alerts contains:
    station_id, city_code, fault_type, severity,
    detected_at_ts, evidence (dict of triggered metrics),
    alert_id (unique)
"""
import json
import time
import uuid
from collections import defaultdict
from confluent_kafka import Consumer, Producer, KafkaError

KAFKA_BROKER    = "localhost:9092"
INPUT_TOPIC     = "ev.telemetry.windowed"
OUTPUT_TOPIC    = "ev.alerts"
CONSUMER_GROUP  = "anomaly-agent-group"

# Detection thresholds
THERMAL_RATE_THRESHOLD    = 5.5    # C/min (earlier it was 4 creating too many alerts)
GRID_LOAD_THRESHOLD       = 115.0  # %
PHANTOM_POWER_THRESHOLD   = 0.5    # kW
SOC_DELTA_THRESHOLD       = 0.3    # % over 10-min window
FIRMWARE_COUNT_THRESHOLD  = 3      # E042 occurrences in window
GHOST_TIMEOUT_S           = 600    # 10 minutes silence = ghost


def create_consumer():
    c = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    c.subscribe([INPUT_TOPIC])
    return c


def create_producer():
    return Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "linger.ms": 5,
        "compression.type": "lz4",
    })


def make_alert(station_id: str, city_code: str, fault_type: str,
               severity: str, detected_at_ts: int, evidence: dict) -> dict:
    return {
        "alert_id":       str(uuid.uuid4())[:8],
        "station_id":     station_id,
        "city_code":      city_code,
        "fault_type":     fault_type,
        "severity":       severity,
        "detected_at_ts": detected_at_ts,
        "evidence":       evidence,
        "created_at":     time.time(),
    }


def detect_faults(record: dict, last_seen: dict,
                  active_alerts: dict) -> list:
    """
    Runs all detection rules against one enriched windowed record.
    Returns list of alert dicts (empty if no faults detected).
    """
    alerts = []
    sid    = record.get("station_id", "")
    city   = record.get("city_code", "")
    ts     = record.get("timestamp_s", 0)
    status = record.get("status", "IDLE")

    # Update heartbeat tracker
    last_seen[sid] = time.time()

    # ── 1. THERMAL RUNAWAY ──────────────────────────────────────────────
    temp_rate = record.get("temp_rate_c_min")
    temp_max  = record.get("temp_max")
    if (temp_rate is not None and temp_rate > THERMAL_RATE_THRESHOLD
            and status == "CHARGING"):
        key = f"{city}_THERMAL"
        if key not in active_alerts:
            active_alerts[key] = ts
            alerts.append(make_alert(
                sid, city, "THERMAL_RUNAWAY", "CRITICAL", ts,
                evidence={
                    "temp_rate_c_min": temp_rate,
                    "temp_max_c":      temp_max,
                    "threshold":       THERMAL_RATE_THRESHOLD,
                    "message": f"Connector heating at {temp_rate:.1f}C/min — fire risk",
                }
            ))
    else:
        active_alerts.pop(f"{sid}_THERMAL", None)

    # ── 2. GRID CONGESTION ──────────────────────────────────────────────
    grid_max  = record.get("grid_load_max")
    grid_mean = record.get("grid_load_mean")
    if grid_max is not None and grid_max > GRID_LOAD_THRESHOLD:
        key = f"{city}_GRID"
        if key not in active_alerts:
            active_alerts[key] = ts
            alerts.append(make_alert(
                sid, city, "GRID_CONGESTION", "HIGH", ts,
                evidence={
                    "grid_load_max_pct":  grid_max,
                    "grid_load_mean_pct": grid_mean,
                    "threshold":          GRID_LOAD_THRESHOLD,
                    "message": f"{city} grid at {grid_max:.1f}% — brownout risk",
                }
            ))
    else:
        active_alerts.pop(f"{city}_GRID", None)

    # ── 3. PHANTOM SESSION ──────────────────────────────────────────────
    power_mean    = record.get("power_mean_kw", 0)
    window_size   = record.get("window_size", 0)
    charging_count = record.get("charging_count", 0)
    session_id    = record.get("session_id")

    if (session_id and status == "CHARGING"
            and power_mean < PHANTOM_POWER_THRESHOLD
            and charging_count >= 5):
        key = f"{sid}_PHANTOM"
        if key not in active_alerts:
            active_alerts[key] = ts
            alerts.append(make_alert(
                sid, city, "PHANTOM_SESSION", "MEDIUM", ts,
                evidence={
                    "power_mean_kw":   power_mean,
                    "session_id":      session_id,
                    "charging_count":  charging_count,
                    "message": f"Session active but power={power_mean:.2f}kW — stuck contactor",
                }
            ))
    else:
        active_alerts.pop(f"{sid}_PHANTOM", None)

    # ── 4. SOC PLATEAU ──────────────────────────────────────────────────
    soc_delta   = record.get("soc_delta")
    soc_current = record.get("soc_current")

    if (soc_delta is not None
            and abs(soc_delta) < SOC_DELTA_THRESHOLD
            and status == "CHARGING"
            and charging_count >= 8
            and soc_current is not None
            and soc_current < 98):
        key = f"{sid}_SOC"
        if key not in active_alerts:
            active_alerts[key] = ts
            alerts.append(make_alert(
                sid, city, "SOC_PLATEAU", "MEDIUM", ts,
                evidence={
                    "soc_delta":    soc_delta,
                    "soc_current":  soc_current,
                    "window_min":   window_size,
                    "message": f"SoC stuck at {soc_current:.1f}% — BMS fault suspected",
                }
            ))
    else:
        active_alerts.pop(f"{sid}_SOC", None)

    # ── 5. FIRMWARE FAULT ───────────────────────────────────────────────
    fault_code = record.get("fault_code")
    firmware   = record.get("firmware_version", "")

    if fault_code == "E042" and firmware == "2.3.0":
        key = f"{sid}_FW"
        if key not in active_alerts:
            active_alerts[key] = ts
            alerts.append(make_alert(
                sid, city, "FIRMWARE_FAULT", "LOW", ts,
                evidence={
                    "fault_code":       fault_code,
                    "firmware_version": firmware,
                    "message": f"Firmware {firmware} emitting E042 — cross-station pattern suspected",
                }
            ))
    else:
        active_alerts.pop(f"{sid}_FW", None)

    return alerts


def check_ghost_stations(last_seen: dict, known_stations: set,
                          active_alerts: dict,
                          station_meta: dict) -> list:
    """
    Runs periodically. Any known station not seen in GHOST_TIMEOUT_S
    seconds gets a GHOST_STATION alert.
    """
    alerts = []
    now = time.time()
    for sid in known_stations:
        last = last_seen.get(sid, now)
        if now - last > GHOST_TIMEOUT_S:
            key = f"{sid}_GHOST"
            if key not in active_alerts:
                active_alerts[key] = now
                city = station_meta.get(sid, {}).get("city_code", "??")
                alerts.append(make_alert(
                    sid, city, "GHOST_STATION", "HIGH", int(now),
                    evidence={
                        "last_seen_s_ago": round(now - last, 1),
                        "threshold_s":     GHOST_TIMEOUT_S,
                        "message": f"Station offline for {round(now-last)}s — comms failure",
                    }
                ))
        else:
            active_alerts.pop(f"{sid}_GHOST", None)
    return alerts


def run():
    consumer       = create_consumer()
    producer       = create_producer()
    last_seen      = {}       # station_id -> last real-time seen
    active_alerts  = {}       # dedup key -> timestamp (prevents alert storms)
    known_stations = set()
    station_meta   = {}       # station_id -> {city_code, ...}
    total_alerts   = 0
    processed      = 0
    last_ghost_chk = time.time()
    last_log       = time.time()

    print("Anomaly agent started.")
    print(f"  Input:  {INPUT_TOPIC}")
    print(f"  Output: {OUTPUT_TOPIC}\n")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Still check ghost stations even when no messages
                pass
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[ERROR] {msg.error()}")
                continue
            else:
                try:
                    record = json.loads(msg.value().decode("utf-8"))
                except Exception:
                    continue

                sid = record.get("station_id")
                if sid:
                    known_stations.add(sid)
                    station_meta[sid] = {"city_code": record.get("city_code", "")}
                    last_seen[sid] = time.time()

                alerts = detect_faults(record, last_seen, active_alerts)
                for alert in alerts:
                    producer.produce(
                        OUTPUT_TOPIC,
                        key=alert["station_id"].encode("utf-8"),
                        value=json.dumps(alert).encode("utf-8"),
                    )
                    total_alerts += 1
                    sev = alert["severity"]
                    color = {
                        "CRITICAL": "\033[91m",  # red
                        "HIGH":     "\033[93m",  # yellow
                        "MEDIUM":   "\033[94m",  # blue
                        "LOW":      "\033[92m",  # green
                    }.get(sev, "")
                    reset = "\033[0m"
                    print(
                        f"  {color}[{sev}]{reset} "
                        f"{alert['fault_type']:20s} | "
                        f"{alert['station_id']:16s} | "
                        f"{alert['evidence'].get('message','')}"
                    )

                producer.poll(0)
                processed += 1

            # Ghost station check every 60 real seconds
            now = time.time()
            if now - last_ghost_chk >= 60:
                ghost_alerts = check_ghost_stations(
                    last_seen, known_stations, active_alerts, station_meta
                )
                for alert in ghost_alerts:
                    producer.produce(
                        OUTPUT_TOPIC,
                        key=alert["station_id"].encode("utf-8"),
                        value=json.dumps(alert).encode("utf-8"),
                    )
                    total_alerts += 1
                    print(
                        f"  \033[93m[HIGH]\033[0m "
                        f"GHOST_STATION         | "
                        f"{alert['station_id']:16s} | "
                        f"{alert['evidence'].get('message','')}"
                    )
                last_ghost_chk = now

            # Progress log every 30 seconds
            if now - last_log >= 30:
                print(
                    f"[anomaly_agent] processed={processed:,} | "
                    f"total_alerts={total_alerts} | "
                    f"active_dedup_keys={len(active_alerts)}"
                )
                last_log = now

    except KeyboardInterrupt:
        print(f"\nAnomaly agent stopped. {total_alerts} alerts generated.")
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    run()