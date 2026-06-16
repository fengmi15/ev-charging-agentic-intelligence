"""
EV Charging Network — Report Agent

Consumes ev.alerts, batches alerts by city and fault type over a
configurable window (default 2 minutes), then calls Claude API to
generate structured natural-language incident reports.

Reports are written to:
  - data/reports/  (JSON files, one per report)
  - ev.reports Kafka topic (for dashboard consumption)

This is the "agentic" layer — Claude acts as a fleet operations analyst,
synthesizing raw alert evidence into actionable maintenance recommendations.
"""
import json
import os
import time
from collections import defaultdict
from pathlib import Path
from dotenv import load_dotenv
from anthropic import Anthropic
from confluent_kafka import Consumer, Producer, KafkaError

load_dotenv()

KAFKA_BROKER    = os.getenv("KAFKA_BROKER", "localhost:9092")
INPUT_TOPIC     = "ev.alerts"
OUTPUT_TOPIC    = "ev.reports"
CONSUMER_GROUP  = "report-agent-group"
BATCH_WINDOW_S  = 120   # batch alerts for 2 minutes before generating report
MAX_ALERTS_BATCH = 20   # cap to avoid huge prompts

Path("data/reports").mkdir(parents=True, exist_ok=True)


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
        "linger.ms": 10,
    })


SEVERITY_PRIORITY = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}

SYSTEM_PROMPT = """You are an EV charging network operations analyst for a major German 
charging infrastructure operator. You receive batched alerts from 110 charging stations 
across 8 German cities (Berlin, Munich, Hamburg, Frankfurt, Cologne, Stuttgart, 
Düsseldorf, Leipzig).

Your job is to synthesize alert batches into concise, actionable incident reports 
for the operations team. Reports must be:
- Professional and direct (operations engineers read these under time pressure)
- Prioritized by severity (CRITICAL first)
- Cross-station pattern aware (firmware faults appearing at multiple stations in 
  same city are more significant than isolated incidents)
- Actionable (concrete next steps, not generic advice)

Output format (strict JSON):
{
  "report_id": "<same as input>",
  "summary": "<2-sentence executive summary>",
  "severity_level": "<CRITICAL|HIGH|MEDIUM|LOW — highest in batch>",
  "incidents": [
    {
      "fault_type": "<type>",
      "affected_stations": ["<id1>", "<id2>"],
      "city": "<city_code>",
      "description": "<1-2 sentences describing what's happening and likely cause>",
      "recommended_action": "<specific next step for ops team>",
      "urgency": "<Immediate|Within 1h|Within 4h|Monitor>"
    }
  ],
  "cross_station_patterns": "<any patterns spanning multiple stations/cities, or null>",
  "generated_at": "<timestamp>"
}"""


def build_prompt(alerts: list, report_id: str) -> str:
    # Sort by severity
    sorted_alerts = sorted(
        alerts,
        key=lambda a: SEVERITY_PRIORITY.get(a.get("severity", "LOW"), 3)
    )[:MAX_ALERTS_BATCH]

    alert_lines = []
    for a in sorted_alerts:
        ev = a.get("evidence", {})
        alert_lines.append(
            f"- [{a['severity']}] {a['fault_type']} | "
            f"Station: {a['station_id']} ({a['city_code']}) | "
            f"{ev.get('message', '')} | "
            f"Evidence: {json.dumps({k:v for k,v in ev.items() if k != 'message'})}"
        )

    cities_hit = list({a["city_code"] for a in sorted_alerts})
    fault_types = list({a["fault_type"] for a in sorted_alerts})

    return f"""Report ID: {report_id}
Time window: last {BATCH_WINDOW_S}s
Cities affected: {', '.join(cities_hit)}
Fault types detected: {', '.join(fault_types)}
Total alerts: {len(alerts)} ({len(sorted_alerts)} shown)

Alert details:
{chr(10).join(alert_lines)}

Generate the incident report JSON now."""


def generate_report(client: Anthropic, alerts: list, report_id: str) -> dict | None:
    if not alerts:
        return None

    prompt = build_prompt(alerts, report_id)

    try:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1000,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()

        # Strip markdown fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        report = json.loads(raw)
        report["report_id"] = report_id
        report["alert_count"] = len(alerts)
        report["generated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        return report

    except Exception as e:
        print(f"[report_agent] Claude API error: {e}")
        return None


def run():
    client   = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    consumer = create_consumer()
    producer = create_producer()

    alert_buffer = []
    batch_start  = time.time()
    report_count = 0

    print("Report agent started.")
    print(f"  Input:   {INPUT_TOPIC}")
    print(f"  Output:  {OUTPUT_TOPIC}")
    print(f"  Batch window: {BATCH_WINDOW_S}s | Max alerts/report: {MAX_ALERTS_BATCH}\n")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is not None and not msg.error():
                try:
                    alert = json.loads(msg.value().decode("utf-8"))
                    alert_buffer.append(alert)
                except Exception:
                    pass
            elif msg is not None and msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[ERROR] {msg.error()}")

            # Generate report when batch window elapses and buffer non-empty
            elapsed = time.time() - batch_start
            if elapsed >= BATCH_WINDOW_S and alert_buffer:
                report_id = f"RPT-{int(time.time())}"
                print(f"\n[report_agent] Generating report {report_id} "
                      f"({len(alert_buffer)} alerts)...")

                report = generate_report(client, alert_buffer, report_id)

                if report:
                    report_count += 1

                    # Save to file
                    fpath = f"data/reports/{report_id}.json"
                    with open(fpath, "w") as f:
                        json.dump(report, f, indent=2)

                    # Publish to Kafka
                    producer.produce(
                        OUTPUT_TOPIC,
                        key=report_id.encode("utf-8"),
                        value=json.dumps(report).encode("utf-8"),
                    )
                    producer.flush()

                    # Pretty print to terminal
                    print(f"\n{'='*60}")
                    print(f"INCIDENT REPORT — {report_id}")
                    print(f"Severity: {report.get('severity_level','?')} | "
                          f"Alerts: {len(alert_buffer)}")
                    print(f"\nSUMMARY: {report.get('summary','')}")
                    print(f"\nINCIDENTS ({len(report.get('incidents',[]))}):")
                    for inc in report.get("incidents", []):
                        print(f"  [{inc.get('urgency','?')}] "
                              f"{inc.get('fault_type')} — "
                              f"{inc.get('city')} — "
                              f"{inc.get('description','')[:80]}")
                        print(f"    → {inc.get('recommended_action','')}")
                    patterns = report.get("cross_station_patterns")
                    if patterns:
                        print(f"\nCROSS-STATION PATTERNS: {patterns}")
                    print(f"{'='*60}\n")
                    print(f"Saved: {fpath}")

                alert_buffer = []
                batch_start  = time.time()

    except KeyboardInterrupt:
        print(f"\nReport agent stopped. {report_count} reports generated.")
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    run()