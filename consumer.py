import json
import sqlite3
import time
import uuid
from confluent_kafka import Consumer, KafkaError, Producer

WORKER_ID = f"worker-{str(uuid.uuid4())[:6]}"
DB_FILE = "positions.db"
KNOTS_TO_KMH = 1.852

# In-memory per-device state: { device_id: consecutive_overspeed_count }
overspeed_streaks = {}


def init_db():
    conn = sqlite3.connect(DB_FILE, timeout=10.0)
    conn.execute("PRAGMA journal_mode=WAL;")  # Allows concurrent reads/writes across workers
    conn.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            device_id TEXT,
            latitude REAL,
            longitude REAL,
            speed_kmh REAL,
            timestamp INTEGER,
            kafka_partition INTEGER,
            kafka_offset INTEGER,
            UNIQUE(kafka_partition, kafka_offset)
        )
    """)
    conn.commit()
    conn.close()


def normalize_payload(raw_data):
    """Supports both real Traccar nested JSON and flat mock JSON."""
    if "position" in raw_data and "device" in raw_data:
        pos = raw_data["position"]
        dev = raw_data["device"]
        speed_knots = float(pos.get("speed", 0.0))
        return {
            "source": "traccar-server",
            "device_id": str(dev.get("uniqueId") or dev.get("id")),
            "latitude": pos.get("latitude"),
            "longitude": pos.get("longitude"),
            "speed_kmh": round(speed_knots * KNOTS_TO_KMH, 1),
            "timestamp": int(time.time()),
        }

    # Fallback: flat mock producer JSON
    return {
        "source": "mock-producer",
        "device_id": str(raw_data.get("deviceId", "unknown")),
        "latitude": raw_data.get("latitude"),
        "longitude": raw_data.get("longitude"),
        "speed_kmh": round(float(raw_data.get("speed", 0.0)), 1),
        "timestamp": int(raw_data.get("timestamp", time.time())),
    }


init_db()

kafka_base_conf = {
    "bootstrap.servers": "127.0.0.1:9092",
    "broker.address.family": "v4",  # Forces IPv4 on Windows 11
}

consumer = Consumer({
    **kafka_base_conf,
    "group.id": "telemetry-processors",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    "client.id": WORKER_ID,
    "partition.assignment.strategy": "cooperative-sticky",
})

producer = Producer(kafka_base_conf)


def on_assign(c, partitions):
    print(f"\n[REBALANCE] {WORKER_ID} ASSIGNED partitions: {[p.partition for p in partitions]}\n")


def on_revoke(c, partitions):
    # Note: In-memory state for revoked partitions is cleared on rebalance
    overspeed_streaks.clear()
    print(f"\n[REBALANCE] {WORKER_ID} REVOKED partitions: {[p.partition for p in partitions]}\n")


consumer.subscribe(["traccar-positions"], on_assign=on_assign, on_revoke=on_revoke)
print(f"[{WORKER_ID}] Active. Listening to 'traccar-positions'...")

db_conn = sqlite3.connect(DB_FILE, timeout=10.0)
cursor = db_conn.cursor()

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"[{WORKER_ID}] Kafka Error: {msg.error()}")
            continue

        part = msg.partition()
        offset = msg.offset()

        try:
            raw_json = json.loads(msg.value().decode("utf-8"))
            event = normalize_payload(raw_json)
            device_id = event["device_id"]
            speed = event["speed_kmh"]

            # Idempotent insert: ignores duplicates if message is replayed
            cursor.execute(
                """INSERT OR IGNORE INTO positions
                   (source, device_id, latitude, longitude, speed_kmh, timestamp, kafka_partition, kafka_offset)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    event["source"],
                    device_id,
                    event["latitude"],
                    event["longitude"],
                    speed,
                    event["timestamp"],
                    part,
                    offset,
                ),
            )
            db_conn.commit()

            print(
                f"[{WORKER_ID}] ({event['source']}) {device_id} | "
                f"Speed: {speed} km/h | [Part {part}, Offset {offset}]"
            )

            # Stateful speeding evaluation
            if speed > 80:
                overspeed_streaks[device_id] = overspeed_streaks.get(device_id, 0) + 1
                streak = overspeed_streaks[device_id]

                if speed > 100 or streak >= 2:
                    alert_type = "CRITICAL_OVERSPEED" if speed > 100 else "SUSTAINED_OVERSPEED"
                    alert_payload = {
                        "alertType": alert_type,
                        "deviceId": device_id,
                        "speedKmh": speed,
                        "consecutiveHighPings": streak,
                        "timestamp": event["timestamp"],
                    }
                    producer.produce(
                        topic="alerts",
                        key=device_id.encode("utf-8"),
                        value=json.dumps(alert_payload).encode("utf-8"),
                    )
                    producer.poll(0)
                    print(f"  --> ALERT [{alert_type}] for {device_id} ({speed} km/h, streak={streak})")
            else:
                overspeed_streaks[device_id] = 0

            consumer.commit(message=msg, asynchronous=False)

        except (json.JSONDecodeError, UnicodeDecodeError) as err:
            # Route poison pills to the Dead Letter Queue instead of crashing
            print(f"[{WORKER_ID}] Poison pill at [Part {part}, Offset {offset}] -> Routing to DLQ")
            producer.produce(
                topic="traccar-positions-dlq",
                key=msg.key(),
                value=msg.value(),
                headers=[("error", str(err)), ("source_partition", str(part)), ("source_offset", str(offset))],
            )
            producer.flush()
            consumer.commit(message=msg, asynchronous=False)

        except Exception as e:
            db_conn.rollback()
            print(f"[{WORKER_ID}] Transient error: {e}. Leaving offset uncommitted for retry.")

except KeyboardInterrupt:
    print(f"\n[{WORKER_ID}] Shutting down...")
finally:
    db_conn.close()
    consumer.close()
    producer.flush()