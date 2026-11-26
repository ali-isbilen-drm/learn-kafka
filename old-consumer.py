import json
import sqlite3
import sys
import uuid
from confluent_kafka import Consumer, Producer, KafkaError

WORKER_ID = f"worker-{str(uuid.uuid4())[:6]}"

# --- SQLite Database Initialization ---
def init_db():
    conn = sqlite3.connect('positions.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id TEXT,
            latitude REAL,
            longitude REAL,
            speed INTEGER,
            timestamp INTEGER,
            kafka_partition INTEGER,
            kafka_offset INTEGER
        )
    ''')
    conn.commit()
    conn.close()

init_db()

# --- Kafka Setup ---
consumer_conf = {
    'bootstrap.servers': '127.0.0.1:9092',
    'broker.address.family': 'v4',        # <--- Forces IPv4
    'group.id': 'telemetry-processors',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'client.id': WORKER_ID,
    'partition.assignment.strategy': 'cooperative-sticky'
}

alert_producer = Producer({
    'bootstrap.servers': '127.0.0.1:9092',
    'broker.address.family': 'v4'         # <--- Forces IPv4
})

consumer = Consumer(consumer_conf)

def on_assign(c, partitions):
    print(f"\n[REBALANCE] {WORKER_ID} ASSIGNED partitions: {[p.partition for p in partitions]}\n")

def on_revoke(c, partitions):
    print(f"\n[REBALANCE] {WORKER_ID} REVOKED partitions: {[p.partition for p in partitions]}\n")

consumer.subscribe(['traccar-positions'], on_assign=on_assign, on_revoke=on_revoke)

print(f"[{WORKER_ID}] Started. Listening to 'traccar-positions'...")

db_conn = sqlite3.connect('positions.db', autocommit=False)
cursor = db_conn.cursor()

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"[{WORKER_ID}] Kafka Error: {msg.error()}")
                break

        try:
            # 1. Parse JSON Payload
            data = json.loads(msg.value().decode('utf-8'))
            device_id = data.get('deviceId')
            speed = data.get('speed', 0)
            lat = data.get('latitude')
            lon = data.get('longitude')
            ts = data.get('timestamp')
            part = msg.partition()
            offset = msg.offset()

            # 2. Pipeline: Insert into SQLite
            cursor.execute(
                '''INSERT INTO positions (device_id, latitude, longitude, speed, timestamp, kafka_partition, kafka_offset)
                   VALUES (?, ?, ?, ?, ?, ?, ?)''',
                (device_id, lat, lon, speed, ts, part, offset)
            )
            db_conn.commit()

            print(f"[{WORKER_ID}] Saved to DB: {device_id} | Speed: {speed} km/h | [Part {part}, Offset {offset}]")

            # 3. Business Rule: Publish Alert if speed > 80
            if speed > 80:
                alert_payload = {
                    'alert': 'OVERSPEED_DETECTED',
                    'deviceId': device_id,
                    'speed': speed,
                    'timestamp': ts,
                    'threshold': 80
                }
                alert_producer.produce(
                    topic='alerts',
                    key=device_id.encode('utf-8'),
                    value=json.dumps(alert_payload).encode('utf-8')
                )
                alert_producer.poll(0)
                print(f"  --> ALERT DISPATCHED for {device_id} (Speed: {speed})")

            # 4. Commit offset ONLY after DB write and alert emission succeed
            consumer.commit(message=msg, asynchronous=False)

        except json.JSONDecodeError:
            # Senior Concept: Handling Poison Pills (Invalid payloads)
            print(f"[{WORKER_ID}] Poison pill detected! Discarding bad message at offset {msg.offset()}")
            consumer.commit(message=msg, asynchronous=False)
        except Exception as e:
            db_conn.rollback()
            print(f"[{WORKER_ID}] Processing failed: {e}. Offset not committed.")

except KeyboardInterrupt:
    print(f"\n[{WORKER_ID}] Stopping gracefully...")
finally:
    db_conn.close()
    consumer.close()
    alert_producer.flush()