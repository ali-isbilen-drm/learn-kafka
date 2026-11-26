import json
import random
import time
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': '127.0.0.1:9092',
    'broker.address.family': 'v4',        # <--- Forces IPv4
    'client.id': 'gps-simulator-producer'
}

producer = Producer(conf)

DEVICES = ['device_101', 'device_102', 'device_103', 'device_104']

def delivery_report(err, msg):
    """Called once per message to indicate delivery result."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"[PRODUCER] Sent -> Key: {msg.key().decode('utf-8')} | Topic: {msg.topic()} | Partition: {msg.partition()} | Offset: {msg.offset()}")

print("Starting GPS Telemetry Producer (Press Ctrl+C to stop)...")

try:
    while True:
        device_id = random.choice(DEVICES)
        payload = {
            'deviceId': device_id,
            'latitude': round(41.0 + random.uniform(-0.1, 0.1), 6),
            'longitude': round(28.9 + random.uniform(-0.1, 0.1), 6),
            'speed': random.randint(30, 115),  # Triggers alerts when > 80
            'timestamp': int(time.time())
        }

        # IMPORTANT: Partitioning by key preserves per-device ordering
        producer.produce(
            topic='traccar-positions',
            key=device_id.encode('utf-8'),
            value=json.dumps(payload).encode('utf-8'),
            callback=delivery_report
        )

        producer.poll(0)  # Serve delivery callbacks
        time.sleep(random.uniform(2.5, 4.0))

except KeyboardInterrupt:
    print("\nStopping producer...")
finally:
    producer.flush()