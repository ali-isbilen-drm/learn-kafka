from confluent_kafka import Consumer

consumer = Consumer({
    'bootstrap.servers': '127.0.0.1:9092',
    'broker.address.family': 'v4',        # <--- Forces IPv4
    'group.id': 'alert-monitoring-group',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True
})
consumer.subscribe(['alerts'])

print("Alert Monitor Active. Listening for overspeed notifications...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        print(f"[CRITICAL ALERT RECEIVED] Key={msg.key().decode('utf-8')}: {msg.value().decode('utf-8')}")
except KeyboardInterrupt:
    consumer.close()