import platform
import subprocess
import sys
import time

OS_NAME = platform.system()  # 'Windows', 'Darwin' (macOS), or 'Linux'
TOPICS = ["traccar-positions", "alerts", "traccar-positions-dlq"]


def run(cmd, ignore_error=False):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0 and not ignore_error:
        print(f"  [!] Command failed: {cmd}\n      {result.stderr.strip()}")
    return result


def start_podman_vm():
    # Windows and macOS run Podman inside a lightweight VM; native Linux does not
    if OS_NAME in ("Windows", "Darwin"):
        print("[1/4] Starting Podman VM...")
        run("podman machine start", ignore_error=True)
    else:
        print("[1/4] Native Linux detected — skipping Podman VM step.")


def start_kafka():
    print("[2/4] Starting Kafka container...")
    res = run("podman start kafka", ignore_error=True)
    if res.returncode != 0:
        print("  -> Container 'kafka' not found. Creating new KRaft container...")
        create_cmd = (
            "podman run -d --name kafka -p 9092:9092 "
            "-e KAFKA_NODE_ID=1 "
            "-e KAFKA_PROCESS_ROLES=broker,controller "
            "-e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 "
            "-e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092 "
            "-e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER "
            "-e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT "
            "-e KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093 "
            "-e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 "
            "-e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 "
            "-e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 "
            "-e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 "
            "-e KAFKA_NUM_PARTITIONS=3 "
            "docker.io/apache/kafka:latest"
        )
        run(create_cmd)

    # Start Kafka UI if you created it earlier
    run("podman start kafka-ui", ignore_error=True)
    time.sleep(3)


def ensure_topics():
    print("[3/4] Verifying Kafka topics...")
    for topic in TOPICS:
        cmd = (
            f"podman exec kafka /opt/kafka/bin/kafka-topics.sh "
            f"--bootstrap-server 127.0.0.1:9092 --create --if-not-exists "
            f"--topic {topic} --partitions 3 --replication-factor 1"
        )
        run(cmd, ignore_error=True)
    print(f"  -> Ready: {', '.join(TOPICS)}")


def start_traccar():
    print("[4/4] Starting Traccar service...")
    if OS_NAME == "Windows":
        res = run('powershell -Command "Start-Service traccar"', ignore_error=True)
        if res.returncode != 0:
            print("  [!] Could not start Traccar automatically (requires Admin terminal).")
            print("      Run in an Admin PowerShell: Start-Service traccar")
    elif OS_NAME == "Linux":
        run("sudo systemctl start traccar", ignore_error=True)
    elif OS_NAME == "Darwin":
        run("brew services start traccar", ignore_error=True)


if __name__ == "__main__":
    print("=== Starting traccar-kafka-lab ===")
    start_podman_vm()
    start_kafka()
    ensure_topics()
    start_traccar()
    print("\nAll services initialized.")
    print("  - Traccar Web UI : http://localhost:8082")
    print("  - Kafka Broker   : 127.0.0.1:9092")