#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT="$HOME/fraud-detection"
KAFKA="$HOME/kafka_2.13-4.3.1"
RUNTIME="$PROJECT/.runtime/demo"
ENV_FILE="$PROJECT/.env.demo"

echo "========== RESET DEMO DATA =========="

# Load demo configuration.
[[ -f "$ENV_FILE" ]] || {
    echo "ERROR: .env.demo not found."
    exit 1
}

set -a
source "$ENV_FILE"
set +a

# Fail-closed isolation checks.
[[ "${MONGODB_DATABASE:-}" == "fraud_detection_demo" ]] || {
    echo "ERROR: Unexpected MongoDB database."; exit 1;
}
[[ "${REDIS_PREFIX:-}" == "fraud:demo:" ]] || {
    echo "ERROR: Unexpected Redis prefix."; exit 1;
}
[[ "${KAFKA_TOPIC:-}" == "ieee_transactions_demo" ]] || {
    echo "ERROR: Unexpected Kafka topic."; exit 1;
}
[[ "${KAFKA_GROUP_ID:-}" == "ieee-fraud-demo-v1" ]] || {
    echo "ERROR: Unexpected Kafka group."; exit 1;
}

# Flink must be stopped.
for marker in flink-job.id flink-cluster.started; do
    if [[ -e "$RUNTIME/$marker" ]]; then
        echo "ERROR: Flink runtime marker still exists: $marker"
        echo "Run scripts/stop_demo.sh --keep-kafka first."
        exit 1
    fi
done

# Kafka must be running and owned by the demo launcher.
if [[ ! -f "$RUNTIME/kafka.pid" ]]; then
    echo "ERROR: Demo Kafka PID marker is missing."
    echo "Run scripts/stop_demo.sh --keep-kafka first."
    exit 1
fi

KAFKA_PID=$(cat "$RUNTIME/kafka.pid")

if ! kill -0 "$KAFKA_PID" 2>/dev/null; then
    echo "ERROR: Demo Kafka process is not running."
    exit 1
fi

KAFKA_CMD=$(tr '\0' ' ' < "/proc/$KAFKA_PID/cmdline" 2>/dev/null || true)

if [[ "$KAFKA_CMD" != *"kafka.Kafka"* ||
      "$KAFKA_CMD" != *"server-wsl.properties"* ]]; then
    echo "ERROR: PID does not match the expected demo Kafka broker."
    echo "No reset performed."
    exit 1
fi

# If Flink REST is reachable, ensure no active jobs remain.
if curl --silent --fail http://localhost:8081/overview >/dev/null; then
    JOBS_JSON=$(curl --silent --fail http://localhost:8081/jobs/overview) || {
        echo "ERROR: Could not inspect Flink jobs."
        exit 1
    }

    if JOBS_JSON="$JOBS_JSON" python - <<'PY_JOBS'
import json
import os

data = json.loads(os.environ["JOBS_JSON"])
active = [
    job for job in data.get("jobs", [])
    if job.get("status") in ("RUNNING", "CREATED", "RESTARTING")
]

if active:
    print("Flink jobs still active:")
    for job in active:
        print(f"  {job.get('jid')} {job.get('status')}")
    raise SystemExit(1)
PY_JOBS
    then
        echo "ERROR: Flink has active jobs. Reset refused."
        exit 1
    fi
elif ss -ltn | grep -qE ':(8081)\b'; then
    echo "ERROR: Flink REST port is occupied but its API is unavailable."
    exit 1
fi

TOPICS="$KAFKA/bin/kafka-topics.sh"
GROUPS="$KAFKA/bin/kafka-consumer-groups.sh"

for path in "$TOPICS" "$GROUPS"; do
    [[ -x "$path" ]] || {
        echo "ERROR: Kafka utility missing: $path"
        exit 1
    }
done

# Verify service connectivity before asking for confirmation.
"$TOPICS" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --list >/dev/null

source "$HOME/pyflink22-env/bin/activate"

python - <<'PY_PREFLIGHT'
import os
import redis
from pymongo import MongoClient

client = MongoClient(
    "mongodb://localhost:27017",
    serverSelectionTimeoutMS=5000
)
client.admin.command("ping")
print("MongoDB connectivity: OK")
client.close()

r = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", "6379")),
    db=int(os.getenv("REDIS_DB", "0")),
    socket_connect_timeout=5,
)
r.ping()
print("Redis connectivity: OK")
PY_PREFLIGHT

echo
echo "The following DEMO resources will be cleared:"
echo "  MongoDB: $MONGODB_DATABASE (transactions, alerts)"
echo "  Redis:   ${REDIS_PREFIX}*"
echo "  Kafka:   $KAFKA_TOPIC"
echo "  Group:   $KAFKA_GROUP_ID"
echo
echo "Development data, models, checkpoints and results are preserved."
echo

read -r -p 'Type RESET DEMO to continue: ' CONFIRM

if [[ "$CONFIRM" != "RESET DEMO" ]]; then
    echo "Reset cancelled. No data was deleted."
    exit 0
fi

# Delete only the demo consumer group.
if "$GROUPS" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --list | grep -Fxq "$KAFKA_GROUP_ID"; then

    echo "Deleting demo consumer group..."

    "$GROUPS" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
        --delete --group "$KAFKA_GROUP_ID"

    GROUP_DELETED=0

    for i in {1..30}; do
        if ! "$GROUPS" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
            --list | grep -Fxq "$KAFKA_GROUP_ID"; then
            GROUP_DELETED=1
            break
        fi
        sleep 1
    done

    [[ "$GROUP_DELETED" -eq 1 ]] || {
        echo "ERROR: Consumer group deletion timed out."
        exit 1
    }
fi

# Delete and recreate only the demo topic.
if "$TOPICS" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --list | grep -Fxq "$KAFKA_TOPIC"; then

    echo "Deleting demo Kafka topic..."

    "$TOPICS" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
        --delete --topic "$KAFKA_TOPIC"

    TOPIC_DELETED=0

    for i in {1..30}; do
        if ! "$TOPICS" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
            --list | grep -Fxq "$KAFKA_TOPIC"; then
            TOPIC_DELETED=1
            break
        fi
        sleep 1
    done

    [[ "$TOPIC_DELETED" -eq 1 ]] || {
        echo "ERROR: Topic deletion timed out."
        exit 1
    }
fi

echo "Creating clean demo Kafka topic..."

"$TOPICS" --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" \
    --create --topic "$KAFKA_TOPIC" \
    --partitions 1 --replication-factor 1

# Clear only the two demo MongoDB collections and demo Redis prefix.
python - <<'PY_RESET'
import os
import redis
from pymongo import MongoClient

database_name = os.environ["MONGODB_DATABASE"]
prefix = os.environ["REDIS_PREFIX"]

client = MongoClient(
    "mongodb://localhost:27017",
    serverSelectionTimeoutMS=5000
)
client.admin.command("ping")

db = client[database_name]

for name in ("transactions", "alerts"):
    result = db[name].delete_many({})
    print(f"MongoDB {database_name}.{name}: deleted {result.deleted_count}")

r = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", "6379")),
    db=int(os.getenv("REDIS_DB", "0")),
    decode_responses=True,
)

r.ping()
deleted = 0
batch = []

for key in r.scan_iter(match=prefix + "*", count=500):
    batch.append(key)
    if len(batch) >= 500:
        deleted += r.delete(*batch)
        batch.clear()

if batch:
    deleted += r.delete(*batch)

print(f"Redis keys matching {prefix}*: deleted {deleted}")
client.close()
PY_RESET

# Stop only the verified demo Kafka broker after reset.
echo "Stopping demo Kafka broker (PID $KAFKA_PID)..."
kill -TERM "$KAFKA_PID"

for i in {1..20}; do
    kill -0 "$KAFKA_PID" 2>/dev/null || break
    sleep 1
done

if kill -0 "$KAFKA_PID" 2>/dev/null; then
    echo "ERROR: Kafka did not stop. PID marker retained."
    exit 1
fi

rm -f "$RUNTIME/kafka.pid"

echo
echo "Demo reset completed successfully."
echo "Development data and Flink checkpoints were preserved."
