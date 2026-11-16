#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT="$HOME/fraud-detection"
KAFKA="$HOME/kafka_2.13-4.3.1"
FLINK="/mnt/d/DataEng/flink-2.2.0"
PYTHON="$HOME/pyflink22-env/bin/python"

RUNTIME="$PROJECT/.runtime/demo"
LOGS="$PROJECT/logs/demo"

cd "$PROJECT"

# Load demo-only configuration.
set -a
source "$PROJECT/.env.demo"
set +a

mkdir -p "$RUNTIME" "$LOGS"

# Convert the Flink file URI to a real local filesystem path.
if [[ "$FLINK_CHECKPOINT_DIR" != file:///* ]]; then
    echo "ERROR: FLINK_CHECKPOINT_DIR must be a local file:/// URI."
    exit 1
fi

CHECKPOINT_PATH="${FLINK_CHECKPOINT_DIR#file://}"
mkdir -p "$CHECKPOINT_PATH"

echo "========== DEMO STARTUP =========="

# Verify required paths.
for path in "$KAFKA/bin/kafka-server-start.sh" \
            "$KAFKA/bin/kafka-topics.sh" \
            "$FLINK/bin/start-cluster.sh" \
            "$FLINK/bin/flink" \
            "$PYTHON" \
            "$PROJECT/src/ieee_flink.py"; do
    if [[ ! -e "$path" ]]; then
        echo "ERROR: Required path missing: $path"
        exit 1
    fi
done

# Do not mix demo and development services.
if ss -ltn | grep -qE ':(9092|9093|8081|6123)\b'; then
    echo "ERROR: Kafka or Flink ports are already occupied."
    echo "Stop the existing project services before starting the demo."
    exit 1
fi

# Check independent services.
if ! redis-cli ping | grep -q PONG; then
    echo "ERROR: Redis is not responding."
    exit 1
fi

if ! "$PYTHON" -c \
    'from pymongo import MongoClient; MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=3000).admin.command("ping")' \
    >/dev/null 2>&1; then
    echo "ERROR: MongoDB is not responding."
    exit 1
fi

# Build a fresh, minimal Flink worker source bundle.
# Generate it before starting services so missing modules fail safely.
echo "Building Flink source bundle..."

"$PYTHON" - <<'PY_BUNDLE'
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
import os

project = Path.home() / "fraud-detection"

modules = [
    "src/__init__.py",
    "src/inference.py",
    "src/behaviour_engine.py",
    "src/risk_engine.py",
    "src/mongo_store.py",
]

for name in modules:
    path = project / name
    if not path.is_file():
        raise SystemExit(f"ERROR: Required source module missing: {name}")

archive = project / "src_bundle.zip"
temporary = project / "src_bundle.zip.tmp"

try:
    with ZipFile(temporary, "w", ZIP_DEFLATED) as bundle:
        for name in modules:
            bundle.write(project / name, arcname=name)

    temporary.replace(archive)
except Exception:
    temporary.unlink(missing_ok=True)
    raise

print(f"Created {archive} ({archive.stat().st_size:,} bytes)")
with ZipFile(archive) as bundle:
    for name in bundle.namelist():
        print(f"  {name}")
PY_BUNDLE

# Clean up demo-owned services if startup fails partway through.
STARTUP_COMPLETE=0

cleanup_on_failure() {
    local exit_code=$?

    if [[ "$exit_code" -ne 0 && "$STARTUP_COMPLETE" -eq 0 ]]; then
        trap - EXIT
        echo
        echo "ERROR: Demo startup failed. Cleaning up demo-owned services..."

        if ! "$PROJECT/scripts/stop_demo.sh"; then
            echo "WARNING: Automatic cleanup was incomplete."
            echo "Inspect the demo runtime markers and service logs."
        fi
    fi

    exit "$exit_code"
}

trap cleanup_on_failure EXIT

# Start Kafka.
echo "Starting Kafka..."
cd "$KAFKA"

nohup bin/kafka-server-start.sh config/server-wsl.properties \
    > "$LOGS/kafka.log" 2>&1 &

KAFKA_PID=$!
echo "$KAFKA_PID" > "$RUNTIME/kafka.pid"

KAFKA_READY=0
for i in {1..30}; do
    if bin/kafka-broker-api-versions.sh \
        --bootstrap-server localhost:9092 >/dev/null 2>&1; then
        KAFKA_READY=1
        break
    fi
    sleep 2
done

if [[ "$KAFKA_READY" -ne 1 ]]; then
    echo "ERROR: Kafka did not become ready. Check $LOGS/kafka.log"
    exit 1
fi

# Create only the isolated demo topic.
bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic "$KAFKA_TOPIC" \
    --partitions 1 \
    --replication-factor 1

echo "Demo Kafka topic ready: $KAFKA_TOPIC"

# Start Flink.
echo "Starting Flink cluster..."
cd "$FLINK"

# Record ownership before startup so the EXIT trap can clean up
# even if start-cluster.sh partially starts and then fails.
touch "$RUNTIME/flink-cluster.started"
./bin/start-cluster.sh > "$LOGS/flink-cluster.log" 2>&1

FLINK_READY=0
for i in {1..30}; do
    if ss -ltn | grep -qE ':(8081)\b'; then
        FLINK_READY=1
        break
    fi
    sleep 2
done

if [[ "$FLINK_READY" -ne 1 ]]; then
    echo "ERROR: Flink did not become ready."
    exit 1
fi

# Submit the correct IEEE-CIS inference job.
echo "Submitting IEEE-CIS Flink job..."
set +e
JOB_OUTPUT=$(
    ./bin/flink run -d \
        -pyclientexec "$PYTHON" \
        -pyexec "$PYTHON" \
        -py "$PROJECT/src/ieee_flink.py" 2>&1
)
JOB_STATUS=$?
set -e

echo "$JOB_OUTPUT" > "$LOGS/flink-job-submit.log"

if [[ "$JOB_STATUS" -ne 0 ]]; then
    echo "ERROR: Flink submission failed. Check $LOGS/flink-job-submit.log"
    exit 1
fi

JOB_ID=$(printf '%s\n' "$JOB_OUTPUT" | grep -Eo '[0-9a-f]{32}' | tail -n 1 || true)

if [[ -z "$JOB_ID" ]]; then
    echo "ERROR: Could not identify Flink JobID."
    exit 1
fi

echo "$JOB_ID" > "$RUNTIME/flink-job.id"

JOB_STATUS_OUTPUT=$("$FLINK/bin/flink" list -r 2>&1)

if ! printf '%s\n' "$JOB_STATUS_OUTPUT" | grep -F "$JOB_ID" | grep -q '(RUNNING)'; then
    echo "ERROR: Flink job is not RUNNING."
    echo "$JOB_STATUS_OUTPUT"
    exit 1
fi

echo
echo "DEMO PIPELINE STARTED SUCCESSFULLY."
echo "MongoDB: $MONGODB_DATABASE"
echo "Redis prefix: $REDIS_PREFIX"
echo "Kafka topic: $KAFKA_TOPIC"
echo "Kafka group: $KAFKA_GROUP_ID"
echo "Starting offsets: $KAFKA_STARTING_OFFSETS"
echo "Flink JobID: $JOB_ID"
echo "Simulator: NOT STARTED"
STARTUP_COMPLETE=1
