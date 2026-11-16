#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT="$HOME/fraud-detection"
KAFKA="$HOME/kafka_2.13-4.3.1"
FLINK="/mnt/d/DataEng/flink-2.2.0"
PYTHON="$HOME/pyflink22-env/bin/python"

KAFKA_CONFIG="$KAFKA/config/server-wsl.properties"
RUNTIME="$PROJECT/.runtime"
LOGS="$PROJECT/logs"

mkdir -p "$RUNTIME" "$LOGS"
cd "$PROJECT"

echo "Checking existing services..."

if ss -ltn | grep -qE ':(9092|9093)\b'; then
    echo "ERROR: Kafka ports are already occupied."
    echo "Stop the existing Kafka broker before using this script."
    exit 1
fi

if ss -ltn | grep -qE ':(8081|6123)\b'; then
    echo "ERROR: Flink ports are already occupied."
    echo "Stop the existing Flink cluster before using this script."
    exit 1
fi

if pgrep -af '[s]imulator.py' >/dev/null; then
    echo "ERROR: A simulator process is already running."
    exit 1
fi

if ! redis-cli ping | grep -q PONG; then
    echo "ERROR: Redis is not responding. Start Redis separately first."
    exit 1
fi

if [[ ! -x "$PYTHON" ]]; then
    echo "ERROR: Project Python environment not found: $PYTHON"
    exit 1
fi

echo "Starting Kafka..."
cd "$KAFKA"
nohup bin/kafka-server-start.sh config/server-wsl.properties \
    > "$LOGS/kafka.log" 2>&1 &
KAFKA_PID=$!
echo "$KAFKA_PID" > "$RUNTIME/kafka.pid"

for i in {1..30}; do
    if "$KAFKA/bin/kafka-broker-api-versions.sh" \
        --bootstrap-server localhost:9092 >/dev/null 2>&1; then
        break
    fi
    sleep 2
done

if ! "$KAFKA/bin/kafka-broker-api-versions.sh" \
    --bootstrap-server localhost:9092 >/dev/null 2>&1; then
    echo "ERROR: Kafka did not become ready. Check logs/kafka.log"
    exit 1
fi
echo "Kafka is ready."

if ! "$KAFKA/bin/kafka-topics.sh" \
    --bootstrap-server localhost:9092 \
    --describe --topic transactions >/dev/null 2>&1; then
    echo "ERROR: Kafka topic 'transactions' was not found."
    exit 1
fi
echo "Kafka topic 'transactions' is ready."

echo "Starting Flink cluster..."
cd "$FLINK"
./bin/start-cluster.sh > "$LOGS/flink-cluster.log" 2>&1
touch "$RUNTIME/flink-cluster.started"

for i in {1..30}; do
    if ss -ltn | grep -qE ':(8081)\b'; then
        break
    fi
    sleep 2
done

if ! ss -ltn | grep -qE ':(8081)\b'; then
    echo "ERROR: Flink did not become ready. Check Flink logs."
    exit 1
fi
echo "Flink cluster is ready."

echo "Submitting Flink job..."
set +e
JOB_OUTPUT=$(
    ./bin/flink run -d \
        -pyclientexec "$PYTHON" \
        -pyexec "$PYTHON" \
        -py "$PROJECT/src/kafka_to_flink.py" 2>&1
)
JOB_STATUS=$?
set -e

echo "$JOB_OUTPUT" > "$LOGS/flink-job-submit.log"
if [[ $JOB_STATUS -ne 0 ]]; then
    echo "ERROR: Flink job submission failed. See logs/flink-job-submit.log"
    exit 1
fi

JOB_ID=$(printf '%s\n' "$JOB_OUTPUT" | grep -Eo '[0-9a-f]{32}' | tail -n 1 || true)
if [[ -z "$JOB_ID" ]]; then
    echo "ERROR: Could not identify the submitted JobID."
    echo "Check logs/flink-job-submit.log"
    exit 1
fi

echo "$JOB_ID" > "$RUNTIME/flink-job.id"
echo "Flink job submitted: $JOB_ID"

JOB_STATUS_OUTPUT=$("$FLINK/bin/flink" list -r 2>&1)
if ! printf '%s\\n' "$JOB_STATUS_OUTPUT" | grep -F "$JOB_ID" | grep -q '(RUNNING)'; then
    echo "ERROR: Flink job was submitted but is not RUNNING."
    echo "$JOB_STATUS_OUTPUT"
    exit 1
fi
echo "Flink job is RUNNING."

echo "Starting transaction simulator..."
cd "$PROJECT"
nohup "$PYTHON" -u src/simulator.py \
    > "$LOGS/simulator.log" 2>&1 &
SIMULATOR_PID=$!
echo "$SIMULATOR_PID" > "$RUNTIME/simulator.pid"

sleep 2
if ! kill -0 "$SIMULATOR_PID" 2>/dev/null; then
    echo "ERROR: Simulator exited. Check logs/simulator.log"
    exit 1
fi

echo
echo "Project startup commands completed."
echo "Kafka, Flink, Flink job, and simulator have been started."
echo "Redis remains independently managed."
