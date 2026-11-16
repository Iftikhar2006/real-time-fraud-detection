#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT="$HOME/fraud-detection"
KAFKA="$HOME/kafka_2.13-4.3.1"
FLINK="/mnt/d/DataEng/flink-2.2.0"
RUNTIME="$PROJECT/.runtime/demo"

KEEP_KAFKA=0

if [[ "${1:-}" == "--keep-kafka" ]]; then
    KEEP_KAFKA=1
elif [[ $# -gt 0 ]]; then
    echo "Usage: scripts/stop_demo.sh [--keep-kafka]"
    exit 1
fi

echo "========== STOP DEMO =========="

STOP_FAILED=0

# Cancel only the recorded demo Flink job.
if [[ -f "$RUNTIME/flink-job.id" ]]; then
    JOB_ID=$(cat "$RUNTIME/flink-job.id")
    echo "Cancelling demo Flink job: $JOB_ID"

    if "$FLINK/bin/flink" cancel "$JOB_ID"; then
        rm -f "$RUNTIME/flink-job.id"
    else
        echo "WARNING: Flink job cancellation failed; will still attempt cluster shutdown."
        STOP_FAILED=1
    fi
else
    echo "No demo Flink JobID recorded."
fi

# Stop only the Flink cluster started by the demo launcher.
if [[ -f "$RUNTIME/flink-cluster.started" ]]; then
    echo "Stopping demo Flink cluster..."

    if "$FLINK/bin/stop-cluster.sh"; then
        rm -f "$RUNTIME/flink-cluster.started" "$RUNTIME/flink-job.id"
    else
        echo "ERROR: Flink cluster shutdown failed."
        STOP_FAILED=1
    fi
else
    echo "No demo-owned Flink cluster recorded."
fi

# Handle only the demo-owned Kafka broker.
if [[ -f "$RUNTIME/kafka.pid" ]]; then
    PID=$(cat "$RUNTIME/kafka.pid")

    if ! kill -0 "$PID" 2>/dev/null; then
        echo "Recorded Kafka process is already stopped."
        rm -f "$RUNTIME/kafka.pid"
    else
        CMD=$(tr '\0' ' ' < "/proc/$PID/cmdline" 2>/dev/null || true)

        if [[ "$CMD" != *"kafka.Kafka"* ||
              "$CMD" != *"server-wsl.properties"* ]]; then
            echo "ERROR: Recorded PID does not match the demo Kafka broker."
            echo "Leaving it untouched."
            exit 1
        fi

        if [[ "$KEEP_KAFKA" -eq 1 ]]; then
            echo "Keeping demo Kafka running for reset."
        else
            echo "Stopping demo Kafka broker (PID $PID)..."
            kill -TERM "$PID"

            for i in {1..20}; do
                kill -0 "$PID" 2>/dev/null || break
                sleep 1
            done

            if kill -0 "$PID" 2>/dev/null; then
                echo "ERROR: Kafka is still running. Not forcing termination."
                exit 1
            fi

            rm -f "$RUNTIME/kafka.pid"
        fi
    fi
else
    echo "No demo-owned Kafka PID recorded."
fi

echo
if [[ "$STOP_FAILED" -ne 0 ]]; then
    echo "WARNING: Demo shutdown completed with errors. Review the messages above."
    exit 1
fi

echo "Demo shutdown procedure completed."
echo "MongoDB and Redis were not stopped or deleted."
echo "Kafka data and Flink checkpoints were preserved."
