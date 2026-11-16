#!/usr/bin/env bash
set -u

PROJECT="$HOME/fraud-detection"
KAFKA="$HOME/kafka_2.13-4.3.1"
FLINK="/mnt/d/DataEng/flink-2.2.0"
RUNTIME="$PROJECT/.runtime"

echo "Stopping fraud detection project..."

# 1. Stop only the simulator PID recorded by our startup script.
if [[ -f "$RUNTIME/simulator.pid" ]]; then
    PID=$(cat "$RUNTIME/simulator.pid")

    if kill -0 "$PID" 2>/dev/null; then
        CMD=$(ps -p "$PID" -o args= 2>/dev/null || true)

        if [[ "$CMD" == *"src/simulator.py"* ]]; then
            echo "Stopping simulator (PID $PID)..."
            kill -TERM "$PID"

            for i in {1..10}; do
                kill -0 "$PID" 2>/dev/null || break
                sleep 1
            done

            if kill -0 "$PID" 2>/dev/null; then
                echo "WARNING: Simulator did not stop. Not forcing termination."
            fi
        else
            echo "WARNING: Simulator PID does not match. Leaving it untouched."
        fi
    fi

    if ! kill -0 "$PID" 2>/dev/null; then
        rm -f "$RUNTIME/simulator.pid"
    fi
else
    echo "No project-owned simulator PID recorded."
fi

# 2. Cancel only the Flink job ID recorded by our startup script.
if [[ -f "$RUNTIME/flink-job.id" ]]; then
    JOB_ID=$(cat "$RUNTIME/flink-job.id")

    echo "Cancelling project Flink job: $JOB_ID"
    "$FLINK/bin/flink" cancel "$JOB_ID"

    if [[ $? -eq 0 ]]; then
        rm -f "$RUNTIME/flink-job.id"
    else
        echo "ERROR: Could not cancel the recorded Flink job."
        echo "Leaving the Flink cluster running for safety."
        exit 1
    fi
else
    echo "No project-owned Flink JobID recorded."
fi

# 3. Stop Flink cluster only if our startup script started it.
if [[ -f "$RUNTIME/flink-cluster.started" ]]; then
    echo "Stopping project-owned Flink cluster..."
    "$FLINK/bin/stop-cluster.sh"

    if [[ $? -eq 0 ]]; then
        rm -f "$RUNTIME/flink-cluster.started"
    else
        echo "WARNING: Flink cluster stop reported an error."
    fi
else
    echo "No project-owned Flink cluster recorded. Leaving it running."
fi

# 4. Stop Kafka only if our startup script recorded its PID.
if [[ -f "$RUNTIME/kafka.pid" ]]; then
    PID=$(cat "$RUNTIME/kafka.pid")

    if kill -0 "$PID" 2>/dev/null; then
        CMD=$(tr '\0' ' ' < "/proc/$PID/cmdline" 2>/dev/null || true)

        if [[ "$CMD" == *"kafka.Kafka"* &&
              "$CMD" == *"server-wsl.properties"* ]]; then
            echo "Stopping project Kafka broker (PID $PID)..."
            kill -TERM "$PID"

            for i in {1..20}; do
                kill -0 "$PID" 2>/dev/null || break
                sleep 1
            done

            if kill -0 "$PID" 2>/dev/null; then
                echo "WARNING: Kafka is still running. Not forcing termination."
            fi
        else
            echo "WARNING: Kafka PID does not match this project's broker."
            echo "Leaving it untouched."
        fi
    fi

    if ! kill -0 "$PID" 2>/dev/null; then
        rm -f "$RUNTIME/kafka.pid"
    fi
else
    echo "No project-owned Kafka PID recorded. Leaving Kafka running."
fi

echo
echo "Shutdown procedure completed."
echo "Redis was not stopped."
echo "Kafka data and Redis data were not deleted."
