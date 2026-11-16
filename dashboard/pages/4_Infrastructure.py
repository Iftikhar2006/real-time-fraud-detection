import json
import shutil
import socket
import subprocess
import time

import psutil
from pathlib import Path
from urllib.request import urlopen
from urllib.error import URLError

import redis
import streamlit as st

from utils.database import get_mongo_client, DATABASES


# --------------------------------------------------
# PAGE HEADER
# --------------------------------------------------

st.title("🖥️ Infrastructure")
st.caption(
    "Service health, runtime processes, and system resources "
    "for the fraud detection platform."
)

if st.button("🔄 Refresh infrastructure status"):
    st.rerun()


# --------------------------------------------------
# LIGHTWEIGHT SERVICE CHECKS
# --------------------------------------------------

def check_tcp(host, port, timeout=1.5):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, f"{host}:{port} is reachable"
    except OSError as exc:
        return False, str(exc)


def check_mongodb():
    try:
        client = get_mongo_client()
        client.admin.command("ping")
        return True, "MongoDB responded to ping"
    except Exception as exc:
        return False, str(exc)


def check_redis():
    try:
        client = redis.Redis(
            host="localhost",
            port=6379,
            socket_connect_timeout=1.5,
            socket_timeout=1.5,
            decode_responses=True,
        )
        response = client.ping()
        client.close()
        return bool(response), "Redis responded to PING"
    except Exception as exc:
        return False, str(exc)


def check_flink_api():
    try:
        with urlopen(
            "http://localhost:8081/overview",
            timeout=2,
        ) as response:
            data = json.loads(response.read().decode("utf-8"))
        return True, data
    except Exception as exc:
        return False, str(exc)


# --------------------------------------------------
# SERVICE STATUS
# --------------------------------------------------

st.header("1. Service Health")
st.caption("Connectivity checks use short timeouts and do not start services.")

kafka_ok, kafka_detail = check_tcp("localhost", 9092)
flink_ok, flink_data = check_flink_api()
mongo_ok, mongo_detail = check_mongodb()
redis_ok, redis_detail = check_redis()

services = [
    ("Kafka Broker", kafka_ok, "localhost:9092", kafka_detail),
    ("Flink REST API", flink_ok, "localhost:8081",
     "REST API reachable" if flink_ok else flink_data),
    ("MongoDB", mongo_ok, "localhost:27017", mongo_detail),
    ("Redis", redis_ok, "localhost:6379", redis_detail),
]

cols = st.columns(4)

for col, (name, online, endpoint, detail) in zip(cols, services):
    with col:
        st.metric(name, "ONLINE" if online else "OFFLINE")
        st.caption(endpoint)
        st.caption(detail if online else "Service unavailable")


# --------------------------------------------------
# REDIS RUNTIME METRICS
# --------------------------------------------------

st.divider()
st.header("2. Redis Runtime Metrics")

if redis_ok:
    try:
        redis_client = redis.Redis(
            host="localhost",
            port=6379,
            socket_connect_timeout=1.5,
            socket_timeout=1.5,
            decode_responses=True,
        )

        server = redis_client.info("server")
        memory_info = redis_client.info("memory")
        stats = redis_client.info("stats")
        clients = redis_client.info("clients")

        total_keys = redis_client.dbsize()

        hits = stats.get("keyspace_hits", 0)
        misses = stats.get("keyspace_misses", 0)
        hit_ratio = (
            hits / (hits + misses) * 100
            if hits + misses > 0 else 0
        )

        redis_cols = st.columns(4)

        redis_cols[0].metric(
            "Redis Version",
            server.get("redis_version", "N/A"),
        )
        redis_cols[1].metric(
            "Uptime",
            f"{server.get('uptime_in_days', 0)} days",
        )
        redis_cols[2].metric(
            "Connected Clients",
            clients.get("connected_clients", 0),
        )
        redis_cols[3].metric(
            "Keys in Current DB",
            total_keys,
        )

        st.subheader("Redis Memory")

        memory_cols = st.columns(3)

        memory_cols[0].metric(
            "Memory Used",
            f"{memory_info.get('used_memory', 0) / (1024 ** 2):.2f} MB",
        )
        memory_cols[1].metric(
            "Peak Memory",
            f"{memory_info.get('used_memory_peak', 0) / (1024 ** 2):.2f} MB",
        )
        memory_cols[2].metric(
            "Memory Usage",
            memory_info.get("used_memory_human", "N/A"),
        )

        st.subheader("Redis Activity")

        activity_cols = st.columns(3)

        activity_cols[0].metric(
            "Operations / Second",
            stats.get("instantaneous_ops_per_sec", 0),
        )
        activity_cols[1].metric(
            "Total Commands Processed",
            f"{stats.get('total_commands_processed', 0):,}",
        )
        activity_cols[2].metric(
            "Cache Hit Ratio",
            f"{hit_ratio:.1f}%",
        )

        st.caption(
            "Metrics are read directly from Redis INFO and DBSIZE. "
            "Key count refers to the currently selected Redis database."
        )

        redis_client.close()

    except Exception as exc:
        st.warning(f"Could not retrieve Redis metrics: {exc}")
else:
    st.info("Redis is offline. Runtime metrics are unavailable.")


# --------------------------------------------------
# FLINK JOB DETAILS
# --------------------------------------------------

st.divider()
st.header("3. Flink Cluster and Job")

if flink_ok:
    overview_cols = st.columns(3)

    overview_cols[0].metric(
        "Task Managers",
        flink_data.get("taskmanagers", "N/A"),
    )
    overview_cols[1].metric(
        "Total Slots",
        flink_data.get("slots-total", "N/A"),
    )
    overview_cols[2].metric(
        "Available Slots",
        flink_data.get("slots-available", "N/A"),
    )

    try:
        with urlopen(
            "http://localhost:8081/jobs/overview",
            timeout=2,
        ) as response:
            jobs_data = json.loads(response.read().decode("utf-8"))

        jobs = jobs_data.get("jobs", [])

        if jobs:
            st.write("Flink jobs")
            st.dataframe(
                [
                    {
                        "Job ID": job.get("jid", ""),
                        "Job Name": job.get("name", ""),
                        "Status": job.get("state", ""),
                    }
                    for job in jobs
                ],
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("Flink is reachable, but no jobs are listed.")

    except Exception as exc:
        st.warning(f"Could not retrieve Flink job details: {exc}")
else:
    st.info("Flink is offline. Job details are unavailable.")


# --------------------------------------------------
# DATABASE ENVIRONMENTS
# --------------------------------------------------

st.divider()
st.header("4. MongoDB Environments")

st.write(
    "The project keeps historical records and live demo records "
    "in separate databases."
)

database_cols = st.columns(2)

for col, (environment, database_name) in zip(
    database_cols,
    DATABASES.items(),
):
    with col:
        st.subheader(
            "Historical Database" if environment == "historical"
            else "Live Demo Database"
        )
        st.code(database_name)
        st.caption(
            "Historical IEEE-CIS project records"
            if environment == "historical"
            else "Isolated database for new demo transactions"
        )

if mongo_ok:
    st.success("MongoDB connection is healthy.")
else:
    st.error("MongoDB connection is unavailable.")


# --------------------------------------------------
# SYSTEM RESOURCES
# --------------------------------------------------

st.divider()
st.header("5. System Performance")

# Sample overall CPU utilisation over a short interval.
try:
    cpu_usage = psutil.cpu_percent(interval=0.2)
    cpu_count = psutil.cpu_count(logical=True) or 1

    cpu_cols = st.columns(2)
    cpu_cols[0].metric("CPU Utilisation", f"{cpu_usage:.1f}%")
    cpu_cols[1].metric("Logical CPU Cores", cpu_count)

    st.progress(
        min(cpu_usage / 100, 1.0),
        text=f"CPU utilisation: {cpu_usage:.1f}%",
    )
except Exception as exc:
    st.warning(f"CPU information unavailable: {exc}")


# Read memory figures directly from Linux procfs.
memory = {}

try:
    with open("/proc/meminfo", "r", encoding="utf-8") as file:
        for line in file:
            key, value = line.split(":", 1)
            memory[key] = int(value.strip().split()[0])  # kB

    total = memory.get("MemTotal", 0)
    available = memory.get("MemAvailable", 0)
    used = total - available

    swap_total = memory.get("SwapTotal", 0)
    swap_free = memory.get("SwapFree", 0)
    swap_used = swap_total - swap_free

    mem_cols = st.columns(3)
    mem_cols[0].metric("Memory Total", f"{total / 1024:.0f} MB")
    mem_cols[1].metric("Memory Used", f"{used / 1024:.0f} MB")
    mem_cols[2].metric(
        "Memory Available",
        f"{available / 1024:.0f} MB",
    )

    if total:
        st.progress(
            min(used / total, 1.0),
            text=f"Memory utilisation: {used / total:.1%}",
        )

    st.caption(
        f"Swap: {swap_used / 1024:.0f} MB used / "
        f"{swap_total / 1024:.0f} MB total"
    )

except Exception as exc:
    st.warning(f"Memory information unavailable: {exc}")


# Disk usage for WSL and the mounted project drive.
disk_locations = [
    ("WSL filesystem (/)", "/"),
    ("Windows D: drive (/mnt/d)", "/mnt/d"),
]

disk_cols = st.columns(len(disk_locations))

for col, (label, path) in zip(disk_cols, disk_locations):
    with col:
        if Path(path).exists():
            usage = shutil.disk_usage(path)
            used_percent = usage.used / usage.total if usage.total else 0

            st.metric(
                label,
                f"{usage.free / (1024 ** 3):.1f} GB free",
            )
            st.caption(
                f"{usage.used / (1024 ** 3):.1f} GB used / "
                f"{usage.total / (1024 ** 3):.1f} GB total"
            )
            st.progress(
                min(used_percent, 1.0),
                text=f"Disk utilisation: {used_percent:.1%}",
            )
        else:
            st.caption(f"{path} is not mounted.")


# --------------------------------------------------
# PROCESS STATUS
# --------------------------------------------------

st.divider()
st.header("6. Runtime Processes")

process_patterns = {
    "Kafka": "kafka.Kafka",
    "Flink": "org.apache.flink",
    "Streamlit Dashboard": "streamlit run dashboard/app.py",
}

for label, pattern in process_patterns.items():
    try:
        result = subprocess.run(
            ["pgrep", "-af", pattern],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )

        lines = [
            line for line in result.stdout.splitlines()
            if line.strip()
        ]

        if lines:
            st.write(f"🟢 **{label}:** Running")
            for line in lines[:3]:
                st.code(line[:220])
        else:
            st.write(f"⚪ **{label}:** No matching process found")

    except Exception as exc:
        st.write(f"⚪ **{label}:** Process check unavailable ({exc})")


st.subheader("Process Resource Usage")
st.caption(
    "CPU and resident memory sampled for the platform's detected processes."
)

process_patterns = {
    "Kafka": ("kafka.Kafka",),
    "Flink": ("org.apache.flink",),
    "Streamlit Dashboard": ("streamlit run dashboard/app.py",),
    "MongoDB": ("mongod",),
    "Redis": ("redis-server",),
}

try:
    matched_processes = []

    for process in psutil.process_iter(
        ["pid", "name", "cmdline", "memory_info"]
    ):
        try:
            info = process.info
            command = " ".join(info.get("cmdline") or [])
            process_name = info.get("name") or ""
            searchable = f"{process_name} {command}".lower()

            for label, patterns in process_patterns.items():
                if any(pattern.lower() in searchable for pattern in patterns):
                    process.cpu_percent(interval=None)
                    matched_processes.append((label, process))
                    break

        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    # Allow a short sampling interval for per-process CPU measurements.
    time.sleep(0.2)

    process_rows = []

    for label, process in matched_processes:
        try:
            with process.oneshot():
                memory_mb = process.memory_info().rss / (1024 ** 2)
                cpu_percent = process.cpu_percent(interval=None)

            process_rows.append({
                "Service": label,
                "PID": process.pid,
                "CPU Usage (%)": round(cpu_percent, 1),
                "RAM Usage (MB)": round(memory_mb, 1),
            })

        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    if process_rows:
        st.dataframe(
            process_rows,
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No matching platform processes are currently running.")

except Exception as exc:
    st.warning(f"Process resource information unavailable: {exc}")


# --------------------------------------------------
# FOOTER
# --------------------------------------------------

st.divider()
st.caption(
    "Infrastructure checks are read-only. This page does not start, "
    "stop, reset, or modify Kafka, Flink, MongoDB, or Redis."
)
