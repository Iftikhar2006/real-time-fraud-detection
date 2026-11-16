# Real-Time Financial Transaction Processing and Fraud Detection

A distributed financial transaction processing pipeline combining Apache Kafka,
Apache Flink, behavioural feature engineering, LightGBM fraud scoring, risk
classification, and persistent storage. A Streamlit dashboard provides
historical analysis, live monitoring, fraud alerts, and infrastructure metrics.

## 1. Project Objective

Financial institutions process large volumes of transactions. Detecting
suspicious activity requires more than checking individual transaction
amounts. Changes in spending behaviour, transaction frequency, timing, and
historical patterns can also indicate potentially fraudulent activity.

This project implements a streaming architecture that processes transaction
events, derives behavioural signals, applies machine-learning fraud scoring,
classifies risk, and stores transaction and alert information.

## 2. System Architecture

```text
                 Transaction Events
                         |
                         v
                  Apache Kafka
                         |
                         v
                   Apache Flink
                         |
              +----------+----------+
              |          |          |
              v          v          v
          Behaviour   LightGBM    Risk Engine
          Features    Scoring    & Classification
              |          |          |
              +----------+----------+
                         |
              +----------+----------+
              |                     |
              v                     v
           MongoDB                Redis
       Persistent Records    Low-Latency State
              |                     |
              +----------+----------+
                         |
                         v
                 Streamlit Dashboard
```

### Processing components

- **Kafka:** receives transaction events through a dedicated demo topic.
- **Flink:** consumes and processes the transaction stream.
- **Behaviour Engine:** derives transaction-history and behavioural features
  using Redis state.
- **LightGBM:** produces a fraud probability using the trained model.
- **Risk Engine:** combines model results and behavioural flags to produce
  risk information and review recommendations.
- **MongoDB:** persists transaction records and active fraud alerts.
- **Redis:** maintains low-latency behavioural state and runtime information.
- **Streamlit:** presents historical analysis, demo activity, alerts, and
  infrastructure information.

## 3. Dataset and Model

Historical model development and evaluation use the IEEE-CIS Fraud Detection
dataset. Transaction and identity data are integrated using `TransactionID`.
The target variable is `isFraud`.

The trained LightGBM classifier uses 423 engineered features, including
35 categorical features.

### Historical validation results

| Metric | Result |
|---|---:|
| Validation PR-AUC | 0.877466 |
| Best iteration | 1,999 |
| Classification threshold | 0.50 |
| Training/validation split | 80:20 |
| Split strategy | Stratified |

These are historical model-development results, not live demo metrics.

### Previously measured pipeline benchmarks

| Target rate | Observed rate | Average latency | P95 latency |
|---:|---:|---:|---:|
| 10 transactions/s | 9.57 transactions/s | 1.126 s | 1.953 s |
| 20 transactions/s | 17.80 transactions/s | 1.233 s | 1.976 s |
| 50 transactions/s | 35.34 transactions/s | 2.108 s | 3.215 s |

These measurements represent previously observed persistence-window
throughput, not maximum sustainable system capacity.

The reported inference speedup is 40.23x.

## 4. Storage Environments

The project separates historical analytical data from fresh demo activity.

| Environment | MongoDB database | Redis namespace | Purpose |
|---|---|---|---|
| Historical | `fraud_detection` | `fraud:ieee:` | Existing IEEE-CIS processing and analysis |
| Demo | `fraud_detection_demo` | `fraud:demo:` | Fresh transaction processing and live monitoring |

Dashboard pages use explicit database environments:

- Live Monitor and Fraud Alerts read from the demo database.
- Historical Analytics reads from the historical database.
- Infrastructure displays service health and runtime information.

Starting the demo does not reset or overwrite the historical database.

Redis state is maintained independently for the two namespaces. Do not use
`FLUSHDB` or `FLUSHALL` as a project cleanup operation.

## 5. Dashboard

The Streamlit dashboard contains five pages:

1. **Project Overview** — objective, architecture, technology stack, model
   evaluation, and previously measured benchmarks.
2. **Live Monitor** — transactions processed in the isolated demo environment.
3. **Fraud Alerts** — active demo risk alerts and their details.
4. **Historical Analytics** — historical transaction and fraud analysis.
5. **Infrastructure** — Kafka, Flink, MongoDB, Redis, WSL resources, and
   process-level performance information.

Run the dashboard from the project directory:

```bash
source ~/pyflink22-env/bin/activate
cd ~/fraud-detection

streamlit run dashboard/app.py --server.address 0.0.0.0 --server.port 8501
```

Open `http://localhost:8501` in your browser.

## 6. Technology Stack

| Component | Technology |
|---|---|
| Programming language | Python |
| Streaming platform | Apache Kafka 4.3.1 |
| Stream processing | Apache Flink 2.2.0 |
| Machine learning | LightGBM |
| Historical and demo storage | MongoDB 8 |
| Low-latency state | Redis 7 |
| Dashboard | Streamlit |
| Environment | WSL Ubuntu 24.04 |

Python package versions are pinned in `requirements.txt`.

## 7. Setup

The documented local environment uses WSL Ubuntu, Python 3.12, Java 17,
Kafka 4.3.1, Flink 2.2.0, MongoDB 8, and Redis 7.

### Python environment

Activate the existing environment:

```bash
source ~/pyflink22-env/bin/activate
cd ~/fraud-detection
```

Install dependencies:
`pip install -r requirements.txt`

### External services

The local setup uses Kafka 4.3.1, Flink 2.2.0, MongoDB 8, and Redis 7.
Ensure MongoDB and Redis are running before starting the demo.

### Demo configuration

If `.env.demo` does not exist, create it with `cp .env.example .env.demo`.
Adjust the checkpoint path if your WSL username differs. Do not commit `.env.demo`.

## 8. Running the Demo

Start the isolated demo pipeline with `./scripts/start_demo.sh`.
The launcher checks dependencies, starts Kafka and Flink, creates the demo topic,
and submits the processing job. It does not start the transaction simulator.

Stop the demo with `./scripts/stop_demo.sh`.
MongoDB, Redis, Kafka data, and Flink checkpoints are preserved.

## 9. Demo Reset

`./scripts/reset_demo.sh` is destructive. It clears demo topic data, demo MongoDB
collections, and Redis keys under the demo namespace after explicit confirmation.
It must not be used to clear historical data.

## 10. Dashboard

Run the dashboard independently with:
`streamlit run dashboard/app.py --server.address 0.0.0.0 --server.port 8501`
Open `http://localhost:8501` in your browser.

The dashboard contains Project Overview, Live Monitor, Fraud Alerts,
Historical Analytics, and Infrastructure.

## 11. Operational Notes

- Use `start_demo.sh` and `stop_demo.sh` for the isolated demo pipeline.
- `start_project.sh` is a legacy launcher.
- Historical and demo databases and Redis namespaces must remain isolated.
- The dashboard can run independently of the Kafka and Flink pipeline.
- The simulator is intentionally deferred until the final testing stage.
