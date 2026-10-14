# Realtime Data Pipeline

<p align="center">
  <strong>High-Throughput Streaming Lakehouse with Apache Kafka, Apache Spark Structured Streaming & Medallion Architecture</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Orchestration-Docker_Compose-18181B?style=for-the-badge&logo=docker&logoColor=white" alt="Docker" />
  <img src="https://img.shields.io/badge/Message_Broker-Apache_Kafka-18181B?style=for-the-badge&logo=apachekafka&logoColor=white" alt="Kafka" />
  <img src="https://img.shields.io/badge/Processing_Engine-Apache_Spark_Streaming-18181B?style=for-the-badge&logo=apachespark&logoColor=white" alt="Spark" />
  <img src="https://img.shields.io/badge/Architecture-Medallion_(Bronze_/_Silver_/_Gold)-18181B?style=for-the-badge" alt="Medallion Architecture" />
  <img src="https://img.shields.io/badge/License-MIT-18181B?style=for-the-badge" alt="License" />
</p>

---

## Overview

**Realtime Data Pipeline** is an enterprise-grade distributed streaming data lakehouse. It continuously ingests real-time developer activity events from the GitHub REST stream into an Apache Kafka cluster, executes real-time schema enforcement, validation, and transformations using Apache Spark Structured Streaming, and persists partitioned datasets across the **Medallion Architecture (Bronze &rarr; Silver &rarr; Gold)**.

---

## Tech Stack

<p align="center">
  <img src="https://img.shields.io/badge/Apache_Kafka-18181B?style=for-the-badge&logo=apachekafka&logoColor=white" alt="Kafka" />
  <img src="https://img.shields.io/badge/Apache_Spark-18181B?style=for-the-badge&logo=apachespark&logoColor=white" alt="Spark" />
  <img src="https://img.shields.io/badge/Docker-18181B?style=for-the-badge&logo=docker&logoColor=white" alt="Docker" />
  <img src="https://img.shields.io/badge/Python-18181B?style=for-the-badge&logo=python&logoColor=white" alt="Python" />
  <img src="https://img.shields.io/badge/Apache_Zookeeper-18181B?style=for-the-badge&logo=apache&logoColor=white" alt="Zookeeper" />
</p>

---

## Medallion Architecture Pipeline

```
GitHub Event Stream / Producer (gh_producer.py)
                    │
                    ▼
       [Apache Kafka Broker: 9092]
  (Topic: github-events, Zookeeper-coordinated)
                    │
                    ▼
        [Spark Structured Streaming]
                    │
    ┌───────────────┼───────────────┐
    ▼               ▼               ▼
[Bronze Layer]   [Silver Layer]  [Gold Layer]
Raw Immutable    Validated,      Aggregated
JSON Events      Deduplicated &  Business Metrics
                 Enriched        & KPIs
```

### 1. Bronze Layer (`bronze_layer.py`)
* Consumes raw payloads directly from Kafka partition offsets.
* Preserves raw audit trails with event ingestion timestamps and payload strings in immutable storage.

### 2. Silver Layer (`silver_layer.py`)
* Applies strict schema enforcement, null checks, and event-time watermark definitions.
* Deserializes nested JSON structures, standardizes actor/repo metadata, and drops corrupted packets.

### 3. Gold Layer (`gold_layer.py`)
* Computes tumbling and sliding window analytics (e.g., event velocity, top active repositories, commit spikes).
* Outputs ready-to-query analytical marts for downstream BI tools and dashboard engines.

---

## Repository Structure

```
realtime-data-pipeline/
├── checkpoints/             # Spark structured streaming fault-tolerance checkpoints
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── data_lake/               # Distributed data lakehouse partition directory
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── producer/
│   └── gh_producer.py       # High-throughput GitHub streaming event producer
├── spark_jobs/
│   ├── bronze_layer.py      # Kafka consumer and Bronze ingestion job
│   ├── silver_layer.py      # Schema parser, deduplicator and Silver job
│   └── gold_layer.py        # Windowed aggregation and Gold analytical job
├── docker-compose.yml       # Zookeeper, Kafka, and Spark container orchestration
├── Dockerfile               # Custom Spark container image with Kafka connectors
├── requirements.txt         # Producer and pipeline Python dependencies
└── README.md
```

---

## Quickstart

### 1. Spin up Distributed Infrastructure
Launch Kafka, Zookeeper, and Apache Spark using Docker Compose:
```bash
git clone https://github.com/prannesshkva/realtime-data-pipeline.git
cd realtime-data-pipeline

docker-compose up -d
```

Verify running containers:
```bash
docker-compose ps
```

### 2. Start the Event Producer
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt

python producer/gh_producer.py
```

### 3. Execute Spark Streaming Jobs
Submit the Medallion transformation jobs inside the Spark container:
```bash
# Ingest raw stream into Bronze
docker exec -it spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /app/spark_jobs/bronze_layer.py

# Transform and clean into Silver
docker exec -it spark spark-submit /app/spark_jobs/silver_layer.py

# Compute KPIs into Gold
docker exec -it spark spark-submit /app/spark_jobs/gold_layer.py
```

---

## Author

* **Architect**: [Prannessh KVA](https://github.com/prannesshkva)
* **Focus**: Distributed Systems, Streaming Lakehouses, and Big Data Engineering

---

## License

This project is licensed under the MIT License.
