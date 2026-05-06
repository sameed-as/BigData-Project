# Real-Time Crime Analytics & Intelligent Alert System

This project implements a Lambda Architecture for processing and analyzing public safety datasets from the City of Chicago. It features a robust batch processing pipeline using Apache Spark, a real-time event streaming layer using Apache Kafka and Apache Storm, and a Streamlit dashboard powered by PostgreSQL and MongoDB.

## Architecture
- **Batch Layer (Apache Spark):** Ingests historical data, cleanses schemas, runs K-Means clustering for geospatial hotspot detection, and computes arrest rates and crime trends. Results are persisted to PostgreSQL.
- **Speed Layer (Kafka + Storm):** 
  - `Kafka Producer` simulates real-time ingestion by streaming the crime dataset row-by-row.
  - `Storm Topology` uses a sliding window (WindowBolt) to aggregate events by police district and detects anomalies (AnomalyBolt) against configured baselines.
  - `AlertBolt` persists triggers to MongoDB (raw JSON) and PostgreSQL (structured).
- **Serving Layer (PostgreSQL + MongoDB):** Stores all analytics results and alerts.
- **Dashboard (Streamlit):** Visualizes live alerts, crime trends, and crime hotspots.

## Project Structure
- `config/` - Externalized configuration (`config.yaml`)
- `data/` - Raw Datasets
- `db/` - Database Initialization Scripts (`postgres_init.sql`, `mongo_setup.py`)
- `docker/` - `docker-compose.yml` and `Dockerfile` for the runner
- `kafka/` - `producer.py` to stream crime events
- `spark/` - `preprocessing.py` and batch analytics jobs
- `storm/` - Python Storm Topology (`streamparse`) with Spouts and Bolts
- `dashboard/` - `app.py` for Streamlit visualization

## Execution
We provide automation scripts to simplify starting and stopping the entire pipeline, ensuring reproducibility on both Linux and Windows.

**For Linux/Mac (using `make`):**
```bash
make up
make batch-analytics
make stream-pipeline
make dashboard
make start-producer
```

**For Windows (using PowerShell):**
```powershell
.\run.ps1 up
.\run.ps1 batch-analytics
.\run.ps1 stream-pipeline
.\run.ps1 dashboard
.\run.ps1 start-producer
```

For complete, detailed instructions on how to use these scripts step-by-step, please see the [Execution Instructions](EXECUTION_INSTRUCTIONS.md) document.

## Contributors
1. sameed - Batch Analytics & Infrastructure (50%)
2. junaidzeb - Real-Time Streaming & Dashboard (50%)
