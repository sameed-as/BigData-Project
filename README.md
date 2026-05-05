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
We provide a `Makefile` to simplify starting and stopping the entire pipeline, ensuring reproducibility.

1. **Start Infrastructure:**
   ```bash
   make up
   ```
2. **Run Batch Analytics (Spark):**
   ```bash
   make batch-analytics
   ```
3. **Start Real-Time Topology (Storm Speed Layer):**
   ```bash
   make stream-pipeline
   ```
4. **Start Kafka Producer (Data Simulation):**
   ```bash
   make start-producer
   ```
5. **Launch Dashboard:**
   ```bash
   make dashboard
   ```
6. **Shut Down Services:**
   ```bash
   make down
   ```

## Contributors
1. sameed - Batch Analytics & Infrastructure (50%)
2. junaidzeb - Real-Time Streaming & Dashboard (50%)
