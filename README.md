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
1. **Start Infrastructure:**
   ```bash
   cd docker
   docker-compose up -d
   ```
2. **Run Batch Analytics (Spark):**
   ```bash
   docker exec -it spark spark-submit /opt/spark-apps/preprocessing.py
   ```
3. **Start Real-Time Topology (Storm):**
   ```bash
   docker exec -it runner sparse submit -n nimbus
   ```
4. **Start Kafka Producer:**
   ```bash
   docker exec -it runner python /app/kafka/producer.py
   ```
5. **Launch Dashboard:**
   ```bash
   docker exec -it runner streamlit run /app/dashboard/app.py
   ```

## Contributors
1. sameed - Batch Analytics & Infrastructure (50%)
2. junaidzeb - Real-Time Streaming & Dashboard (50%)
