# Execution Instructions: Chicago Crime Analytics

This document outlines the correct, step-by-step instructions for running the complete Lambda Architecture pipeline. We have implemented a `Makefile` to fulfill the reproducibility requirement (8.2) and simplify execution.

## Preliminary Steps
Run the `download_data.py` script to download the datasets:
```bash
python3 download_data.py
```

## 1. Start the Infrastructure
Launch all required services (Zookeeper, Kafka, Spark, Storm, Postgres, Mongo, and the Runner) in the background.
```bash
make up
```
*Wait a minute or two for all services (especially Kafka and Zookeeper) to fully initialize.*

---

## 2. Run Batch Analytics (Spark)
Run the Apache Spark job to process historical data, compute crime trends, calculate arrest rates, and identify K-Means hotspots. This populates the PostgreSQL database.

In a new terminal, run:
```bash
make batch-analytics
```

---

## 3. Launch the Dashboard (Streamlit)
Start the Streamlit dashboard to visualize the batch analytics and prepare to receive real-time alerts.

In a new terminal, run:
```bash
make dashboard
```
*You can now open a web browser and navigate to `http://localhost:8501`.*

---

## 4. Start the Speed Layer (Storm Pipeline)
Start the real-time processing pipeline. This will consume messages from Kafka, calculate sliding windows, detect anomalies, and save alerts.

In a new terminal, run:
```bash
make stream-pipeline
```

---

## 5. Start the Kafka Producer
Finally, start simulating the real-time data stream by publishing the `crime.csv` dataset to the Kafka topic.

In a new terminal, run:
```bash
make start-producer
```

*Watch the Storm Pipeline terminal to see the throughput and anomalies being detected. Check your Streamlit dashboard to see the live alerts appear in real-time!*

---

## 6. Manual Shutdown
Once you are finished with the demo, you must safely shut everything down to stop all background services.

Press `Ctrl+C` in any open terminals running the dashboard, pipeline, or producer.
Then, from the project root directory, run:
```bash
make down
```
*(Alternatively, you can manually run `sudo docker-compose -f docker/docker-compose.yml down`)*

Your data will be safely persisted in the database volumes.
