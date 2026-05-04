# Execution Instructions: Chicago Crime Analytics

This document outlines the correct, step-by-step instructions for running the complete Lambda Architecture pipeline.

## Premilinary Steps
run the download_data.py script to download the data
```bash
python3 download_data.py
```


## 1. Start the Infrastructure
First, launch all the required services (Zookeeper, Kafka, Spark, Storm, Postgres, Mongo, and the Runner container) in the background.

From the project root directory, run:
```bash
sudo docker-compose -f docker/docker-compose.yml up -d
```

*Wait a minute or two for all services (especially Kafka and Zookeeper) to fully initialize.*

---

## 2. Run Batch Analytics (Spark)
Run the Apache Spark job to process historical data, compute crime trends, calculate arrest rates, and identify K-Means hotspots. This populates the PostgreSQL database.

In a new terminal, run:
```bash
sudo docker exec -it spark /opt/spark/bin/spark-submit \
  --packages org.postgresql:postgresql:42.5.4 \
  /opt/spark-apps/analytics.py
```

---

## 3. Launch the Dashboard (Streamlit)
Start the Streamlit dashboard to visualize the batch analytics and prepare to receive real-time alerts.

In a new terminal, run:
```bash
sudo docker exec -it -e IN_DOCKER=1 runner streamlit run /app/dashboard/app.py --server.port 8501 --server.address 0.0.0.0
```
*You can now open a web browser and navigate to `http://localhost:8501`.*

---

## 4. Start the Speed Layer (Storm Pipeline)
Start the real-time processing pipeline. This will consume messages from Kafka, calculate 5-minute sliding windows, detect anomalies, and save alerts to both MongoDB and PostgreSQL.

In a new terminal, run:
```bash
sudo docker exec -it -e IN_DOCKER=1 runner python3 /app/storm/run_pipeline.py
```

---

## 5. Start the Kafka Producer
Finally, start simulating the real-time data stream by publishing the `crime.csv` dataset to the Kafka topic.

In a new terminal, run:
```bash
sudo docker exec -it runner python3 /app/kafka/producer.py
```

*Watch the Storm Pipeline terminal to see the throughput and anomalies being detected. Check your Streamlit dashboard to see the live alerts appear in real-time!*

---

## 6. Clean Shutdown
Once you are finished with the demo, you can safely shut everything down.

1. Press `Ctrl+C` in all open terminals running the dashboard, pipeline, and producer.
2. Run the following command to stop and remove the Docker containers:
```bash
sudo docker-compose -f docker/docker-compose.yml down
```
*(Your data will be safely persisted in the database volumes).*
