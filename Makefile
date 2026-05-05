.PHONY: help up down batch-analytics stream-pipeline start-producer dashboard clean

help:
	@echo "Available targets:"
	@echo "  make up               - Start all infrastructure containers (Kafka, Zookeeper, Spark, Storm, DBs)"
	@echo "  make down             - Stop and remove all containers"
	@echo "  make batch-analytics  - Run the Spark batch analytics job"
	@echo "  make stream-pipeline  - Run the Storm streaming pipeline"
	@echo "  make start-producer   - Start the Kafka producer to simulate real-time data"
	@echo "  make dashboard        - Start the Streamlit dashboard"
	@echo "  make clean            - Remove dangling docker images and stopped containers safely"
	@echo "  make install-libs     - Install required libs"

up:
	sudo docker-compose -f docker/docker-compose.yml up -d

down:
	sudo docker-compose -f docker/docker-compose.yml down

batch-analytics:
	sudo docker exec -it spark /opt/spark/bin/spark-submit --driver-memory 4g --executor-memory 4g --packages org.postgresql:postgresql:42.5.4 /opt/spark-apps/analytics.py

stream-pipeline:
	sudo docker exec -it -e IN_DOCKER=1 runner python3 /app/storm/run_pipeline.py

start-producer:
	sudo docker exec -it runner python3 /app/kafka/producer.py

dashboard:
	sudo docker exec -it -e IN_DOCKER=1 runner streamlit run /app/dashboard/app.py --server.port 8501 --server.address 0.0.0.0

install-libs:
	sudo docker exec -it spark pip install numpy

clean:
	sudo docker system prune -f
