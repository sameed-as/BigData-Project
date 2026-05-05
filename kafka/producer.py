import os
import json
import time
import csv
import yaml
from kafka import KafkaProducer

def load_config():
    config_path = os.path.join(os.path.dirname(__file__), '../config/config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def run_producer():
    config = load_config()
    broker = config['kafka']['broker']
    topic = config['kafka']['topic']
    rate = config['kafka']['publication_rate']
    dataset_path = os.path.join(os.path.dirname(__file__), '..', config['kafka']['dataset_path'].lstrip('/'))

    print(f"Connecting to Kafka at {broker}")
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print(f"Streaming {dataset_path} to topic {topic} at {rate} msg/sec")
    
    with open(dataset_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                # Construct the payload
                payload = {
                    "CASE NUMBER": row.get("Case Number") or row.get("CASE NUMBER"),
                    "DATE":         row.get("Date")         or row.get("DATE"),
                    "BLOCK":        row.get("Block")        or row.get("BLOCK"),
                    "PRIMARY TYPE": row.get("Primary Type") or row.get("PRIMARY TYPE"),
                    "DISTRICT":     row.get("District")     or row.get("DISTRICT"),
                    "ARREST":       row.get("Arrest")       or row.get("ARREST"),
                    "LATITUDE":     row.get("Latitude")     or row.get("LATITUDE"),
                    "LONGITUDE":    row.get("Longitude")    or row.get("LONGITUDE"),
                }
                
                # Send to Kafka
                producer.send(topic, payload)
                
                # Sleep to match publication rate
                time.sleep(1.0 / rate)
            except Exception as e:
                print(f"Error processing row, skipping: {e}")
                continue

if __name__ == "__main__":
    try:
        run_producer()
    except Exception as e:
        print(f"Producer failed: {e}")
