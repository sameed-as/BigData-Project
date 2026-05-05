"""
Standalone Crime Analytics Speed Layer Pipeline
Runs the same processing logic as the Storm topology but without streamparse/thriftpy.
Pipeline: Kafka -> Parse -> District -> Window -> Anomaly -> Alert (MongoDB + PostgreSQL)
"""

import json
import time
import yaml
import os
import datetime
import logging
import threading
from collections import defaultdict
from queue import Queue, Empty

from kafka import KafkaConsumer
import pymongo
import psycopg2

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger("pipeline")

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
config_path = '/app/config/config.yaml'
if not os.path.exists(config_path):
    config_path = os.path.join(os.path.dirname(__file__), '../config/config.yaml')

with open(config_path) as f:
    cfg = yaml.safe_load(f)

KAFKA_BROKER  = cfg['kafka']['broker']
KAFKA_TOPIC   = cfg['kafka']['topic']
WINDOW_SECS   = cfg['storm']['window_size_minutes'] * 60
THRESHOLD     = cfg['storm']['anomaly_threshold']

PG_HOST = cfg['postgres']['host']
PG_PORT = cfg['postgres']['port']
PG_DB   = cfg['postgres']['db']
PG_USER = cfg['postgres']['user']
PG_PASS = cfg['postgres']['password']

MONGO_HOST = cfg['mongo']['host']
MONGO_PORT = cfg['mongo']['port']
MONGO_DB   = cfg['mongo']['db']
MONGO_COL  = cfg['mongo']['alerts_collection']

# --------------------------------------------------------------------------- #
# Counters for monitoring
# --------------------------------------------------------------------------- #
msg_count = {'received': 0, 'parsed': 0, 'anomalies': 0}

# --------------------------------------------------------------------------- #
q_raw      = Queue(maxsize=5000)   # Kafka -> Parse
q_parsed   = Queue(maxsize=5000)   # Parse -> District
q_district = Queue(maxsize=5000)   # District -> Window
q_windowed = Queue(maxsize=5000)   # Window -> Anomaly
q_alerts   = Queue(maxsize=1000)   # Anomaly -> Alert

# --------------------------------------------------------------------------- #
# Stage 1 – Kafka Consumer (Spout)
# --------------------------------------------------------------------------- #
def kafka_spout():
    log.info(f"Connecting to Kafka at {KAFKA_BROKER} topic={KAFKA_TOPIC}")
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='crime_pipeline_v3',
            consumer_timeout_ms=-1
        )
        log.info("Kafka consumer connected successfully")
        while True:
            msg_pack = consumer.poll(timeout_ms=500)
            for tp, messages in msg_pack.items():
                for message in messages:
                    msg_count['received'] += 1
                    q_raw.put(json.dumps(message.value))
    except Exception as e:
        log.error(f"KAFKA SPOUT CRASHED: {e}", exc_info=True)

# --------------------------------------------------------------------------- #
# Stage 2 – Parse Bolt
# --------------------------------------------------------------------------- #
def parse_bolt():
    first = True
    while True:
        raw = q_raw.get()
        try:
            record = json.loads(raw)
            if first:
                log.info(f"SAMPLE RECORD keys: {list(record.keys())}, values sample: {dict(list(record.items())[:3])}")
                first = False
            district = str(record.get('DISTRICT') or record.get('District') or record.get('district') or 'UNKNOWN')
            record['DISTRICT'] = district
            msg_count['parsed'] += 1
            q_parsed.put(record)
        except Exception as e:
            log.warning(f"Parse error: {e}")

# --------------------------------------------------------------------------- #
# Stage 3 – District Bolt (groups and tags by district)
# --------------------------------------------------------------------------- #
def district_bolt():
    while True:
        record = q_parsed.get()
        district = record.get('DISTRICT', 'UNKNOWN')
        # Emits district-tagged tuples
        q_district.put({'district': district, 'record': record})

# --------------------------------------------------------------------------- #
# Stage 4 – Window Bolt (sliding window counter per district)
# --------------------------------------------------------------------------- #
def window_bolt():
    counts = defaultdict(list)
    while True:
        item = q_district.get()
        district = item['district']
        now = time.time()
        counts[district].append(now)
        # Evict events outside the window
        counts[district] = [t for t in counts[district] if now - t <= WINDOW_SECS]
        q_windowed.put({'district': district, 'count': len(counts[district]), 'ts': now})

# --------------------------------------------------------------------------- #
# Stage 5 – Anomaly Bolt
# --------------------------------------------------------------------------- #
def anomaly_bolt():
    last_alert = defaultdict(lambda: 0)
    while True:
        item = q_windowed.get()
        district = item['district']
        count    = item['count']
        ts       = item['ts']
        if count > THRESHOLD and (ts - last_alert[district]) >= 60:
            last_alert[district] = ts
            msg_count['anomalies'] += 1
            q_alerts.put({'district': district, 'count': count, 'threshold': THRESHOLD, 'ts': ts})
            log.info(f"🚨 ANOMALY: District {district} – {count} events (threshold={THRESHOLD})")

# --------------------------------------------------------------------------- #
# Stage 6 – Alert Bolt (write to MongoDB + PostgreSQL)
# --------------------------------------------------------------------------- #
def alert_bolt():
    mongo  = pymongo.MongoClient(f"mongodb://{MONGO_HOST}:{MONGO_PORT}/")
    col    = mongo[MONGO_DB][MONGO_COL]
    pg     = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS)
    pg.autocommit = True

    while True:
        alert = q_alerts.get()
        dt_str = datetime.datetime.fromtimestamp(alert['ts']).isoformat()

        # MongoDB
        try:
            col.insert_one({
                'district':    alert['district'],
                'event_count': alert['count'],
                'threshold':   alert['threshold'],
                'timestamp':   dt_str,
                'severity':    'HIGH'
            })
        except Exception as e:
            log.error(f"Mongo insert failed: {e}")

        # PostgreSQL
        try:
            with pg.cursor() as cur:
                cur.execute(
                    "INSERT INTO alerts (district, timestamp, event_count, threshold, alert_severity_level) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (alert['district'], dt_str, alert['count'], alert['threshold'], 'HIGH')
                )
            log.info(f"✅ Alert saved: District {alert['district']} @ {dt_str}")
        except Exception as e:
            log.error(f"PostgreSQL insert failed: {e}")

# --------------------------------------------------------------------------- #
# Main – start all stages as daemon threads
# --------------------------------------------------------------------------- #
if __name__ == '__main__':
    stages = [
        threading.Thread(target=kafka_spout,  name="kafka-spout",  daemon=True),
        threading.Thread(target=parse_bolt,   name="parse-bolt",   daemon=True),
        threading.Thread(target=district_bolt,name="district-bolt",daemon=True),
        threading.Thread(target=window_bolt,  name="window-bolt",  daemon=True),
        threading.Thread(target=anomaly_bolt, name="anomaly-bolt", daemon=True),
        threading.Thread(target=alert_bolt,   name="alert-bolt",   daemon=True),
    ]
    log.info("🚀 Starting Crime Analytics Speed Layer Pipeline...")
    for t in stages:
        t.start()
    log.info("✅ All stages running. Waiting for Kafka events...")
    try:
        while True:
            time.sleep(5)
            log.info(f"Throughput – received:{msg_count['received']} parsed:{msg_count['parsed']} "
                     f"anomalies:{msg_count['anomalies']} | "
                     f"Queue depths – raw:{q_raw.qsize()} parsed:{q_parsed.qsize()} "
                     f"district:{q_district.qsize()} windowed:{q_windowed.qsize()} alerts:{q_alerts.qsize()}")
    except KeyboardInterrupt:
        log.info("Pipeline stopped.")
