from streamparse import Bolt
import pymongo
import psycopg2
import yaml
import datetime
import os

class AlertBolt(Bolt):
    def initialize(self, conf, ctx):
        config_path = '/app/config/config.yaml'
        if not os.path.exists(config_path):
            config_path = os.path.join(os.path.dirname(__file__), '../../../config/config.yaml')
            
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # DB Configuration
        mongo_host = self.config['mongo']['host']
        mongo_port = self.config['mongo']['port']
        mongo_db = self.config['mongo']['db']
        
        pg_host = self.config['postgres']['host']
        pg_port = self.config['postgres']['port']
        pg_db = self.config['postgres']['db']
        pg_user = self.config['postgres']['user']
        pg_pass = self.config['postgres']['password']
        
        # Local overrides if running outside docker
        if not os.environ.get('IN_DOCKER'):
            if mongo_host == 'mongo': mongo_host = 'localhost'
            if pg_host == 'postgres': pg_host = 'localhost'
        
        self.mongo_client = pymongo.MongoClient(f"mongodb://{mongo_host}:{mongo_port}/")
        self.mongo_col = self.mongo_client[mongo_db][self.config['mongo']['alerts_collection']]
        
        self.pg_conn = psycopg2.connect(
            host=pg_host, port=pg_port, dbname=pg_db, user=pg_user, password=pg_pass
        )
        self.pg_conn.autocommit = True

    def process(self, tup):
        district = tup.values[0]
        count = tup.values[1]
        threshold = tup.values[2]
        ts = tup.values[3]
        dt_str = datetime.datetime.fromtimestamp(ts).isoformat()
        
        alert_doc = {
            'district': district,
            'event_count': count,
            'threshold': threshold,
            'timestamp': dt_str,
            'severity': 'HIGH'
        }
        
        try:
            # Mongo
            self.mongo_col.insert_one(alert_doc)
            
            # Postgres (make sure table `alerts` is created in init.sql)
            with self.pg_conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO alerts (district, timestamp, event_count, threshold, alert_severity_level) VALUES (%s, %s, %s, %s, %s)",
                    (district, dt_str, count, threshold, 'HIGH')
                )
            self.logger.info(f"Alert saved: District {district} at {dt_str} with {count} events.")
        except Exception as e:
            self.logger.error(f"Failed to save alert: {e}")
