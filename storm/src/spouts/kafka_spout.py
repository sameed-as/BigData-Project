import json
import yaml
import os
from streamparse import Spout
from kafka import KafkaConsumer

class CrimeKafkaSpout(Spout):
    outputs = ['raw_json']

    def initialize(self, stormconf, context):
        config_path = '/app/config/config.yaml'
        # Default config if running locally outside docker
        if not os.path.exists(config_path):
            config_path = os.path.join(os.path.dirname(__file__), '../../../config/config.yaml')
        
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        broker = self.config['kafka']['broker']
        # If running locally for testing, we might need localhost instead of kafka
        if not os.environ.get('IN_DOCKER') and broker == 'kafka:9092':
            broker = 'localhost:29092'
            
        topic = self.config['kafka']['topic']
        
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[broker],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )

    def next_tuple(self):
        msg_pack = self.consumer.poll(timeout_ms=50)
        for tp, messages in msg_pack.items():
            for message in messages:
                self.emit([json.dumps(message.value)])
