from streamparse import Bolt
import yaml
import os

class AnomalyBolt(Bolt):
    outputs = ['district', 'window_count', 'threshold', 'timestamp']

    def initialize(self, conf, ctx):
        config_path = '/app/config/config.yaml'
        if not os.path.exists(config_path):
            config_path = os.path.join(os.path.dirname(__file__), '../../../config/config.yaml')
            
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        self.threshold = self.config.get('storm', {}).get('anomaly_threshold', 10)
        
        # Keep track of when we last alerted to avoid spam
        self.last_alert_time = {}

    def process(self, tup):
        district = tup.values[0]
        count = tup.values[1]
        timestamp = tup.values[2]
        
        if count > self.threshold:
            # Throttle alerts to once per minute per district
            last_alert = self.last_alert_time.get(district, 0)
            if timestamp - last_alert >= 60:
                self.emit([district, count, self.threshold, timestamp])
                self.last_alert_time[district] = timestamp
