from streamparse import Bolt
import time
import yaml
import os

class WindowBolt(Bolt):
    outputs = ['district', 'window_count', 'timestamp']

    def initialize(self, conf, ctx):
        self.counts = {} # district -> [timestamps]
        
        config_path = '/app/config/config.yaml'
        if not os.path.exists(config_path):
            config_path = os.path.join(os.path.dirname(__file__), '../../../config/config.yaml')
        
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
            
        self.window_size = self.config.get('storm', {}).get('window_size_minutes', 5) * 60

    def process(self, tup):
        district = tup.values[0]
        now = time.time()
        
        if district not in self.counts:
            self.counts[district] = []
        self.counts[district].append(now)
        
        # Clean up old events outside the window
        self.counts[district] = [t for t in self.counts[district] if now - t <= self.window_size]
        
        # In a real sliding window, we might only emit on 'slide_interval_minutes'.
        # For real-time anomalies per event, emitting every time is acceptable.
        self.emit([district, len(self.counts[district]), now])
