import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from streamparse import Grouping, Topology
from spouts.kafka_spout import CrimeKafkaSpout
from bolts.parse_bolt import ParseBolt
from bolts.district_bolt import DistrictBolt
from bolts.window_bolt import WindowBolt
from bolts.anomaly_bolt import AnomalyBolt
from bolts.alert_bolt import AlertBolt

class CrimeTopology(Topology):
    kafka_spout = CrimeKafkaSpout.spec()
    parse_bolt = ParseBolt.spec(inputs={kafka_spout: Grouping.SHUFFLE})
    district_bolt = DistrictBolt.spec(inputs={parse_bolt: Grouping.SHUFFLE})
    
    # We want window_bolt to count per district, so group by district
    window_bolt = WindowBolt.spec(inputs={district_bolt: Grouping.fields('district')})
    
    # Anomaly_bolt also needs district-specific routing to throttle properly
    anomaly_bolt = AnomalyBolt.spec(inputs={window_bolt: Grouping.fields('district')})
    
    # Alert bolt can process anything
    alert_bolt = AlertBolt.spec(inputs={anomaly_bolt: Grouping.SHUFFLE})
