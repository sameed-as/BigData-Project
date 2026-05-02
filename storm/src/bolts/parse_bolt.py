import json
from streamparse import Bolt

class ParseBolt(Bolt):
    outputs = ['case_number', 'date', 'block', 'primary_type', 'district', 'arrest', 'latitude', 'longitude']

    def process(self, tup):
        record_str = tup.values[0]
        try:
            record = json.loads(record_str)
            # Validate required fields
            req_fields = ['CASE NUMBER', 'DATE', 'BLOCK', 'PRIMARY TYPE', 'DISTRICT', 'ARREST', 'LATITUDE', 'LONGITUDE']
            
            # Simple validation: just ensure the key exists and has some value
            if all(record.get(f) for f in req_fields):
                self.emit([
                    record['CASE NUMBER'],
                    record['DATE'],
                    record['BLOCK'],
                    record['PRIMARY TYPE'],
                    record['DISTRICT'],
                    record['ARREST'],
                    record['LATITUDE'],
                    record['LONGITUDE']
                ])
            else:
                self.logger.warning(f"Missing required fields in record: {record}")
        except Exception as e:
            self.logger.error(f"Error parsing tuple: {e}")
