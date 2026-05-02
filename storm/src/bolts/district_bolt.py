from streamparse import Bolt

class DistrictBolt(Bolt):
    outputs = ['district', 'case_number', 'date', 'primary_type']

    def process(self, tup):
        district = tup.values[4]
        case_number = tup.values[0]
        date = tup.values[1]
        primary_type = tup.values[3]
        
        # Emits the district along with key info so we can group by district next
        self.emit([district, case_number, date, primary_type])
