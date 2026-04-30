from pymongo import MongoClient
import sys

def setup_mongo():
    try:
        # Connect to MongoDB
        client = MongoClient('mongodb://mongo:27017/')
        
        # Create database
        db = client['crime_db']
        
        # Create collection
        alerts = db['alert_logs']
        
        # Create index on district and timestamp for faster queries
        alerts.create_index([("district", 1), ("timestamp", -1)])
        print("MongoDB setup complete. 'crime_db' and 'alert_logs' created.")
    except Exception as e:
        print(f"Error setting up MongoDB: {e}")
        sys.exit(1)

if __name__ == "__main__":
    setup_mongo()
