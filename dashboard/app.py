import streamlit as st
import pandas as pd
import pymongo
import psycopg2
import yaml
import os
import time

st.set_page_config(page_title="Real-Time Crime Analytics", layout="wide")

@st.cache_resource
def load_config():
    config_path = '/app/config/config.yaml'
    if not os.path.exists(config_path):
        config_path = os.path.join(os.path.dirname(__file__), '../config/config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

config = load_config()

# Database Connections
def get_pg_connection():
    pg_host = config['postgres']['host']
    if not os.environ.get('IN_DOCKER'):
        if pg_host == 'postgres': pg_host = 'localhost'
        
    return psycopg2.connect(
        host=pg_host,
        port=config['postgres']['port'],
        dbname=config['postgres']['db'],
        user=config['postgres']['user'],
        password=config['postgres']['password']
    )

def get_mongo_collection():
    mongo_host = config['mongo']['host']
    if not os.environ.get('IN_DOCKER'):
        if mongo_host == 'mongo': mongo_host = 'localhost'
        
    client = pymongo.MongoClient(f"mongodb://{mongo_host}:{config['mongo']['port']}/")
    return client[config['mongo']['db']][config['mongo']['alerts_collection']]

st.title("🚓 Real-Time Crime Analytics & Intelligent Alert System")

# Refresh mechanism for real-time
placeholder = st.empty()

with placeholder.container():
    col1, col2 = st.columns([1, 2])

    with col1:
        st.subheader("🚨 Live Alerts (MongoDB)")
        try:
            col = get_mongo_collection()
            alerts = list(col.find({}, {'_id': 0}).sort("timestamp", -1).limit(10))
            if alerts:
                for alert in alerts:
                    st.error(f"**District {alert['district']}** | {alert['timestamp']}\nEvents: {alert['event_count']} (Threshold: {alert['threshold']})")
            else:
                st.info("No live alerts currently.")
        except Exception as e:
            st.error("Could not connect to MongoDB.")

    with col2:
        st.subheader("🗺️ Crime Hotspots (K-Means)")
        try:
            conn = get_pg_connection()
            # Assuming 'hotspots' table was created by Spark job
            query = "SELECT latitude, longitude, cluster_id FROM hotspots LIMIT 1000"
            df_hotspots = pd.read_sql(query, conn)
            if not df_hotspots.empty:
                # rename for streamlit map
                df_hotspots = df_hotspots.rename(columns={"latitude": "lat", "longitude": "lon"})
                st.map(df_hotspots)
            else:
                st.info("Run the Spark batch job to generate hotspot data.")
        except Exception as e:
            st.error("Could not load hotspot data from Postgres. Ensure Spark job has run.")

    st.markdown("---")
    st.subheader("📊 Batch Analytics: Crime Trends & Arrest Rates")
    try:
        conn = get_pg_connection()
        col3, col4 = st.columns(2)
        
        with col3:
            st.write("**Top Crime Types by Arrest Rate**")
            query_arrest = "SELECT primary_type, arrest_rate FROM top_10_arrest_rates ORDER BY arrest_rate DESC LIMIT 10"
            df_arrest = pd.read_sql(query_arrest, conn)
            if not df_arrest.empty:
                st.bar_chart(data=df_arrest.set_index("primary_type"))
            else:
                st.info("No arrest rate data available.")
                
        with col4:
            st.write("**Crime Count by Month**")
            query_trend = "SELECT month, SUM(crime_count) as total_crimes FROM crime_trends GROUP BY month ORDER BY month"
            df_trend = pd.read_sql(query_trend, conn)
            if not df_trend.empty:
                st.line_chart(data=df_trend.set_index("month"))
            else:
                st.info("No crime trend data available.")
    except Exception as e:
         pass

# Auto-refresh logic can be added or user can manually refresh
if st.button("Refresh Data"):
    st.rerun()
