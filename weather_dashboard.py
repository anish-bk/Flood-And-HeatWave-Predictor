"""
Real-Time Weather Prediction Dashboard
Fetches weather from OpenWeatherMap → Streams via Kafka → ML Prediction → Display

Features:
- District selection (Bara, Dhanusa, Sarlahi, Parsa, Siraha)
- Auto-streaming every 30 seconds from OpenWeatherMap
- Real-time heatwave and flood predictions
- Cassandra storage for historical data

Usage:
    1. Start Kafka: docker-compose up -d
    2. Start Producer API: python kafka_producer_api.py
    3. Run this dashboard: python weather_dashboard.py
"""

import os
import json
import time
import threading
import requests
from datetime import datetime
from queue import Queue, Empty
from collections import deque
import warnings
warnings.filterwarnings('ignore')

import gradio as gr
import pandas as pd
import numpy as np

# Try importing Kafka
try:
    from kafka import KafkaConsumer
    from kafka.errors import KafkaError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print("⚠️  kafka-python not installed. Run: pip install kafka-python")

# Try importing ML libraries
try:
    import joblib
    import xgboost as xgb
    ML_AVAILABLE = True
except ImportError:
    ML_AVAILABLE = False
    print("⚠️  ML libraries not available")

# Try importing Cassandra
try:
    from cassandra.cluster import Cluster
    from cassandra.query import SimpleStatement
    import uuid
    CASSANDRA_AVAILABLE = True
except ImportError:
    CASSANDRA_AVAILABLE = False
    print("⚠️  cassandra-driver not installed. Run: pip install cassandra-driver")


# ============================================================================
# CONFIGURATION
# ============================================================================

KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'topic': 'weather-data',
    'group_id': 'weather-dashboard-group',
    'auto_offset_reset': 'latest'
}

# Producer API endpoint
PRODUCER_API_URL = "http://localhost:8000"

# Districts to monitor
DISTRICTS = ["Bara", "Dhanusa", "Sarlahi", "Parsa", "Siraha"]

# Streaming interval in seconds
STREAM_INTERVAL = 30

MODEL_PATHS = {
    'heatwave': 'models/xgb_heatwave_model.joblib',
    'flood': 'models/xgb_flood_proxy_model.joblib'
}

# Cassandra Configuration
CASSANDRA_CONFIG = {
    'hosts': ['127.0.0.1'],
    'port': 9042,
    'keyspace': 'weather_monitoring'
}


# ============================================================================
# CASSANDRA STORAGE
# ============================================================================

class CassandraStorage:
    """Cassandra storage for weather data and predictions."""
    
    def __init__(self, hosts: list, port: int, keyspace: str):
        self.hosts = hosts
        self.port = port
        self.keyspace = keyspace
        self.cluster = None
        self.session = None
        self.connected = False
        self.stats = {
            'records_stored': 0,
            'predictions_stored': 0,
            'errors': 0
        }
    
    def connect(self) -> bool:
        """Connect to Cassandra cluster."""
        if not CASSANDRA_AVAILABLE:
            print("⚠️  Cassandra driver not available")
            return False
        
        try:
            self.cluster = Cluster(contact_points=self.hosts, port=self.port)
            self.session = self.cluster.connect()
            self.connected = True
            print(f"✅ Connected to Cassandra at {self.hosts}:{self.port}")
            self._setup_schema()
            return True
        except Exception as e:
            print(f"⚠️  Cassandra connection failed: {e}")
            print("   Data will not be persisted to database.")
            self.connected = False
            return False
    
    def _setup_schema(self):
        """Create keyspace and tables if they don't exist."""
        # Create keyspace
        self.session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)
        
        self.session.set_keyspace(self.keyspace)
        
        # Table for weather observations
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS weather_observations (
                id UUID,
                district TEXT,
                date TEXT,
                fetch_time TIMESTAMP,
                max_temp DOUBLE,
                min_temp DOUBLE,
                temp_range DOUBLE,
                precipitation DOUBLE,
                humidity DOUBLE,
                wind_speed DOUBLE,
                pressure DOUBLE,
                cloudiness INT,
                visibility DOUBLE,
                weather_desc TEXT,
                sunrise TEXT,
                sunset TEXT,
                source TEXT,
                PRIMARY KEY ((district), fetch_time, id)
            ) WITH CLUSTERING ORDER BY (fetch_time DESC)
        """)
        
        # Table for predictions
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS weather_predictions (
                id UUID,
                district TEXT,
                prediction_time TIMESTAMP,
                max_temp DOUBLE,
                precipitation DOUBLE,
                humidity DOUBLE,
                heatwave_probability DOUBLE,
                flood_probability DOUBLE,
                heatwave_risk TEXT,
                flood_risk TEXT,
                PRIMARY KEY ((district), prediction_time, id)
            ) WITH CLUSTERING ORDER BY (prediction_time DESC)
        """)
        
        # Table for daily aggregates
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS daily_weather_summary (
                district TEXT,
                date TEXT,
                record_count INT,
                avg_temp DOUBLE,
                max_temp DOUBLE,
                min_temp DOUBLE,
                total_precip DOUBLE,
                avg_humidity DOUBLE,
                heatwave_alerts INT,
                flood_alerts INT,
                PRIMARY KEY ((district), date)
            ) WITH CLUSTERING ORDER BY (date DESC)
        """)
        
        print("✅ Cassandra schema ready")
    
    def store_weather_data(self, data: dict) -> bool:
        """Store weather observation in Cassandra."""
        if not self.connected:
            return False
        
        try:
            record_id = uuid.uuid4()
            fetch_time = datetime.fromisoformat(data.get('fetched_at', datetime.now().isoformat()))
            
            self.session.execute("""
                INSERT INTO weather_observations 
                (id, district, date, fetch_time, max_temp, min_temp, temp_range,
                 precipitation, humidity, wind_speed, pressure, cloudiness,
                 visibility, weather_desc, sunrise, sunset, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                record_id,
                data.get('District', ''),
                data.get('Date', ''),
                fetch_time,
                float(data.get('MaxTemp_2m', 0)),
                float(data.get('MinTemp_2m', 0)),
                float(data.get('TempRange_2m', 0)),
                float(data.get('Precip', 0)),
                float(data.get('RH_2m', 0)),
                float(data.get('WindSpeed_10m', 0)),
                float(data.get('Pressure', 0)),
                int(data.get('Cloudiness', 0)),
                float(data.get('Visibility', 0)),
                data.get('WeatherDesc', ''),
                data.get('Sunrise', ''),
                data.get('Sunset', ''),
                data.get('source', 'openweathermap')
            ))
            
            self.stats['records_stored'] += 1
            return True
            
        except Exception as e:
            print(f"❌ Error storing weather data: {e}")
            self.stats['errors'] += 1
            return False
    
    def store_prediction(self, data: dict) -> bool:
        """Store prediction in Cassandra."""
        if not self.connected:
            return False
        
        try:
            record_id = uuid.uuid4()
            prediction_time = datetime.now()
            
            self.session.execute("""
                INSERT INTO weather_predictions
                (id, district, prediction_time, max_temp, precipitation, humidity,
                 heatwave_probability, flood_probability, heatwave_risk, flood_risk)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                record_id,
                data.get('District', ''),
                prediction_time,
                float(data.get('MaxTemp_2m', 0)),
                float(data.get('Precip', 0)),
                float(data.get('RH_2m', 0)),
                float(data.get('heatwave_probability', 0)),
                float(data.get('flood_probability', 0)),
                data.get('heatwave_risk', 'LOW'),
                data.get('flood_risk', 'LOW')
            ))
            
            self.stats['predictions_stored'] += 1
            return True
            
        except Exception as e:
            print(f"❌ Error storing prediction: {e}")
            self.stats['errors'] += 1
            return False
    
    def get_recent_observations(self, district: str, limit: int = 20) -> list:
        """Get recent observations for a district."""
        if not self.connected:
            return []
        
        try:
            rows = self.session.execute("""
                SELECT * FROM weather_observations
                WHERE district = %s
                LIMIT %s
            """, (district, limit))
            
            return [dict(row._asdict()) for row in rows]
        except Exception as e:
            print(f"❌ Error fetching observations: {e}")
            return []
    
    def get_recent_predictions(self, district: str, limit: int = 20) -> list:
        """Get recent predictions for a district."""
        if not self.connected:
            return []
        
        try:
            rows = self.session.execute("""
                SELECT * FROM weather_predictions
                WHERE district = %s
                LIMIT %s
            """, (district, limit))
            
            return [dict(row._asdict()) for row in rows]
        except Exception as e:
            print(f"❌ Error fetching predictions: {e}")
            return []
    
    def get_stats(self) -> dict:
        """Get storage statistics."""
        return self.stats.copy()
    
    def close(self):
        """Close Cassandra connection."""
        if self.cluster:
            self.cluster.shutdown()
            self.connected = False


# ============================================================================
# ML PREDICTOR
# ============================================================================

class WeatherPredictor:
    """ML model wrapper for weather predictions."""
    
    def __init__(self):
        self.heatwave_model = None
        self.flood_model = None
        self._load_models()
    
    def _load_models(self):
        """Load XGBoost models."""
        try:
            if os.path.exists(MODEL_PATHS['heatwave']):
                self.heatwave_model = joblib.load(MODEL_PATHS['heatwave'])
                print("✅ Loaded heatwave model")
            else:
                print(f"⚠️  Heatwave model not found at {MODEL_PATHS['heatwave']}")
                
            if os.path.exists(MODEL_PATHS['flood']):
                self.flood_model = joblib.load(MODEL_PATHS['flood'])
                print("✅ Loaded flood model")
            else:
                print(f"⚠️  Flood model not found at {MODEL_PATHS['flood']}")
        except Exception as e:
            print(f"❌ Error loading models: {e}")
    
    def predict(self, data: dict) -> dict:
        """Make predictions from weather data."""
        features = self._extract_features(data)
        
        heatwave_prob = 0.0
        flood_prob = 0.0
        
        # Heatwave prediction
        if self.heatwave_model is not None:
            try:
                if isinstance(self.heatwave_model, xgb.Booster):
                    dmatrix = xgb.DMatrix([list(features.values())], feature_names=list(features.keys()))
                    heatwave_prob = float(self.heatwave_model.predict(dmatrix)[0])
                else:
                    heatwave_prob = float(self.heatwave_model.predict_proba([list(features.values())])[0][1])
            except Exception as e:
                heatwave_prob = self._rule_based_heatwave(data)
        else:
            heatwave_prob = self._rule_based_heatwave(data)
        
        # Flood prediction
        if self.flood_model is not None:
            try:
                if isinstance(self.flood_model, xgb.Booster):
                    dmatrix = xgb.DMatrix([list(features.values())], feature_names=list(features.keys()))
                    flood_prob = float(self.flood_model.predict(dmatrix)[0])
                else:
                    flood_prob = float(self.flood_model.predict_proba([list(features.values())])[0][1])
            except Exception as e:
                flood_prob = self._rule_based_flood(data)
        else:
            flood_prob = self._rule_based_flood(data)
        
        return {
            'heatwave_probability': heatwave_prob,
            'flood_probability': flood_prob,
            'heatwave_risk': 'HIGH' if heatwave_prob > 0.5 else 'MEDIUM' if heatwave_prob > 0.3 else 'LOW',
            'flood_risk': 'HIGH' if flood_prob > 0.5 else 'MEDIUM' if flood_prob > 0.3 else 'LOW'
        }
    
    def _rule_based_heatwave(self, data: dict) -> float:
        """Rule-based heatwave prediction."""
        temp = data.get('MaxTemp_2m', data.get('temp_max', 30))
        humidity = data.get('RH_2m', data.get('humidity', 50))
        # Heat index consideration
        if temp > 40:
            return min(1.0, 0.7 + (temp - 40) / 20)
        elif temp > 35:
            return min(1.0, 0.3 + (temp - 35) / 10)
        else:
            return max(0.0, (temp - 30) / 20)
    
    def _rule_based_flood(self, data: dict) -> float:
        """Rule-based flood prediction."""
        precip = data.get('Precip', data.get('precipitation', 0))
        humidity = data.get('RH_2m', data.get('humidity', 50))
        
        flood_score = 0.0
        if precip > 100:
            flood_score = 0.8
        elif precip > 50:
            flood_score = 0.5
        elif precip > 20:
            flood_score = 0.3
        
        # Humidity factor
        if humidity > 90:
            flood_score += 0.2
        elif humidity > 80:
            flood_score += 0.1
        
        return min(1.0, flood_score)
    
    def _extract_features(self, data: dict) -> dict:
        """Extract model features from raw data."""
        temp = data.get('MaxTemp_2m', data.get('temp_max', 30))
        precip = data.get('Precip', data.get('precipitation', 0))
        humidity = data.get('RH_2m', data.get('humidity', 50))
        
        return {
            'Precip': precip,
            'precip_3d': precip * 3,
            'precip_7d': precip * 7,
            'precip_lag_1': precip,
            'precip_lag_3': precip,
            'precip_lag_7': precip,
            'MaxTemp_2m': temp,
            'maxT_3d_mean': temp,
            'maxT_lag_1': temp,
            'maxT_lag_3': temp,
            'anom_maxT': temp - 30,
            'RH_2m': humidity,
            'wetness_flag': 1 if humidity > 80 else 0,
            'API': precip * 0.9,
            'TempRange_2m': data.get('TempRange_2m', data.get('temp_range', 10)),
            'WindSpeed_10m': data.get('WindSpeed_10m', data.get('wind_speed', 5)),
            'WindSpeed_50m': data.get('WindSpeed_50m', 10),
            'doy_sin': np.sin(2 * np.pi * datetime.now().timetuple().tm_yday / 365),
            'doy_cos': np.cos(2 * np.pi * datetime.now().timetuple().tm_yday / 365),
            'month': datetime.now().month,
            'year': datetime.now().year
        }


# ============================================================================
# WEATHER STREAMING SERVICE
# ============================================================================

class WeatherStreamingService:
    """Service to fetch weather data and stream via Kafka."""
    
    def __init__(self, producer_api_url: str, cassandra_storage: CassandraStorage = None):
        self.producer_api_url = producer_api_url
        self.cassandra = cassandra_storage
        self.streaming = False
        self.current_district = None
        self.stream_thread = None
        self.data_history = deque(maxlen=50)
        self.predictor = WeatherPredictor()
        self.stats = {
            'api_calls': 0,
            'successful': 0,
            'errors': 0,
            'cassandra_stored': 0,
            'last_fetch_time': None
        }
        self.latest_data = None
        self.latest_prediction = None
    
    def fetch_weather(self, district: str) -> dict:
        """Fetch weather for a district from OpenWeatherMap via Producer API."""
        try:
            # Call the producer API which fetches from OpenWeatherMap
            url = f"{self.producer_api_url}/weather/city/{district}"
            response = requests.post(url, timeout=15)
            self.stats['api_calls'] += 1
            
            if response.status_code == 200:
                data = response.json()
                self.stats['successful'] += 1
                self.stats['last_fetch_time'] = datetime.now().isoformat()
                
                # Extract weather data
                weather = data.get('weather', {})
                
                # Transform to our format
                result = {
                    'District': district,
                    'Date': weather.get('date', datetime.now().strftime('%Y-%m-%d')),
                    'MaxTemp_2m': weather.get('temp_max', weather.get('temperature', 0)),
                    'MinTemp_2m': weather.get('temp_min', 0),
                    'TempRange_2m': weather.get('temp_range', 0),
                    'Precip': weather.get('precipitation', 0),
                    'RH_2m': weather.get('humidity', 0),
                    'WindSpeed_10m': weather.get('wind_speed', 0),
                    'Pressure': weather.get('pressure', 0),
                    'Cloudiness': weather.get('cloudiness', 0),
                    'Visibility': weather.get('visibility_km', 0),
                    'WeatherDesc': weather.get('weather_description', ''),
                    'Sunrise': weather.get('sunrise', ''),
                    'Sunset': weather.get('sunset', ''),
                    'fetched_at': datetime.now().isoformat(),
                    'kafka_published': data.get('kafka_published', False),
                    'source': 'openweathermap'
                }
                
                # Make prediction
                prediction = self.predictor.predict(result)
                result.update(prediction)
                
                self.latest_data = result
                self.latest_prediction = prediction
                self.data_history.append(result)
                
                # Store in Cassandra
                if self.cassandra and self.cassandra.connected:
                    if self.cassandra.store_weather_data(result):
                        self.stats['cassandra_stored'] += 1
                    self.cassandra.store_prediction(result)
                
                return result
            else:
                self.stats['errors'] += 1
                error_msg = response.json().get('detail', 'Unknown error')
                return {'error': error_msg, 'District': district}
                
        except requests.exceptions.ConnectionError:
            self.stats['errors'] += 1
            return {'error': 'Producer API not running. Start with: python kafka_producer_api.py', 'District': district}
        except Exception as e:
            self.stats['errors'] += 1
            return {'error': str(e), 'District': district}
    
    def start_streaming(self, district: str, interval: int = 30):
        """Start auto-streaming for a district."""
        self.current_district = district
        self.streaming = True
        
        def stream_loop():
            while self.streaming and self.current_district:
                self.fetch_weather(self.current_district)
                time.sleep(interval)
        
        self.stream_thread = threading.Thread(target=stream_loop, daemon=True)
        self.stream_thread.start()
        print(f"🚀 Started streaming for {district} every {interval}s")
    
    def stop_streaming(self):
        """Stop auto-streaming."""
        self.streaming = False
        self.current_district = None
        print("⏹️  Streaming stopped")
    
    def get_history(self) -> list:
        """Get data history."""
        return list(self.data_history)
    
    def get_stats(self) -> dict:
        """Get service statistics."""
        return self.stats.copy()


# ============================================================================
# GLOBAL INSTANCES
# ============================================================================

# Initialize Cassandra storage
cassandra_storage = CassandraStorage(
    hosts=CASSANDRA_CONFIG['hosts'],
    port=CASSANDRA_CONFIG['port'],
    keyspace=CASSANDRA_CONFIG['keyspace']
)
cassandra_storage.connect()

# Initialize streaming service with Cassandra
streaming_service = WeatherStreamingService(PRODUCER_API_URL, cassandra_storage)


# ============================================================================
# DASHBOARD FUNCTIONS
# ============================================================================

def create_weather_card(data: dict) -> str:
    """Create HTML card for current weather."""
    if not data or 'error' in data:
        error_msg = data.get('error', 'No data available') if data else 'Select a district to start'
        return f"""
        <div style="text-align: center; padding: 40px; color: #64748b; background: #1e293b; border-radius: 16px;">
            <div style="font-size: 48px;">🌤️</div>
            <div style="font-size: 18px; margin-top: 10px;">{error_msg}</div>
        </div>
        """
    
    temp = data.get('MaxTemp_2m', 0)
    precip = data.get('Precip', 0)
    humidity = data.get('RH_2m', 0)
    wind = data.get('WindSpeed_10m', 0)
    desc = data.get('WeatherDesc', 'N/A').title()
    
    temp_color = "#ef4444" if temp > 40 else "#f59e0b" if temp > 35 else "#22c55e"
    
    return f"""
    <div style="background: linear-gradient(135deg, #1e293b, #334155); padding: 25px; border-radius: 20px; border: 1px solid #475569;">
        <div style="text-align: center; margin-bottom: 20px;">
            <div style="font-size: 18px; color: #94a3b8;">📍 {data.get('District', 'Unknown')}</div>
            <div style="font-size: 14px; color: #64748b;">{data.get('Date', 'N/A')} | {desc}</div>
        </div>
        
        <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 20px;">
            <div style="text-align: center; background: rgba(0,0,0,0.2); padding: 20px; border-radius: 12px;">
                <div style="font-size: 48px; color: {temp_color}; font-weight: bold;">{temp:.1f}°C</div>
                <div style="color: #94a3b8; margin-top: 5px;">🌡️ Temperature</div>
            </div>
            <div style="text-align: center; background: rgba(0,0,0,0.2); padding: 20px; border-radius: 12px;">
                <div style="font-size: 48px; color: #3b82f6; font-weight: bold;">{humidity:.0f}%</div>
                <div style="color: #94a3b8; margin-top: 5px;">💧 Humidity</div>
            </div>
            <div style="text-align: center; background: rgba(0,0,0,0.2); padding: 20px; border-radius: 12px;">
                <div style="font-size: 48px; color: #06b6d4; font-weight: bold;">{precip:.1f}mm</div>
                <div style="color: #94a3b8; margin-top: 5px;">🌧️ Precipitation</div>
            </div>
            <div style="text-align: center; background: rgba(0,0,0,0.2); padding: 20px; border-radius: 12px;">
                <div style="font-size: 48px; color: #a855f7; font-weight: bold;">{wind:.1f}</div>
                <div style="color: #94a3b8; margin-top: 5px;">💨 Wind (m/s)</div>
            </div>
        </div>
        
        <div style="margin-top: 15px; text-align: center; color: #64748b; font-size: 12px;">
            ☀️ Sunrise: {data.get('Sunrise', 'N/A')} | 🌙 Sunset: {data.get('Sunset', 'N/A')}
        </div>
    </div>
    """


def create_prediction_card(data: dict) -> str:
    """Create HTML card for predictions."""
    if not data or 'error' in data:
        return """
        <div style="text-align: center; padding: 40px; color: #64748b; background: #1e293b; border-radius: 16px;">
            <div style="font-size: 48px;">🔮</div>
            <div style="font-size: 18px; margin-top: 10px;">Awaiting weather data for prediction...</div>
        </div>
        """
    
    heatwave_prob = data.get('heatwave_probability', 0) * 100
    flood_prob = data.get('flood_probability', 0) * 100
    heatwave_risk = data.get('heatwave_risk', 'LOW')
    flood_risk = data.get('flood_risk', 'LOW')
    
    hw_color = "#ef4444" if heatwave_risk == 'HIGH' else "#f59e0b" if heatwave_risk == 'MEDIUM' else "#22c55e"
    fl_color = "#ef4444" if flood_risk == 'HIGH' else "#f59e0b" if flood_risk == 'MEDIUM' else "#22c55e"
    
    hw_bg = "rgba(239,68,68,0.2)" if heatwave_risk == 'HIGH' else "rgba(245,158,11,0.2)" if heatwave_risk == 'MEDIUM' else "rgba(34,197,94,0.2)"
    fl_bg = "rgba(239,68,68,0.2)" if flood_risk == 'HIGH' else "rgba(245,158,11,0.2)" if flood_risk == 'MEDIUM' else "rgba(34,197,94,0.2)"
    
    return f"""
    <div style="background: linear-gradient(135deg, #1e293b, #334155); padding: 25px; border-radius: 20px; border: 1px solid #475569;">
        <div style="text-align: center; margin-bottom: 20px;">
            <div style="font-size: 18px; color: #94a3b8;">🔮 ML Predictions</div>
            <div style="font-size: 14px; color: #64748b;">Based on current weather conditions</div>
        </div>
        
        <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 20px;">
            <div style="text-align: center; background: {hw_bg}; padding: 25px; border-radius: 16px; border: 2px solid {hw_color};">
                <div style="font-size: 36px;">🔥</div>
                <div style="font-size: 14px; color: #94a3b8; margin: 10px 0;">Heatwave Risk</div>
                <div style="font-size: 42px; color: {hw_color}; font-weight: bold;">{heatwave_prob:.1f}%</div>
                <div style="font-size: 18px; color: {hw_color}; font-weight: 600; margin-top: 10px; 
                            padding: 5px 15px; background: rgba(0,0,0,0.3); border-radius: 20px; display: inline-block;">
                    {heatwave_risk}
                </div>
            </div>
            <div style="text-align: center; background: {fl_bg}; padding: 25px; border-radius: 16px; border: 2px solid {fl_color};">
                <div style="font-size: 36px;">🌊</div>
                <div style="font-size: 14px; color: #94a3b8; margin: 10px 0;">Flood Risk</div>
                <div style="font-size: 42px; color: {fl_color}; font-weight: bold;">{flood_prob:.1f}%</div>
                <div style="font-size: 18px; color: {fl_color}; font-weight: 600; margin-top: 10px;
                            padding: 5px 15px; background: rgba(0,0,0,0.3); border-radius: 20px; display: inline-block;">
                    {flood_risk}
                </div>
            </div>
        </div>
    </div>
    """


def create_status_html(streaming: bool, district: str, stats: dict, cassandra_stats: dict = None) -> str:
    """Create status bar HTML."""
    status_color = "#22c55e" if streaming else "#64748b"
    status_text = f"🔴 LIVE - Streaming {district}" if streaming else "⚫ Stopped"
    
    cassandra_stored = stats.get('cassandra_stored', 0)
    cassandra_status = "🟢" if cassandra_storage.connected else "🔴"
    
    return f"""
    <div style="display: flex; justify-content: space-between; align-items: center; 
                background: linear-gradient(135deg, #1e293b, #0f172a); padding: 15px 25px; 
                border-radius: 12px; border: 1px solid #334155;">
        <div style="display: flex; align-items: center; gap: 15px;">
            <div style="width: 12px; height: 12px; background: {status_color}; border-radius: 50%; 
                        animation: {'pulse 1.5s infinite' if streaming else 'none'};"></div>
            <span style="color: #f1f5f9; font-weight: 600;">{status_text}</span>
        </div>
        <div style="display: flex; gap: 20px; color: #94a3b8; font-size: 14px;">
            <span>📡 API: {stats.get('api_calls', 0)}</span>
            <span>✅ Success: {stats.get('successful', 0)}</span>
            <span>{cassandra_status} Cassandra: {cassandra_stored}</span>
            <span>❌ Errors: {stats.get('errors', 0)}</span>
        </div>
    </div>
    <style>
        @keyframes pulse {{
            0%, 100% {{ opacity: 1; }}
            50% {{ opacity: 0.5; }}
        }}
    </style>
    """


def create_history_table(history: list) -> pd.DataFrame:
    """Create history DataFrame."""
    if not history:
        return pd.DataFrame(columns=['Time', 'District', 'Temp (°C)', 'Humidity (%)', 'Precip (mm)', 'Heatwave %', 'Flood %'])
    
    rows = []
    for item in reversed(history[-15:]):
        if 'error' not in item:
            rows.append({
                'Time': item.get('fetched_at', '')[:19].replace('T', ' '),
                'District': item.get('District', ''),
                'Temp (°C)': round(item.get('MaxTemp_2m', 0), 1),
                'Humidity (%)': round(item.get('RH_2m', 0), 0),
                'Precip (mm)': round(item.get('Precip', 0), 1),
                'Heatwave %': round(item.get('heatwave_probability', 0) * 100, 1),
                'Flood %': round(item.get('flood_probability', 0) * 100, 1)
            })
    
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=['Time', 'District', 'Temp (°C)', 'Humidity (%)', 'Precip (mm)', 'Heatwave %', 'Flood %'])


# ============================================================================
# EVENT HANDLERS
# ============================================================================

def fetch_once(district: str):
    """Fetch weather once for selected district."""
    if not district:
        return (
            create_weather_card(None),
            create_prediction_card(None),
            create_status_html(False, None, streaming_service.get_stats()),
            create_history_table([]),
            f"⚠️ Please select a district"
        )
    
    data = streaming_service.fetch_weather(district)
    stats = streaming_service.get_stats()
    history = streaming_service.get_history()
    
    if 'error' in data:
        return (
            create_weather_card(data),
            create_prediction_card(None),
            create_status_html(False, district, stats),
            create_history_table(history),
            f"❌ Error: {data['error']}"
        )
    
    return (
        create_weather_card(data),
        create_prediction_card(data),
        create_status_html(streaming_service.streaming, district, stats),
        create_history_table(history),
        f"✅ Fetched weather for {district} - Temp: {data['MaxTemp_2m']:.1f}°C, Humidity: {data['RH_2m']:.0f}%"
    )


def start_streaming(district: str):
    """Start auto-streaming for district."""
    if not district:
        return f"⚠️ Please select a district first"
    
    # Fetch once immediately
    streaming_service.fetch_weather(district)
    
    # Start streaming
    streaming_service.start_streaming(district, interval=STREAM_INTERVAL)
    
    return f"🚀 Started auto-streaming for {district} every {STREAM_INTERVAL} seconds"


def stop_streaming():
    """Stop auto-streaming."""
    streaming_service.stop_streaming()
    return "⏹️ Streaming stopped"


def refresh_dashboard():
    """Refresh dashboard with latest data."""
    data = streaming_service.latest_data
    stats = streaming_service.get_stats()
    history = streaming_service.get_history()
    district = streaming_service.current_district
    
    return (
        create_weather_card(data),
        create_prediction_card(data),
        create_status_html(streaming_service.streaming, district, stats),
        create_history_table(history)
    )


# ============================================================================
# CUSTOM CSS
# ============================================================================

CUSTOM_CSS = """
.gradio-container {
    background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%) !important;
    min-height: 100vh;
}
.main-title {
    text-align: center;
    font-size: 2.5em;
    font-weight: 700;
    background: linear-gradient(135deg, #3b82f6, #22c55e, #f59e0b);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    padding: 20px;
    margin-bottom: 10px;
}
.subtitle {
    text-align: center;
    color: #64748b;
    font-size: 1.1em;
    margin-bottom: 25px;
}
.section-header {
    color: #94a3b8;
    font-size: 1.2em;
    font-weight: 600;
    border-left: 4px solid #3b82f6;
    padding-left: 12px;
    margin: 20px 0 15px 0;
}
"""


# ============================================================================
# MAIN DASHBOARD
# ============================================================================

def create_dashboard():
    """Create the Gradio dashboard."""
    
    with gr.Blocks(
        title="🌡️ Weather Prediction Dashboard",
        theme=gr.themes.Soft(
            primary_hue="blue",
            secondary_hue="green",
            neutral_hue="slate"
        ),
        css=CUSTOM_CSS
    ) as app:
        
        # Header
        gr.HTML("""
        <div class="main-title">
            🌡️ Real-Time Weather & Disaster Prediction 🌊
        </div>
        <div class="subtitle">
            OpenWeatherMap → Kafka Streaming → ML Prediction → Live Dashboard
        </div>
        """)
        
        # Status bar
        status_html = gr.HTML(value=create_status_html(False, None, {}))
        
        # District Selection & Controls
        with gr.Row():
            with gr.Column(scale=2):
                district_dropdown = gr.Dropdown(
                    choices=DISTRICTS,
                    label="📍 Select District",
                    value=None,
                    info="Choose a district to monitor",
                    interactive=True
                )
            with gr.Column(scale=1):
                fetch_btn = gr.Button("🔍 Fetch Once", variant="secondary", size="lg")
            with gr.Column(scale=1):
                start_btn = gr.Button("▶️ Start Streaming", variant="primary", size="lg")
            with gr.Column(scale=1):
                stop_btn = gr.Button("⏹️ Stop", variant="stop", size="lg")
        
        # Status message
        status_msg = gr.Textbox(
            label="Status", 
            value="👆 Select a district and click 'Fetch Once' or 'Start Streaming'",
            interactive=False
        )
        
        # Main content
        with gr.Row():
            with gr.Column(scale=1):
                gr.HTML('<div class="section-header">🌤️ Current Weather</div>')
                weather_card = gr.HTML(value=create_weather_card(None))
            
            with gr.Column(scale=1):
                gr.HTML('<div class="section-header">🔮 Risk Predictions</div>')
                prediction_card = gr.HTML(value=create_prediction_card(None))
        
        # History table
        gr.HTML('<div class="section-header">📊 Recent Data History</div>')
        history_table = gr.Dataframe(
            value=create_history_table([]),
            interactive=False,
            wrap=True
        )
        
        # Auto-refresh timer (every 5 seconds to update UI)
        timer = gr.Timer(value=5)
        
        # Info accordion
        with gr.Accordion("ℹ️ About This Dashboard", open=False):
            gr.Markdown(f"""
            ## 🌡️ Weather Prediction Dashboard
            
            This dashboard monitors weather conditions for districts in Nepal's Terai region and predicts disaster risks.
            
            ### 📍 Monitored Districts
            - **Bara** - Southern plains district
            - **Dhanusa** - Terai district bordering India  
            - **Sarlahi** - Agricultural district
            - **Parsa** - Contains Parsa Wildlife Reserve
            - **Siraha** - Eastern Terai district
            
            ### 🔄 Data Flow
            ```
            OpenWeatherMap API → Producer API (port 8000) → Kafka → This Dashboard
            ```
            
            ### ⏱️ Streaming
            - Click **"Start Streaming"** to auto-fetch weather every **{STREAM_INTERVAL} seconds**
            - Click **"Fetch Once"** for a single data point
            - Click **"Stop"** to stop auto-streaming
            
            ### 🔮 Predictions
            - **Heatwave Risk**: Based on temperature thresholds (>35°C moderate, >40°C high)
            - **Flood Risk**: Based on precipitation and humidity levels
            
            ### 🚀 Prerequisites
            1. Start Docker: `docker-compose up -d`
            2. Start Producer API: `python kafka_producer_api.py`
            3. Ensure OpenWeatherMap API key is set
            """)
        
        # Event handlers
        fetch_btn.click(
            fn=fetch_once,
            inputs=[district_dropdown],
            outputs=[weather_card, prediction_card, status_html, history_table, status_msg]
        )
        
        start_btn.click(
            fn=start_streaming,
            inputs=[district_dropdown],
            outputs=[status_msg]
        )
        
        stop_btn.click(
            fn=stop_streaming,
            outputs=[status_msg]
        )
        
        timer.tick(
            fn=refresh_dashboard,
            outputs=[weather_card, prediction_card, status_html, history_table]
        )
        
        # On district change, fetch immediately
        district_dropdown.change(
            fn=fetch_once,
            inputs=[district_dropdown],
            outputs=[weather_card, prediction_card, status_html, history_table, status_msg]
        )
    
    return app


# ============================================================================
# MAIN
# ============================================================================

def main():
    cassandra_status = "✅ Connected" if cassandra_storage.connected else "❌ Not connected"
    
    print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║   🌡️  REAL-TIME WEATHER & DISASTER PREDICTION DASHBOARD                      ║
║                                                                              ║
║   OpenWeatherMap → Kafka → ML Prediction → Cassandra → Live Display          ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

📍 Monitored Districts: Bara, Dhanusa, Sarlahi, Parsa, Siraha
⏱️  Streaming Interval: {interval} seconds
🔗 Producer API: {api_url}

🗄️  Cassandra Storage:
   Status: {cassandra_status}
   Hosts: {cassandra_hosts}
   Keyspace: {keyspace}
   Tables: weather_observations, weather_predictions, daily_weather_summary

🔗 Dashboard: http://localhost:7860

💡 Prerequisites:
   1. Start Docker: docker-compose up -d
   2. Start Producer API: python kafka_producer_api.py
   3. Open this dashboard and select a district

📊 Data is automatically stored in Cassandra when streaming!
""".format(
        interval=STREAM_INTERVAL, 
        api_url=PRODUCER_API_URL,
        cassandra_status=cassandra_status,
        cassandra_hosts=CASSANDRA_CONFIG['hosts'],
        keyspace=CASSANDRA_CONFIG['keyspace']
    ))
    
    app = create_dashboard()
    app.launch(
        server_name="127.0.0.1",
        server_port=7860,
        share=False
    )


if __name__ == "__main__":
    main()
