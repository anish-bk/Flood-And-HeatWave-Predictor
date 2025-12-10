"""
Apache Cassandra Integration for Weather Data Pipeline
Demonstrates distributed NoSQL storage for time-series weather data.

This module shows how to:
1. Connect to a Cassandra cluster
2. Create keyspace and tables for weather data
3. Stream data into Cassandra
4. Query weather data with CQL
5. Perform aggregations and analytics

Prerequisites:
    pip install cassandra-driver

For local development, use Docker:
    docker run -d --name cassandra -p 9042:9042 cassandra:latest

Usage:
    python cassandra_integration.py --setup       # Create keyspace and tables
    python cassandra_integration.py --ingest      # Ingest data from CSV files
    python cassandra_integration.py --stream      # Simulate streaming to Cassandra
    python cassandra_integration.py --query       # Run sample queries
    python cassandra_integration.py --demo        # Full demo (simulated if no Cassandra)
"""

import os
import sys
import time
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np

# Try importing Cassandra driver
try:
    from cassandra.cluster import Cluster, NoHostAvailable
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.query import BatchStatement, SimpleStatement, ConsistencyLevel
    from cassandra import InvalidRequest
    CASSANDRA_AVAILABLE = True
except ImportError:
    CASSANDRA_AVAILABLE = False
    print("⚠️  cassandra-driver not installed. Run: pip install cassandra-driver")

# Colored output
try:
    from colorama import init, Fore, Style
    init()
except ImportError:
    class Fore:
        RED = GREEN = YELLOW = BLUE = CYAN = MAGENTA = WHITE = RESET = ''
    class Style:
        BRIGHT = DIM = RESET_ALL = ''


# ============================================================================
# CONFIGURATION
# ============================================================================

CASSANDRA_CONFIG = {
    'hosts': ['127.0.0.1'],  # Cassandra nodes
    'port': 9042,
    'keyspace': 'weather_data',
    'username': None,  # Set if authentication enabled
    'password': None,
}

DATA_DIR = "data/district_files"


# ============================================================================
# CASSANDRA DATA MODEL
# ============================================================================

# CQL statements for schema creation
CQL_CREATE_KEYSPACE = """
CREATE KEYSPACE IF NOT EXISTS weather_data
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
}
AND durable_writes = true;
"""

# Main weather observations table - partitioned by district and year
CQL_CREATE_WEATHER_TABLE = """
CREATE TABLE IF NOT EXISTS weather_data.weather_observations (
    district TEXT,
    year INT,
    date DATE,
    timestamp TIMESTAMP,
    latitude DOUBLE,
    longitude DOUBLE,
    max_temp DOUBLE,
    min_temp DOUBLE,
    temp_range DOUBLE,
    precipitation DOUBLE,
    humidity DOUBLE,
    wind_speed_10m DOUBLE,
    wind_speed_50m DOUBLE,
    solar_radiation DOUBLE,
    PRIMARY KEY ((district, year), date)
) WITH CLUSTERING ORDER BY (date DESC)
AND default_time_to_live = 0
AND gc_grace_seconds = 864000;
"""

# Aggregated daily statistics by district
CQL_CREATE_DAILY_STATS = """
CREATE TABLE IF NOT EXISTS weather_data.daily_stats (
    district TEXT,
    date DATE,
    avg_temp DOUBLE,
    max_temp DOUBLE,
    min_temp DOUBLE,
    total_precip DOUBLE,
    avg_humidity DOUBLE,
    heatwave_flag BOOLEAN,
    flood_risk_flag BOOLEAN,
    PRIMARY KEY (district, date)
) WITH CLUSTERING ORDER BY (date DESC);
"""

# Alerts table for real-time monitoring
CQL_CREATE_ALERTS = """
CREATE TABLE IF NOT EXISTS weather_data.weather_alerts (
    alert_id UUID,
    district TEXT,
    alert_type TEXT,
    severity TEXT,
    timestamp TIMESTAMP,
    temperature DOUBLE,
    precipitation DOUBLE,
    message TEXT,
    acknowledged BOOLEAN,
    PRIMARY KEY ((district), timestamp, alert_id)
) WITH CLUSTERING ORDER BY (timestamp DESC);
"""

# Streaming ingest log
CQL_CREATE_INGEST_LOG = """
CREATE TABLE IF NOT EXISTS weather_data.ingest_log (
    batch_id UUID,
    ingest_time TIMESTAMP,
    source_station TEXT,
    records_count INT,
    status TEXT,
    duration_ms INT,
    PRIMARY KEY ((source_station), ingest_time)
) WITH CLUSTERING ORDER BY (ingest_time DESC);
"""


# ============================================================================
# CASSANDRA CLIENT
# ============================================================================

class CassandraWeatherDB:
    """
    Cassandra client for weather data operations.
    
    This class demonstrates:
    - Connection management with retry logic
    - Schema creation and management
    - Batch inserts for streaming data
    - Time-series queries with CQL
    """
    
    def __init__(self, hosts: List[str] = None, port: int = 9042, 
                 keyspace: str = 'weather_data'):
        self.hosts = hosts or CASSANDRA_CONFIG['hosts']
        self.port = port
        self.keyspace = keyspace
        self.cluster = None
        self.session = None
        self.connected = False
        
        # Prepared statements (for performance)
        self.prepared_statements = {}
    
    def connect(self) -> bool:
        """Connect to Cassandra cluster."""
        if not CASSANDRA_AVAILABLE:
            print(f"{Fore.RED}❌ cassandra-driver not installed{Style.RESET_ALL}")
            return False
        
        try:
            print(f"{Fore.CYAN}🔌 Connecting to Cassandra at {self.hosts}:{self.port}...{Style.RESET_ALL}")
            
            # Authentication if needed
            auth_provider = None
            if CASSANDRA_CONFIG['username']:
                auth_provider = PlainTextAuthProvider(
                    username=CASSANDRA_CONFIG['username'],
                    password=CASSANDRA_CONFIG['password']
                )
            
            self.cluster = Cluster(
                contact_points=self.hosts,
                port=self.port,
                auth_provider=auth_provider
            )
            
            self.session = self.cluster.connect()
            self.connected = True
            
            print(f"{Fore.GREEN}✅ Connected to Cassandra cluster{Style.RESET_ALL}")
            return True
            
        except NoHostAvailable as e:
            print(f"{Fore.RED}❌ Cannot connect to Cassandra: {e}{Style.RESET_ALL}")
            print(f"{Fore.YELLOW}   Make sure Cassandra is running on {self.hosts}:{self.port}{Style.RESET_ALL}")
            return False
        except Exception as e:
            print(f"{Fore.RED}❌ Connection error: {e}{Style.RESET_ALL}")
            return False
    
    def close(self):
        """Close connection."""
        if self.cluster:
            self.cluster.shutdown()
            self.connected = False
            print(f"{Fore.YELLOW}🔌 Disconnected from Cassandra{Style.RESET_ALL}")
    
    def setup_schema(self):
        """Create keyspace and tables."""
        if not self.connected:
            print(f"{Fore.RED}❌ Not connected to Cassandra{Style.RESET_ALL}")
            return False
        
        print(f"\n{Fore.CYAN}📦 Setting up Cassandra schema...{Style.RESET_ALL}\n")
        
        statements = [
            ("Keyspace", CQL_CREATE_KEYSPACE),
            ("Weather Observations Table", CQL_CREATE_WEATHER_TABLE),
            ("Daily Stats Table", CQL_CREATE_DAILY_STATS),
            ("Alerts Table", CQL_CREATE_ALERTS),
            ("Ingest Log Table", CQL_CREATE_INGEST_LOG),
        ]
        
        for name, cql in statements:
            try:
                self.session.execute(cql)
                print(f"   {Fore.GREEN}✓{Style.RESET_ALL} Created {name}")
            except Exception as e:
                print(f"   {Fore.RED}✗{Style.RESET_ALL} Failed to create {name}: {e}")
        
        # Use the keyspace
        self.session.set_keyspace(self.keyspace)
        
        print(f"\n{Fore.GREEN}✅ Schema setup complete{Style.RESET_ALL}")
        return True
    
    def prepare_statements(self):
        """Prepare CQL statements for better performance."""
        if not self.connected:
            return
        
        self.session.set_keyspace(self.keyspace)
        
        # Insert weather observation
        self.prepared_statements['insert_weather'] = self.session.prepare("""
            INSERT INTO weather_observations 
            (district, year, date, timestamp, latitude, longitude, max_temp, min_temp, 
             temp_range, precipitation, humidity, wind_speed_10m, wind_speed_50m, solar_radiation)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        
        # Insert alert
        self.prepared_statements['insert_alert'] = self.session.prepare("""
            INSERT INTO weather_alerts 
            (alert_id, district, alert_type, severity, timestamp, temperature, precipitation, message, acknowledged)
            VALUES (uuid(), ?, ?, ?, ?, ?, ?, ?, false)
        """)
        
        print(f"{Fore.GREEN}✓ Prepared statements ready{Style.RESET_ALL}")
    
    def insert_weather_batch(self, records: List[Dict]) -> int:
        """Insert a batch of weather records."""
        if not self.connected:
            return 0
        
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        count = 0
        
        for record in records:
            try:
                date_val = record.get('Date')
                if isinstance(date_val, str):
                    date_val = datetime.strptime(date_val[:10], '%Y-%m-%d')
                elif isinstance(date_val, pd.Timestamp):
                    date_val = date_val.to_pydatetime()
                
                year = date_val.year if date_val else 2024
                
                batch.add(self.prepared_statements['insert_weather'], (
                    record.get('District', 'Unknown'),
                    year,
                    date_val.date() if date_val else None,
                    datetime.now(),
                    float(record.get('Latitude', 0)),
                    float(record.get('Longitude', 0)),
                    float(record.get('MaxTemp_2m', 0)),
                    float(record.get('MinTemp_2m', 0)),
                    float(record.get('TempRange_2m', 0)),
                    float(record.get('Precip', 0)),
                    float(record.get('RH_2m', 0)),
                    float(record.get('WindSpeed_10m', 0)),
                    float(record.get('WindSpeed_50m', 0)),
                    float(record.get('SolarRad', 0)),
                ))
                count += 1
                
                # Execute batch every 50 records
                if count % 50 == 0:
                    self.session.execute(batch)
                    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
                    
            except Exception as e:
                print(f"{Fore.YELLOW}⚠️ Skipping record: {e}{Style.RESET_ALL}")
        
        # Execute remaining
        if count % 50 != 0:
            self.session.execute(batch)
        
        return count
    
    def insert_alert(self, district: str, alert_type: str, severity: str,
                     temperature: float, precipitation: float, message: str):
        """Insert a weather alert."""
        if not self.connected or 'insert_alert' not in self.prepared_statements:
            return
        
        self.session.execute(self.prepared_statements['insert_alert'], (
            district, alert_type, severity, datetime.now(),
            temperature, precipitation, message
        ))
    
    def query_weather_by_district(self, district: str, year: int, limit: int = 100):
        """Query weather data for a district."""
        if not self.connected:
            return []
        
        cql = f"""
            SELECT date, max_temp, precipitation, humidity 
            FROM weather_observations 
            WHERE district = %s AND year = %s 
            LIMIT {limit}
        """
        
        rows = self.session.execute(cql, (district, year))
        return list(rows)
    
    def query_recent_alerts(self, district: str = None, limit: int = 20):
        """Query recent weather alerts."""
        if not self.connected:
            return []
        
        if district:
            cql = f"""
                SELECT district, alert_type, severity, timestamp, message 
                FROM weather_alerts 
                WHERE district = %s
                LIMIT {limit}
            """
            rows = self.session.execute(cql, (district,))
        else:
            # Note: In production, use a different partition strategy for global queries
            cql = f"""
                SELECT district, alert_type, severity, timestamp, message 
                FROM weather_alerts 
                LIMIT {limit}
            """
            rows = self.session.execute(cql)
        
        return list(rows)
    
    def get_district_stats(self, district: str, year: int) -> Dict:
        """Get aggregated statistics for a district."""
        if not self.connected:
            return {}
        
        cql = """
            SELECT 
                AVG(max_temp) as avg_temp,
                MAX(max_temp) as max_temp,
                SUM(precipitation) as total_precip,
                COUNT(*) as record_count
            FROM weather_observations 
            WHERE district = %s AND year = %s
        """
        
        rows = self.session.execute(cql, (district, year))
        row = rows.one()
        
        if row:
            return {
                'avg_temp': row.avg_temp,
                'max_temp': row.max_temp,
                'total_precip': row.total_precip,
                'record_count': row.record_count
            }
        return {}


# ============================================================================
# STREAMING SIMULATION WITH CASSANDRA
# ============================================================================

class CassandraStreamingPipeline:
    """
    Simulates streaming data ingestion to Cassandra.
    
    Architecture:
    
    Weather Stations → Kafka (simulated) → Cassandra → Analytics
          │                   │                │           │
       CSV Files       Batch Queue      Time-Series     Queries
                                          Storage
    """
    
    def __init__(self, data_dir: str = DATA_DIR, simulated: bool = False):
        self.data_dir = data_dir
        self.simulated = simulated  # Run without real Cassandra
        self.db = CassandraWeatherDB()
        self.stations = {}
        self.stats = {
            'records_ingested': 0,
            'batches_processed': 0,
            'alerts_generated': 0,
            'start_time': None
        }
    
    def initialize(self) -> bool:
        """Initialize connection and schema."""
        if self.simulated:
            print(f"\n{Fore.YELLOW}🎭 Running in SIMULATED mode (no real Cassandra){Style.RESET_ALL}\n")
            return True
        
        if not self.db.connect():
            print(f"\n{Fore.YELLOW}💡 Tip: Start Cassandra with Docker:{Style.RESET_ALL}")
            print(f"   docker run -d --name cassandra -p 9042:9042 cassandra:latest\n")
            return False
        
        self.db.setup_schema()
        self.db.prepare_statements()
        return True
    
    def load_stations(self):
        """Load data from CSV files."""
        print(f"\n{Fore.CYAN}📂 Loading weather station data...{Style.RESET_ALL}\n")
        
        if not os.path.exists(self.data_dir):
            print(f"{Fore.RED}❌ Data directory not found: {self.data_dir}{Style.RESET_ALL}")
            return
        
        csv_files = sorted([f for f in os.listdir(self.data_dir) if f.endswith('.csv')])
        
        for csv_file in csv_files:
            district = csv_file.replace('.csv', '')
            file_path = os.path.join(self.data_dir, csv_file)
            
            df = pd.read_csv(file_path, parse_dates=['Date'])
            df['District'] = district
            self.stations[district] = {
                'data': df,
                'current_index': 0,
                'total_records': len(df)
            }
            
            print(f"   {Fore.GREEN}✓{Style.RESET_ALL} {district}: {len(df):,} records")
        
        print(f"\n   {Fore.CYAN}Total: {len(self.stations)} stations loaded{Style.RESET_ALL}")
    
    def stream_to_cassandra(self, batch_size: int = 100, max_batches: int = 20):
        """Stream data to Cassandra in batches."""
        print(f"\n{'='*70}")
        print(f"{Fore.CYAN}  🌊 STREAMING DATA TO CASSANDRA{Style.RESET_ALL}")
        print(f"{'='*70}\n")
        
        self.stats['start_time'] = datetime.now()
        
        for batch_num in range(1, max_batches + 1):
            batch_records = []
            
            # Collect records from all stations (round-robin)
            for district, station in self.stations.items():
                idx = station['current_index']
                if idx < station['total_records']:
                    # Get a few records per station
                    end_idx = min(idx + 10, station['total_records'])
                    records = station['data'].iloc[idx:end_idx].to_dict('records')
                    batch_records.extend(records)
                    station['current_index'] = end_idx
            
            if not batch_records:
                print(f"\n{Fore.YELLOW}📭 No more records to stream{Style.RESET_ALL}")
                break
            
            # Process batch
            self.process_batch(batch_num, batch_records)
            
            # Simulate processing time
            time.sleep(0.3)
        
        self.print_summary()
    
    def process_batch(self, batch_num: int, records: List[Dict]):
        """Process a batch of records."""
        start_time = time.time()
        
        print(f"{Fore.MAGENTA}📦 Batch #{batch_num}{Style.RESET_ALL} | ", end='')
        print(f"Records: {len(records)} | ", end='')
        
        # Insert to Cassandra (or simulate)
        if self.simulated:
            # Simulate insert
            time.sleep(0.1)
            inserted = len(records)
        else:
            inserted = self.db.insert_weather_batch(records)
        
        self.stats['records_ingested'] += inserted
        self.stats['batches_processed'] += 1
        
        # Check for alerts
        alerts = self.check_alerts(records)
        self.stats['alerts_generated'] += len(alerts)
        
        duration = (time.time() - start_time) * 1000
        
        # Calculate stats
        df = pd.DataFrame(records)
        avg_temp = df['MaxTemp_2m'].mean() if 'MaxTemp_2m' in df.columns else 0
        total_precip = df['Precip'].sum() if 'Precip' in df.columns else 0
        
        print(f"Avg Temp: {Fore.YELLOW}{avg_temp:.1f}°C{Style.RESET_ALL} | ", end='')
        print(f"Precip: {Fore.CYAN}{total_precip:.1f}mm{Style.RESET_ALL} | ", end='')
        print(f"⏱️ {duration:.0f}ms", end='')
        
        if alerts:
            print(f" | {Fore.RED}⚠️ {len(alerts)} alerts{Style.RESET_ALL}")
        else:
            print()
    
    def check_alerts(self, records: List[Dict]) -> List[Dict]:
        """Check records for alert conditions."""
        alerts = []
        
        for record in records:
            district = record.get('District', 'Unknown')
            temp = record.get('MaxTemp_2m', 0)
            precip = record.get('Precip', 0)
            
            # Heatwave alert
            if temp > 40:
                alert = {
                    'district': district,
                    'type': 'HEATWAVE',
                    'severity': 'HIGH' if temp > 45 else 'MEDIUM',
                    'temperature': temp,
                    'message': f'Extreme temperature: {temp:.1f}°C'
                }
                alerts.append(alert)
                
                if not self.simulated:
                    self.db.insert_alert(
                        district, 'HEATWAVE', alert['severity'],
                        temp, precip, alert['message']
                    )
            
            # Flood risk alert
            if precip > 100:
                alert = {
                    'district': district,
                    'type': 'FLOOD_RISK',
                    'severity': 'HIGH' if precip > 150 else 'MEDIUM',
                    'precipitation': precip,
                    'message': f'Heavy precipitation: {precip:.1f}mm'
                }
                alerts.append(alert)
                
                if not self.simulated:
                    self.db.insert_alert(
                        district, 'FLOOD_RISK', alert['severity'],
                        temp, precip, alert['message']
                    )
        
        return alerts
    
    def print_summary(self):
        """Print streaming summary."""
        duration = (datetime.now() - self.stats['start_time']).total_seconds()
        throughput = self.stats['records_ingested'] / duration if duration > 0 else 0
        
        print(f"\n{'='*70}")
        print(f"{Fore.GREEN}  ✅ STREAMING COMPLETE{Style.RESET_ALL}")
        print(f"{'='*70}")
        print(f"""
   📊 Records Ingested:    {Fore.GREEN}{self.stats['records_ingested']:,}{Style.RESET_ALL}
   📦 Batches Processed:   {Fore.GREEN}{self.stats['batches_processed']}{Style.RESET_ALL}
   ⚠️  Alerts Generated:    {Fore.RED}{self.stats['alerts_generated']}{Style.RESET_ALL}
   ⏱️  Duration:            {Fore.YELLOW}{duration:.1f} seconds{Style.RESET_ALL}
   🚀 Throughput:          {Fore.CYAN}{throughput:.0f} records/sec{Style.RESET_ALL}
""")
    
    def run_sample_queries(self):
        """Run sample CQL queries."""
        if self.simulated:
            print(f"\n{Fore.YELLOW}🎭 Query simulation (no real Cassandra){Style.RESET_ALL}")
            self.show_sample_queries()
            return
        
        print(f"\n{'='*70}")
        print(f"{Fore.CYAN}  📊 SAMPLE CASSANDRA QUERIES{Style.RESET_ALL}")
        print(f"{'='*70}\n")
        
        # Query weather data
        print(f"{Fore.YELLOW}Query 1: Recent weather for Chitawan (2020){Style.RESET_ALL}")
        results = self.db.query_weather_by_district('Chitawan', 2020, limit=5)
        for row in results[:5]:
            print(f"   {row.date} | Temp: {row.max_temp:.1f}°C | Precip: {row.precipitation:.1f}mm")
        
        # Query alerts
        print(f"\n{Fore.YELLOW}Query 2: Recent alerts{Style.RESET_ALL}")
        alerts = self.db.query_recent_alerts(limit=5)
        for alert in alerts[:5]:
            print(f"   [{alert.severity}] {alert.district}: {alert.message}")
    
    def show_sample_queries(self):
        """Show sample CQL queries that would be used."""
        print(f"""
{Fore.CYAN}Sample CQL Queries for Weather Data:{Style.RESET_ALL}

{Fore.YELLOW}1. Get recent weather for a district:{Style.RESET_ALL}
   SELECT date, max_temp, precipitation, humidity 
   FROM weather_observations 
   WHERE district = 'Chitawan' AND year = 2024 
   LIMIT 10;

{Fore.YELLOW}2. Get high temperature days:{Style.RESET_ALL}
   SELECT date, max_temp, district 
   FROM weather_observations 
   WHERE district = 'Bara' AND year = 2024 
   AND max_temp > 40
   ALLOW FILTERING;

{Fore.YELLOW}3. Get recent alerts for a district:{Style.RESET_ALL}
   SELECT alert_type, severity, timestamp, message 
   FROM weather_alerts 
   WHERE district = 'Saptari'
   LIMIT 20;

{Fore.YELLOW}4. Aggregate statistics:{Style.RESET_ALL}
   SELECT district, AVG(max_temp), MAX(max_temp), SUM(precipitation)
   FROM weather_observations 
   WHERE district = 'Makwanpur' AND year = 2024;
""")
    
    def close(self):
        """Close connections."""
        if not self.simulated:
            self.db.close()


# ============================================================================
# MAIN
# ============================================================================

def show_architecture():
    """Show the Cassandra integration architecture."""
    print(f"""
{Fore.CYAN}{Style.BRIGHT}
╔══════════════════════════════════════════════════════════════════════════════╗
║                                                                              ║
║   🗄️  APACHE CASSANDRA INTEGRATION FOR WEATHER DATA PIPELINE                 ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
{Style.RESET_ALL}

{Fore.YELLOW}ARCHITECTURE:{Style.RESET_ALL}

    ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
    │  Weather        │     │  Streaming      │     │  Apache         │
    │  Stations       │────▶│  Layer          │────▶│  Cassandra      │
    │  (10 Districts) │     │  (Batch Queue)  │     │  (NoSQL DB)     │
    └─────────────────┘     └─────────────────┘     └─────────────────┘
                                                            │
                                                            ▼
    ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
    │  ML             │◀────│  Analytics      │◀────│  Time-Series    │
    │  Predictions    │     │  Engine         │     │  Queries        │
    └─────────────────┘     └─────────────────┘     └─────────────────┘

{Fore.YELLOW}DATA MODEL:{Style.RESET_ALL}

    ┌─────────────────────────────────────────────────────────────────┐
    │  Keyspace: weather_data                                         │
    ├─────────────────────────────────────────────────────────────────┤
    │  Tables:                                                        │
    │  • weather_observations - Partitioned by (district, year)       │
    │  • daily_stats - Aggregated daily statistics                    │
    │  • weather_alerts - Real-time alert tracking                    │
    │  • ingest_log - Streaming audit log                             │
    └─────────────────────────────────────────────────────────────────┘

{Fore.YELLOW}WHY CASSANDRA?{Style.RESET_ALL}

    ✓ Linear scalability for massive time-series data
    ✓ High write throughput for streaming ingestion
    ✓ Distributed architecture matches weather station topology
    ✓ Time-based partitioning for efficient queries
    ✓ No single point of failure
""")


def main():
    parser = argparse.ArgumentParser(
        description="Apache Cassandra Integration for Weather Data"
    )
    parser.add_argument('--setup', action='store_true',
                       help='Create keyspace and tables')
    parser.add_argument('--ingest', action='store_true',
                       help='Ingest data from CSV files')
    parser.add_argument('--stream', action='store_true',
                       help='Simulate streaming to Cassandra')
    parser.add_argument('--query', action='store_true',
                       help='Run sample queries')
    parser.add_argument('--demo', action='store_true',
                       help='Full demo (simulated if no Cassandra)')
    parser.add_argument('--batches', type=int, default=15,
                       help='Number of batches to stream (default: 15)')
    args = parser.parse_args()
    
    show_architecture()
    
    # Default to demo mode
    if not any([args.setup, args.ingest, args.stream, args.query, args.demo]):
        args.demo = True
    
    # Check if Cassandra is available
    simulated = False
    if args.demo:
        # Try to connect, fall back to simulation
        test_db = CassandraWeatherDB()
        if not test_db.connect():
            simulated = True
            print(f"\n{Fore.YELLOW}💡 Running in simulation mode for demo{Style.RESET_ALL}")
        else:
            test_db.close()
    
    # Initialize pipeline
    pipeline = CassandraStreamingPipeline(simulated=simulated)
    
    try:
        if args.setup or args.demo:
            pipeline.initialize()
        
        if args.ingest or args.stream or args.demo:
            pipeline.load_stations()
            pipeline.stream_to_cassandra(max_batches=args.batches)
        
        if args.query or args.demo:
            pipeline.run_sample_queries()
            
    finally:
        pipeline.close()
    
    print(f"\n{Fore.GREEN}✅ Cassandra demo complete!{Style.RESET_ALL}\n")


if __name__ == "__main__":
    main()
