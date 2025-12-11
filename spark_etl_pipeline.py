"""
Spark ETL Pipeline for Weather Data (Big Data Lambda Architecture)
===================================================================
This pipeline implements a proper Lambda Architecture with:

    INGESTION LAYER:
        OpenWeatherMap API → Kafka Producer → Kafka Topic

    SPEED LAYER (Real-time):
        Kafka Topic → Spark Streaming → Cassandra (immediate insights)

    BATCH LAYER (Historical):
        Kafka Topic → Spark Streaming → HDFS (raw data lake)
        HDFS → Spark Batch → Cassandra (recomputed views)

Components:
    - Kafka: Message queue for decoupling and buffering
    - HDFS: Distributed file system for raw data persistence (Data Lake)
    - Spark: Distributed stream/batch processing
    - Cassandra: Distributed time-series storage for serving layer

Modes:
    stream-kafka: Kafka → Spark → HDFS + Cassandra (RECOMMENDED - Lambda Architecture)
    batch-hdfs:   HDFS → Spark Batch → Cassandra (Batch reprocessing)
    stream:       Direct API fetch → Spark → Cassandra (fallback)
    all:          Full ETL pipeline
    extract:      Fetch to files
    transform:    Transform files
    load:         Load to Cassandra

Usage (Recommended - Full Lambda Architecture):
    1. Start infrastructure: docker-compose up -d
    2. Wait for HDFS: Check http://localhost:9870 (NameNode UI)
    3. Start Kafka producer: python kafka_producer_api.py
    4. Start Spark consumer: python spark_etl_pipeline.py --mode stream-kafka
    5. Start dashboard: python weather_dashboard.py

HDFS Web UIs:
    - NameNode: http://localhost:9870
    - DataNode: http://localhost:9864
"""

import os
import sys
import json
import time
import argparse
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, avg, max as spark_max, min as spark_min,
    count, sum as spark_sum, lag, lead, window, udf,
    from_json, to_json, struct, current_timestamp,
    year, month, dayofmonth, hour, minute, dayofyear,
    sin, cos, round as spark_round, coalesce, explode
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, TimestampType, FloatType, ArrayType
)
from pyspark.sql.window import Window

# Configuration
CONFIG = {
    'openweathermap_api_key': os.environ.get('OPENWEATHERMAP_API_KEY', ''),
    'kafka_bootstrap_servers': 'localhost:9092',
    'kafka_topic': 'weather-data',
    'cassandra_host': '127.0.0.1',
    'cassandra_port': 9042,
    'cassandra_keyspace': 'weather_monitoring',
    'spark_app_name': 'WeatherETLPipeline',
    'spark_master': 'local[*]',
    # HDFS Configuration (Lambda Architecture - Batch Layer)
    'hdfs_namenode': 'hdfs://localhost:9000',
    'hdfs_raw_path': '/weather/raw',           # Raw data lake
    'hdfs_processed_path': '/weather/processed', # Processed data
    'hdfs_checkpoint_path': '/weather/checkpoints', # Streaming checkpoints
}

# Districts to monitor (Nepal Terai region)
DISTRICTS = {
    'Bara': {'lat': 27.0667, 'lon': 85.0667},
    'Dhanusa': {'lat': 26.8167, 'lon': 86.0000},
    'Sarlahi': {'lat': 26.8667, 'lon': 85.5833},
    'Parsa': {'lat': 27.1333, 'lon': 84.8833},
    'Siraha': {'lat': 26.6536, 'lon': 86.2019},
}

# Schema for raw weather data
RAW_WEATHER_SCHEMA = StructType([
    StructField("district", StringType(), False),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("fetch_time", TimestampType(), False),
    StructField("date", StringType(), True),
    StructField("temp", DoubleType(), True),
    StructField("temp_min", DoubleType(), True),
    StructField("temp_max", DoubleType(), True),
    StructField("feels_like", DoubleType(), True),
    StructField("pressure", IntegerType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_deg", IntegerType(), True),
    StructField("clouds", IntegerType(), True),
    StructField("visibility", IntegerType(), True),
    StructField("rain_1h", DoubleType(), True),
    StructField("rain_3h", DoubleType(), True),
    StructField("snow_1h", DoubleType(), True),
    StructField("weather_main", StringType(), True),
    StructField("weather_description", StringType(), True),
    StructField("sunrise", TimestampType(), True),
    StructField("sunset", TimestampType(), True),
    StructField("source", StringType(), True),
])


def create_spark_session(app_name: str = None, with_cassandra: bool = True, with_kafka: bool = False, with_hdfs: bool = False) -> SparkSession:
    """Create and configure Spark session with HDFS, Kafka, and Cassandra support."""
    app_name = app_name or CONFIG['spark_app_name']
    
    packages = []
    if with_cassandra:
        packages.append("com.datastax.spark:spark-cassandra-connector_2.12:3.4.1")
    if with_kafka:
        packages.append("org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    
    builder = SparkSession.builder \
        .appName(app_name) \
        .master(CONFIG['spark_master']) \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    
    # HDFS Configuration
    if with_hdfs:
        builder = builder \
            .config("spark.hadoop.fs.defaultFS", CONFIG['hdfs_namenode']) \
            .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
    
    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))
    
    if with_cassandra:
        # Add Cassandra connector configuration
        builder = builder \
            .config("spark.cassandra.connection.host", CONFIG['cassandra_host']) \
            .config("spark.cassandra.connection.port", CONFIG['cassandra_port'])
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"✅ Spark session created: {app_name}")
    print(f"   Master: {CONFIG['spark_master']}")
    print(f"   Version: {spark.version}")
    if with_hdfs:
        print(f"   HDFS: {CONFIG['hdfs_namenode']}")
    
    return spark


# =============================================================================
# HDFS UTILITIES (Data Lake Operations)
# =============================================================================

def check_hdfs_connection() -> bool:
    """Check if HDFS is available and accessible."""
    try:
        import subprocess
        # Try to list HDFS root via WebHDFS
        result = subprocess.run(
            ['curl', '-s', '-o', '/dev/null', '-w', '%{http_code}', 'http://localhost:9870'],
            capture_output=True, text=True, timeout=5, shell=True
        )
        return result.stdout.strip() == '200'
    except Exception:
        return False


def check_hdfs_datanode_ready() -> bool:
    """Check if HDFS DataNode is registered and ready for writes."""
    try:
        import urllib.request
        import json
        # Query NameNode JMX for live DataNodes
        url = "http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo"
        with urllib.request.urlopen(url, timeout=5) as response:
            data = json.loads(response.read().decode())
            beans = data.get('beans', [{}])
            if beans:
                live_nodes = beans[0].get('LiveNodes', '{}')
                if isinstance(live_nodes, str):
                    live_nodes = json.loads(live_nodes)
                # Check if at least one DataNode is live
                return len(live_nodes) > 0
    except Exception as e:
        print(f"   ⚠️ Could not check HDFS DataNode status: {e}")
    return False


def setup_hdfs_directories(spark: SparkSession) -> bool:
    """Create HDFS directories for weather data lake."""
    try:
        # Get Hadoop FileSystem
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.defaultFS", CONFIG['hdfs_namenode'])
        
        uri = spark._jvm.java.net.URI(CONFIG['hdfs_namenode'])
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
        
        # Create directories
        directories = [
            CONFIG['hdfs_raw_path'],
            CONFIG['hdfs_processed_path'],
            CONFIG['hdfs_checkpoint_path'],
            f"{CONFIG['hdfs_raw_path']}/weather-data",
            f"{CONFIG['hdfs_checkpoint_path']}/kafka_stream",
        ]
        
        for dir_path in directories:
            path = spark._jvm.org.apache.hadoop.fs.Path(dir_path)
            if not fs.exists(path):
                fs.mkdirs(path)
                print(f"   📁 Created HDFS directory: {dir_path}")
            else:
                print(f"   ✓ HDFS directory exists: {dir_path}")
        
        return True
    except Exception as e:
        print(f"   ⚠️ HDFS setup warning: {e}")
        return False


def get_hdfs_checkpoint_path() -> str:
    """Get the appropriate checkpoint path (HDFS if available, local fallback)."""
    if check_hdfs_connection():
        return f"{CONFIG['hdfs_namenode']}{CONFIG['hdfs_checkpoint_path']}/kafka_stream"
    else:
        # Fallback to local directory
        local_checkpoint = os.path.join(os.getcwd(), "data", "checkpoints", "kafka_stream")
        os.makedirs(local_checkpoint, exist_ok=True)
        return local_checkpoint


def get_hdfs_raw_path() -> str:
    """Get the HDFS raw data path (or local fallback)."""
    if check_hdfs_connection():
        return f"{CONFIG['hdfs_namenode']}{CONFIG['hdfs_raw_path']}/weather-data"
    else:
        local_path = os.path.join(os.getcwd(), "data", "raw")
        os.makedirs(local_path, exist_ok=True)
        return local_path


# =============================================================================
# EXTRACT: Fetch data from OpenWeatherMap API
# =============================================================================

class WeatherExtractor:
    """Extract weather data from OpenWeatherMap API."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.openweathermap.org/data/2.5/weather"
        self.stats = {'fetched': 0, 'errors': 0}
    
    def fetch_weather(self, district: str, lat: float, lon: float) -> Optional[Dict]:
        """Fetch current weather for a location."""
        if not self.api_key:
            print("❌ OpenWeatherMap API key not set")
            return None
        
        try:
            params = {
                'lat': lat,
                'lon': lon,
                'appid': self.api_key,
                'units': 'metric'
            }
            
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # Parse response into our schema
            weather_record = {
                'district': district,
                'latitude': lat,
                'longitude': lon,
                'fetch_time': datetime.utcnow(),
                'date': datetime.utcnow().strftime('%Y-%m-%d'),
                'temp': data['main'].get('temp'),
                'temp_min': data['main'].get('temp_min'),
                'temp_max': data['main'].get('temp_max'),
                'feels_like': data['main'].get('feels_like'),
                'pressure': data['main'].get('pressure'),
                'humidity': data['main'].get('humidity'),
                'wind_speed': data['wind'].get('speed'),
                'wind_deg': data['wind'].get('deg'),
                'clouds': data['clouds'].get('all'),
                'visibility': data.get('visibility'),
                'rain_1h': data.get('rain', {}).get('1h', 0.0),
                'rain_3h': data.get('rain', {}).get('3h', 0.0),
                'snow_1h': data.get('snow', {}).get('1h', 0.0),
                'weather_main': data['weather'][0]['main'] if data.get('weather') else None,
                'weather_description': data['weather'][0]['description'] if data.get('weather') else None,
                'sunrise': datetime.fromtimestamp(data['sys']['sunrise']) if data.get('sys', {}).get('sunrise') else None,
                'sunset': datetime.fromtimestamp(data['sys']['sunset']) if data.get('sys', {}).get('sunset') else None,
                'source': 'openweathermap'
            }
            
            self.stats['fetched'] += 1
            return weather_record
            
        except Exception as e:
            print(f"❌ Error fetching {district}: {e}")
            self.stats['errors'] += 1
            return None
    
    def fetch_all_districts(self) -> List[Dict]:
        """Fetch weather for all configured districts."""
        records = []
        for district, coords in DISTRICTS.items():
            record = self.fetch_weather(district, coords['lat'], coords['lon'])
            if record:
                records.append(record)
                print(f"   ✓ {district}: {record['temp']:.1f}°C, {record['humidity']}% humidity")
        return records


def extract_to_spark(spark: SparkSession, extractor: WeatherExtractor) -> DataFrame:
    """Extract weather data and load into Spark DataFrame."""
    print("\n📥 EXTRACT: Fetching weather data from OpenWeatherMap...")
    
    records = extractor.fetch_all_districts()
    
    if not records:
        print("❌ No data extracted")
        return spark.createDataFrame([], RAW_WEATHER_SCHEMA)
    
    # Convert to Spark DataFrame
    df = spark.createDataFrame(records, RAW_WEATHER_SCHEMA)
    
    print(f"✅ Extracted {df.count()} records from {len(DISTRICTS)} districts")
    return df


def extract_from_files(spark: SparkSession, input_path: str) -> DataFrame:
    """Extract weather data from parquet/JSON files."""
    print(f"\n📥 EXTRACT: Loading data from {input_path}...")
    
    if input_path.endswith('.json'):
        df = spark.read.schema(RAW_WEATHER_SCHEMA).json(input_path)
    else:
        df = spark.read.parquet(input_path)
    
    print(f"✅ Loaded {df.count()} records")
    return df


# =============================================================================
# TRANSFORM: Process and engineer features using Spark
# =============================================================================

def transform_weather_data(df: DataFrame) -> DataFrame:
    """
    Transform raw weather data:
    - Clean and validate data
    - Engineer features for ML models
    - Add temporal features
    - Calculate derived metrics
    """
    print("\n🔄 TRANSFORM: Processing weather data with Spark...")
    
    # Step 1: Data cleaning - handle nulls and outliers
    print("   Step 1: Cleaning data...")
    df_cleaned = df \
        .withColumn("temp", when(col("temp").between(-50, 60), col("temp")).otherwise(None)) \
        .withColumn("humidity", when(col("humidity").between(0, 100), col("humidity")).otherwise(None)) \
        .withColumn("pressure", when(col("pressure").between(800, 1100), col("pressure")).otherwise(None)) \
        .withColumn("wind_speed", when(col("wind_speed") >= 0, col("wind_speed")).otherwise(0)) \
        .withColumn("rain_1h", coalesce(col("rain_1h"), lit(0.0))) \
        .withColumn("rain_3h", coalesce(col("rain_3h"), lit(0.0))) \
        .withColumn("snow_1h", coalesce(col("snow_1h"), lit(0.0))) \
        .withColumn("visibility", coalesce(col("visibility"), lit(10000)))
    
    # Step 2: Add temporal features
    print("   Step 2: Adding temporal features...")
    df_temporal = df_cleaned \
        .withColumn("year", year(col("fetch_time"))) \
        .withColumn("month", month(col("fetch_time"))) \
        .withColumn("day", dayofmonth(col("fetch_time"))) \
        .withColumn("hour", hour(col("fetch_time"))) \
        .withColumn("day_of_year", dayofyear(col("fetch_time"))) \
        .withColumn("doy_sin", sin(2 * 3.14159 * col("day_of_year") / 365)) \
        .withColumn("doy_cos", cos(2 * 3.14159 * col("day_of_year") / 365)) \
        .withColumn("hour_sin", sin(2 * 3.14159 * col("hour") / 24)) \
        .withColumn("hour_cos", cos(2 * 3.14159 * col("hour") / 24))
    
    # Step 3: Calculate derived weather metrics
    print("   Step 3: Calculating derived metrics...")
    df_derived = df_temporal \
        .withColumn("temp_range", col("temp_max") - col("temp_min")) \
        .withColumn("visibility_km", col("visibility") / 1000.0) \
        .withColumn("total_precip", col("rain_1h") + col("snow_1h")) \
        .withColumn("is_rainy", when(col("rain_1h") > 0, 1).otherwise(0)) \
        .withColumn("is_cloudy", when(col("clouds") > 50, 1).otherwise(0)) \
        .withColumn("is_windy", when(col("wind_speed") > 10, 1).otherwise(0))
    
    # Step 4: Calculate heat index (simplified formula)
    print("   Step 4: Calculating heat index...")
    df_heat = df_derived \
        .withColumn("heat_index", 
            when(col("temp") >= 27,
                 -8.785 + 1.611 * col("temp") + 2.339 * col("humidity") 
                 - 0.146 * col("temp") * col("humidity"))
            .otherwise(col("temp")))
    
    # Step 5: Risk indicators
    print("   Step 5: Adding risk indicators...")
    df_risk = df_heat \
        .withColumn("heatwave_indicator", 
            when(col("temp_max") >= 40, 3)
            .when(col("temp_max") >= 35, 2)
            .when(col("temp_max") >= 32, 1)
            .otherwise(0)) \
        .withColumn("flood_indicator",
            when(col("rain_1h") >= 50, 3)
            .when(col("rain_1h") >= 20, 2)
            .when(col("rain_1h") >= 5, 1)
            .otherwise(0)) \
        .withColumn("humidity_risk",
            when(col("humidity") >= 90, 2)
            .when(col("humidity") >= 80, 1)
            .otherwise(0))
    
    # Step 6: Add processing metadata
    print("   Step 6: Adding metadata...")
    df_final = df_risk \
        .withColumn("etl_timestamp", current_timestamp()) \
        .withColumn("etl_version", lit("1.0"))
    
    record_count = df_final.count()
    print(f"✅ Transformed {record_count} records")
    
    return df_final


def transform_with_aggregations(df: DataFrame, spark: SparkSession) -> Dict[str, DataFrame]:
    """
    Create aggregated views for analytics:
    - Hourly aggregates per district
    - Daily summaries
    - Risk alerts
    """
    print("\n📊 Creating aggregated views...")
    
    results = {}
    
    # Hourly aggregates per district
    print("   Creating hourly aggregates...")
    df_hourly = df.groupBy("district", "date", "hour") \
        .agg(
            spark_round(avg("temp"), 2).alias("avg_temp"),
            spark_round(spark_max("temp_max"), 2).alias("max_temp"),
            spark_round(spark_min("temp_min"), 2).alias("min_temp"),
            spark_round(avg("humidity"), 1).alias("avg_humidity"),
            spark_round(spark_sum("rain_1h"), 2).alias("total_rain"),
            spark_round(avg("wind_speed"), 2).alias("avg_wind"),
            spark_max("heatwave_indicator").alias("max_heatwave_risk"),
            spark_max("flood_indicator").alias("max_flood_risk"),
            count("*").alias("record_count")
        )
    results['hourly'] = df_hourly
    
    # Daily summaries
    print("   Creating daily summaries...")
    df_daily = df.groupBy("district", "date") \
        .agg(
            spark_round(avg("temp"), 2).alias("avg_temp"),
            spark_round(spark_max("temp_max"), 2).alias("max_temp"),
            spark_round(spark_min("temp_min"), 2).alias("min_temp"),
            spark_round(avg("humidity"), 1).alias("avg_humidity"),
            spark_round(spark_sum("total_precip"), 2).alias("total_precip"),
            spark_round(avg("wind_speed"), 2).alias("avg_wind"),
            spark_sum("is_rainy").alias("rainy_hours"),
            spark_max("heatwave_indicator").alias("heatwave_risk_level"),
            spark_max("flood_indicator").alias("flood_risk_level"),
            count("*").alias("observation_count")
        ) \
        .withColumn("etl_timestamp", current_timestamp())
    results['daily'] = df_daily
    
    # High-risk alerts
    print("   Identifying risk alerts...")
    df_alerts = df.filter(
        (col("heatwave_indicator") >= 2) | (col("flood_indicator") >= 2)
    ).select(
        "district", "fetch_time", "date",
        "temp_max", "rain_1h", "humidity",
        "heatwave_indicator", "flood_indicator",
        when(col("heatwave_indicator") >= 2, "HEATWAVE")
        .when(col("flood_indicator") >= 2, "FLOOD")
        .otherwise("UNKNOWN").alias("alert_type")
    )
    results['alerts'] = df_alerts
    
    print(f"✅ Created {len(results)} aggregated views")
    return results


# =============================================================================
# LOAD: Write to Cassandra and/or files
# =============================================================================

def load_to_cassandra(df: DataFrame, table_name: str, keyspace: str = None):
    """Load DataFrame to Cassandra table."""
    keyspace = keyspace or CONFIG['cassandra_keyspace']
    
    print(f"\n📤 LOAD: Writing to Cassandra {keyspace}.{table_name}...")
    
    try:
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=table_name, keyspace=keyspace) \
            .mode("append") \
            .save()
        
        print(f"✅ Loaded {df.count()} records to {keyspace}.{table_name}")
        
    except Exception as e:
        print(f"❌ Cassandra write failed: {e}")
        print("   Falling back to local storage...")
        load_to_files(df, f"data/cassandra_fallback/{table_name}")


def load_to_files(df: DataFrame, output_path: str, format: str = "parquet"):
    """Load DataFrame to local files (Parquet, JSON, or CSV).
    
    Note: On Windows, parquet/JSON may fail without Hadoop winutils.
    Falls back to CSV or pandas if Spark file write fails.
    """
    print(f"\n📤 LOAD: Writing to {output_path} ({format})...")
    
    try:
        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else '.', exist_ok=True)
        
        if format == "json":
            df.coalesce(1).write.mode("overwrite").json(output_path)
        elif format == "csv":
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
        else:
            df.write.mode("overwrite").parquet(output_path)
        
        print(f"✅ Saved to {output_path}")
        return True
        
    except Exception as e:
        # Handle Windows Hadoop native library error
        if "NativeIO" in str(e) or "UnsatisfiedLinkError" in str(e):
            print(f"⚠️  Spark file write failed (Windows Hadoop issue). Using pandas fallback...")
            try:
                # Convert to pandas and save as CSV
                pdf = df.toPandas()
                csv_path = output_path + ".csv"
                pdf.to_csv(csv_path, index=False)
                print(f"✅ Saved to {csv_path} (pandas fallback)")
                return True
            except Exception as e2:
                print(f"⚠️  Pandas fallback also failed: {e2}")
                print(f"   Data was saved to Cassandra. Local file output skipped.")
                return False
        else:
            print(f"❌ File write error: {e}")
            return False


def setup_cassandra_tables(spark: SparkSession):
    """Create Cassandra keyspace and tables if they don't exist."""
    print("\n🔧 Setting up Cassandra schema...")
    
    try:
        from cassandra.cluster import Cluster
        
        cluster = Cluster([CONFIG['cassandra_host']], port=CONFIG['cassandra_port'])
        session = cluster.connect()
        
        # Create keyspace
        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {CONFIG['cassandra_keyspace']}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)
        session.set_keyspace(CONFIG['cassandra_keyspace'])
        
        # Weather observations table (for transformed data)
        session.execute("""
            CREATE TABLE IF NOT EXISTS weather_transformed (
                district TEXT,
                fetch_time TIMESTAMP,
                date TEXT,
                temp DOUBLE,
                temp_min DOUBLE,
                temp_max DOUBLE,
                temp_range DOUBLE,
                humidity INT,
                pressure INT,
                wind_speed DOUBLE,
                clouds INT,
                rain_1h DOUBLE,
                total_precip DOUBLE,
                visibility_km DOUBLE,
                heat_index DOUBLE,
                heatwave_indicator INT,
                flood_indicator INT,
                humidity_risk INT,
                year INT,
                month INT,
                day INT,
                hour INT,
                doy_sin DOUBLE,
                doy_cos DOUBLE,
                etl_timestamp TIMESTAMP,
                etl_version TEXT,
                PRIMARY KEY ((district), fetch_time)
            ) WITH CLUSTERING ORDER BY (fetch_time DESC)
        """)
        
        # Daily summary table
        session.execute("""
            CREATE TABLE IF NOT EXISTS weather_daily_summary (
                district TEXT,
                date TEXT,
                avg_temp DOUBLE,
                max_temp DOUBLE,
                min_temp DOUBLE,
                avg_humidity DOUBLE,
                total_precip DOUBLE,
                avg_wind DOUBLE,
                rainy_hours INT,
                heatwave_risk_level INT,
                flood_risk_level INT,
                observation_count INT,
                etl_timestamp TIMESTAMP,
                PRIMARY KEY ((district), date)
            ) WITH CLUSTERING ORDER BY (date DESC)
        """)
        
        # Alerts table
        session.execute("""
            CREATE TABLE IF NOT EXISTS weather_alerts (
                district TEXT,
                fetch_time TIMESTAMP,
                date TEXT,
                alert_type TEXT,
                temp_max DOUBLE,
                rain_1h DOUBLE,
                humidity INT,
                heatwave_indicator INT,
                flood_indicator INT,
                PRIMARY KEY ((district, date), fetch_time)
            ) WITH CLUSTERING ORDER BY (fetch_time DESC)
        """)
        
        cluster.shutdown()
        print("✅ Cassandra schema ready")
        return True
        
    except Exception as e:
        print(f"⚠️  Cassandra setup failed: {e}")
        print("   ETL will save to local files instead")
        return False


# =============================================================================
# ETL ORCHESTRATION
# =============================================================================

def run_full_etl(spark: SparkSession, output_path: str = "data/etl_output", 
                 use_cassandra: bool = True):
    """Run complete ETL pipeline: Extract → Transform → Load."""
    print("\n" + "="*70)
    print("  🚀 SPARK ETL PIPELINE - FULL RUN")
    print("="*70)
    
    start_time = time.time()
    
    # Setup Cassandra if enabled
    cassandra_available = False
    if use_cassandra:
        cassandra_available = setup_cassandra_tables(spark)
    
    # EXTRACT
    extractor = WeatherExtractor(CONFIG['openweathermap_api_key'])
    df_raw = extract_to_spark(spark, extractor)
    
    if df_raw.count() == 0:
        print("❌ No data extracted. Exiting.")
        return
    
    # Save raw data
    load_to_files(df_raw, f"{output_path}/raw")
    
    # TRANSFORM
    df_transformed = transform_weather_data(df_raw)
    aggregates = transform_with_aggregations(df_transformed, spark)
    
    # LOAD
    if cassandra_available:
        # Select columns that match Cassandra schema
        df_for_cassandra = df_transformed.select(
            "district", "fetch_time", "date", "temp", "temp_min", "temp_max",
            "temp_range", "humidity", "pressure", "wind_speed", "clouds",
            "rain_1h", "total_precip", "visibility_km", "heat_index",
            "heatwave_indicator", "flood_indicator", "humidity_risk",
            "year", "month", "day", "hour", "doy_sin", "doy_cos",
            "etl_timestamp", "etl_version"
        )
        load_to_cassandra(df_for_cassandra, "weather_transformed")
        
        if 'daily' in aggregates:
            load_to_cassandra(aggregates['daily'], "weather_daily_summary")
        
        if 'alerts' in aggregates and aggregates['alerts'].count() > 0:
            load_to_cassandra(aggregates['alerts'], "weather_alerts")
    
    # Always save to files as backup
    load_to_files(df_transformed, f"{output_path}/transformed")
    load_to_files(aggregates['daily'], f"{output_path}/daily_summary")
    if aggregates['alerts'].count() > 0:
        load_to_files(aggregates['alerts'], f"{output_path}/alerts")
    
    # Summary
    elapsed = time.time() - start_time
    print("\n" + "="*70)
    print("  ✅ ETL PIPELINE COMPLETE")
    print("="*70)
    print(f"   Records extracted:    {df_raw.count()}")
    print(f"   Records transformed:  {df_transformed.count()}")
    print(f"   Daily summaries:      {aggregates['daily'].count()}")
    print(f"   Alerts generated:     {aggregates['alerts'].count()}")
    print(f"   Cassandra loaded:     {'Yes' if cassandra_available else 'No (files only)'}")
    print(f"   Execution time:       {elapsed:.2f} seconds")
    print("="*70)
    
    return df_transformed


def run_streaming_etl(spark: SparkSession, interval: int = 60, 
                      output_path: str = "data/etl_streaming"):
    """Run continuous streaming ETL at specified interval."""
    print("\n" + "="*70)
    print("  🔄 SPARK STREAMING ETL PIPELINE")
    print(f"  Interval: {interval} seconds")
    print("="*70)
    
    extractor = WeatherExtractor(CONFIG['openweathermap_api_key'])
    cassandra_available = setup_cassandra_tables(spark)
    
    batch_num = 0
    
    try:
        while True:
            batch_num += 1
            batch_start = time.time()
            
            print(f"\n📦 Batch #{batch_num} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Extract
            df_raw = extract_to_spark(spark, extractor)
            
            if df_raw.count() > 0:
                # Transform
                df_transformed = transform_weather_data(df_raw)
                
                # Load to Cassandra (primary storage for Big Data pipeline)
                if cassandra_available:
                    df_for_cassandra = df_transformed.select(
                        "district", "fetch_time", "date", "temp", "temp_min", "temp_max",
                        "temp_range", "humidity", "pressure", "wind_speed", "clouds",
                        "rain_1h", "total_precip", "visibility_km", "heat_index",
                        "heatwave_indicator", "flood_indicator", "humidity_risk",
                        "year", "month", "day", "hour", "doy_sin", "doy_cos",
                        "etl_timestamp", "etl_version"
                    )
                    load_to_cassandra(df_for_cassandra, "weather_transformed")
                
                # Save batch to local files (optional backup, may fail on Windows)
                batch_path = f"{output_path}/batch_{batch_num:04d}"
                load_to_files(df_transformed, batch_path)  # Non-fatal if this fails
            
            batch_time = time.time() - batch_start
            sleep_time = max(0, interval - batch_time)
            
            print(f"   Batch time: {batch_time:.1f}s, sleeping {sleep_time:.1f}s")
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        print(f"\n\n🛑 Streaming stopped after {batch_num} batches")


# =============================================================================
# KAFKA STREAMING ETL (Recommended Big Data Architecture)
# =============================================================================

# Schema for Kafka weather messages (matches kafka_producer_api.py output)
KAFKA_WEATHER_SCHEMA = StructType([
    StructField("District", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("MaxTemp_2m", DoubleType(), True),
    StructField("MinTemp_2m", DoubleType(), True),
    StructField("TempRange_2m", DoubleType(), True),
    StructField("Precip", DoubleType(), True),
    StructField("RH_2m", DoubleType(), True),
    StructField("WindSpeed_10m", DoubleType(), True),
    StructField("Pressure", DoubleType(), True),
    StructField("Cloudiness", IntegerType(), True),
    StructField("Visibility", DoubleType(), True),
    StructField("WeatherDesc", StringType(), True),
    StructField("Sunrise", StringType(), True),
    StructField("Sunset", StringType(), True),
    StructField("fetched_at", StringType(), True),
    StructField("source", StringType(), True),
])


def run_kafka_streaming_etl(spark: SparkSession, batch_interval: int = 10):
    """
    Run Spark Structured Streaming consuming from Kafka (Lambda Architecture).
    
    This implements the Lambda Architecture:
        SPEED LAYER:  Kafka → Spark → Cassandra (real-time serving)
        BATCH LAYER:  Kafka → Spark → HDFS (raw data lake for reprocessing)
    
    Prerequisites:
        1. docker-compose up -d (Kafka, Cassandra, HDFS)
        2. Wait for HDFS: http://localhost:9870
        3. python kafka_producer_api.py (produces weather data to Kafka)
        4. python spark_etl_pipeline.py --mode stream-kafka
    """
    print("\n" + "="*70)
    print("  🔄 SPARK KAFKA STREAMING ETL (Lambda Architecture)")
    print("="*70)
    print(f"  Kafka Bootstrap: {CONFIG['kafka_bootstrap_servers']}")
    print(f"  Kafka Topic:     {CONFIG['kafka_topic']}")
    print(f"  Batch Interval:  {batch_interval} seconds")
    print("="*70)
    
    # Check HDFS availability (NameNode + DataNode must be ready)
    hdfs_available = check_hdfs_connection()
    hdfs_datanode_ready = False
    
    if hdfs_available:
        print("\n🔍 Checking HDFS cluster status...")
        hdfs_datanode_ready = check_hdfs_datanode_ready()
        
        if hdfs_datanode_ready:
            print("✅ HDFS Connected (Lambda Architecture - Batch Layer)")
            print(f"   NameNode UI: http://localhost:9870")
            print(f"   Raw Data:    {CONFIG['hdfs_namenode']}{CONFIG['hdfs_raw_path']}")
            setup_hdfs_directories(spark)
        else:
            print("⚠️  HDFS NameNode is up but DataNode not ready")
            print("   Wait for DataNode to register (check http://localhost:9870)")
            print("   Falling back to local storage...")
            hdfs_available = False
    else:
        print("\n⚠️  HDFS Not Available - Using local storage fallback")
        print("   Start HDFS with: docker-compose up -d namenode datanode")
    
    # Setup Cassandra schema (Speed Layer)
    cassandra_available = setup_cassandra_tables(spark)
    
    # Get checkpoint path (HDFS only if DataNode is ready, otherwise local)
    if hdfs_datanode_ready:
        checkpoint_dir = f"{CONFIG['hdfs_namenode']}{CONFIG['hdfs_checkpoint_path']}/kafka_stream"
        hdfs_raw_path = f"{CONFIG['hdfs_namenode']}{CONFIG['hdfs_raw_path']}/weather-data"
    else:
        # Fallback to local directory
        checkpoint_dir = os.path.join(os.getcwd(), "data", "checkpoints", "kafka_stream")
        hdfs_raw_path = os.path.join(os.getcwd(), "data", "raw")
        os.makedirs(checkpoint_dir, exist_ok=True)
        os.makedirs(hdfs_raw_path, exist_ok=True)
    
    print(f"\n📍 Storage Paths:")
    print(f"   Checkpoint: {checkpoint_dir}")
    print(f"   Raw Data:   {hdfs_raw_path}")
    
    print("\n📡 Starting Kafka consumer...")
    print("   Waiting for messages from Kafka topic 'weather-data'...")
    print("   (Run 'python kafka_producer_api.py' and send weather data)")
    print("\n   Press Ctrl+C to stop\n")
    
    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", CONFIG['kafka_bootstrap_servers']) \
        .option("subscribe", CONFIG['kafka_topic']) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON from Kafka value
    parsed_df = kafka_df \
        .selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_timestamp") \
        .select(
            from_json(col("json_str"), KAFKA_WEATHER_SCHEMA).alias("data"),
            col("kafka_timestamp")
        ) \
        .select("data.*", "kafka_timestamp")
    
    # Transform to our schema
    transformed_df = parsed_df \
        .withColumnRenamed("District", "district") \
        .withColumnRenamed("Date", "date") \
        .withColumn("fetch_time", col("kafka_timestamp")) \
        .withColumn("temp", col("MaxTemp_2m")) \
        .withColumn("temp_min", col("MinTemp_2m")) \
        .withColumn("temp_max", col("MaxTemp_2m")) \
        .withColumn("temp_range", col("TempRange_2m")) \
        .withColumn("humidity", col("RH_2m").cast(IntegerType())) \
        .withColumn("pressure", col("Pressure").cast(IntegerType())) \
        .withColumn("wind_speed", col("WindSpeed_10m")) \
        .withColumn("clouds", col("Cloudiness")) \
        .withColumn("rain_1h", col("Precip")) \
        .withColumn("total_precip", col("Precip")) \
        .withColumn("visibility_km", col("Visibility") / 1000) \
        .withColumn("year", year(col("fetch_time"))) \
        .withColumn("month", month(col("fetch_time"))) \
        .withColumn("day", dayofmonth(col("fetch_time"))) \
        .withColumn("hour", hour(col("fetch_time"))) \
        .withColumn("doy_sin", sin(2 * 3.14159 * dayofyear(col("fetch_time")) / 365)) \
        .withColumn("doy_cos", cos(2 * 3.14159 * dayofyear(col("fetch_time")) / 365)) \
        .withColumn("heat_index", 
            when(col("temp") > 27, 
                 col("temp") + 0.5 * (col("humidity") - 10))
            .otherwise(col("temp"))) \
        .withColumn("heatwave_indicator",
            when(col("temp") > 40, 0.9)
            .when(col("temp") > 35, 0.5)
            .when(col("temp") > 30, 0.2)
            .otherwise(0.0)) \
        .withColumn("flood_indicator",
            when(col("rain_1h") > 50, 0.8)
            .when(col("rain_1h") > 20, 0.5)
            .when(col("humidity") > 90, 0.3)
            .otherwise(0.1)) \
        .withColumn("humidity_risk",
            when(col("humidity") > 90, "HIGH")
            .when(col("humidity") > 70, "MEDIUM")
            .otherwise("LOW")) \
        .withColumn("etl_timestamp", current_timestamp()) \
        .withColumn("etl_version", lit("kafka-spark-hdfs-1.0"))
    
    # Select columns for output
    output_df = transformed_df.select(
        "district", "fetch_time", "date", "temp", "temp_min", "temp_max",
        "temp_range", "humidity", "pressure", "wind_speed", "clouds",
        "rain_1h", "total_precip", "visibility_km", "heat_index",
        "heatwave_indicator", "flood_indicator", "humidity_risk",
        "year", "month", "day", "hour", "doy_sin", "doy_cos",
        "etl_timestamp", "etl_version"
    )
    
    # Lambda Architecture: Write to both HDFS (Batch) and Cassandra (Speed)
    def write_batch_lambda(batch_df, batch_id):
        """Lambda Architecture batch writer - writes to both HDFS and Cassandra."""
        if batch_df.count() > 0:
            record_count = batch_df.count()
            print(f"\n📦 Kafka Batch #{batch_id}: Processing {record_count} records...")
            
            # Show sample
            batch_df.select("district", "temp", "humidity", "heatwave_indicator").show(5, False)
            
            # ==================================================================
            # BATCH LAYER: Write raw data to HDFS (Data Lake) or Local
            # ==================================================================
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                if hdfs_datanode_ready:
                    # Write to HDFS as Parquet (columnar format for analytics)
                    hdfs_output = f"{hdfs_raw_path}/batch_{batch_id}_{timestamp}"
                    batch_df.write \
                        .mode("append") \
                        .parquet(hdfs_output)
                    print(f"   📂 HDFS (Batch Layer): {record_count} records → {hdfs_output}")
                else:
                    # Fallback to local parquet
                    local_output = os.path.join(hdfs_raw_path, f"batch_{batch_id}_{timestamp}")
                    batch_df.toPandas().to_parquet(f"{local_output}.parquet")
                    print(f"   📂 Local (Fallback): {record_count} records → {local_output}.parquet")
            except Exception as e:
                print(f"   ⚠️ Data Lake write warning: {e}")
            
            # ==================================================================
            # SPEED LAYER: Write to Cassandra (Real-time Serving)
            # ==================================================================
            if cassandra_available:
                try:
                    batch_df.write \
                        .format("org.apache.spark.sql.cassandra") \
                        .options(table="weather_transformed", keyspace=CONFIG['cassandra_keyspace']) \
                        .mode("append") \
                        .save()
                    print(f"   ⚡ Cassandra (Speed Layer): {record_count} records written")
                except Exception as e:
                    print(f"   ❌ Cassandra write error: {e}")
            
            print(f"   ✅ Batch #{batch_id} complete (Lambda Architecture)")
    
    # Start streaming query with HDFS checkpoint
    query = output_df \
        .writeStream \
        .foreachBatch(write_batch_lambda) \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_dir) \
        .trigger(processingTime=f"{batch_interval} seconds") \
        .start()
    
    print("✅ Kafka streaming query started (Lambda Architecture)")
    print(f"   Checkpoint: {checkpoint_dir}")
    print("   Consuming from topic 'weather-data'...")
    print("\n   Data Flow:")
    print("   ┌─────────────┐    ┌─────────────┐    ┌───────────────┐")
    print("   │    Kafka    │ → │    Spark    │ → │     HDFS      │ (Batch Layer)")
    print("   │  (Ingest)   │    │  (Process)  │    │  (Data Lake)  │")
    print("   └─────────────┘    └─────────────┘    └───────────────┘")
    print("                           │")
    print("                           ↓")
    print("                    ┌───────────────┐")
    print("                    │   Cassandra   │ (Speed Layer)")
    print("                    │  (Real-time)  │")
    print("                    └───────────────┘")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n\n🛑 Kafka streaming stopped")
        query.stop()


# =============================================================================
# BATCH HDFS PROCESSING (Lambda Architecture - Batch Layer)
# =============================================================================

def run_batch_hdfs_etl(spark: SparkSession, output_to_cassandra: bool = True):
    """
    Run batch processing on HDFS data (Lambda Architecture - Batch Layer).
    
    This reads historical data from HDFS and recomputes batch views:
        HDFS (Raw Data) → Spark Batch → Cassandra (Batch Views)
    
    Use this for:
        - Reprocessing historical data
        - Computing aggregates over large time windows
        - Rebuilding views after schema changes
    
    Prerequisites:
        1. docker-compose up -d (HDFS, Cassandra)
        2. Data must exist in HDFS from stream-kafka mode
    """
    print("\n" + "="*70)
    print("  📊 SPARK BATCH ETL (Lambda Architecture - Batch Layer)")
    print("="*70)
    
    # Check HDFS
    hdfs_available = check_hdfs_connection()
    if not hdfs_available:
        print("❌ HDFS not available. Start with: docker-compose up -d namenode datanode")
        return
    
    hdfs_raw_path = f"{CONFIG['hdfs_namenode']}{CONFIG['hdfs_raw_path']}/weather-data"
    print(f"  HDFS Source: {hdfs_raw_path}")
    
    # Setup HDFS directories
    setup_hdfs_directories(spark)
    
    # Read all parquet files from HDFS
    print(f"\n📂 Reading raw data from HDFS...")
    try:
        df_raw = spark.read.parquet(f"{hdfs_raw_path}/*")
        record_count = df_raw.count()
        print(f"   ✅ Loaded {record_count} records from HDFS")
        
        if record_count == 0:
            print("   ⚠️ No data in HDFS. Run stream-kafka mode first to populate data.")
            return
        
    except Exception as e:
        print(f"   ❌ Error reading from HDFS: {e}")
        print("   ⚠️ No data in HDFS. Run stream-kafka mode first to populate data.")
        return
    
    # Show data summary
    print(f"\n📊 Data Summary:")
    df_raw.select("district", "date", "temp", "humidity").show(5)
    
    print(f"\n📈 Computing batch aggregations...")
    
    # Daily aggregations
    from pyspark.sql.functions import avg, max as spark_max, min as spark_min, count, sum as spark_sum
    
    df_daily = df_raw.groupBy("district", "date").agg(
        spark_round(avg("temp"), 2).alias("avg_temp"),
        spark_round(spark_max("temp"), 2).alias("max_temp"),
        spark_round(spark_min("temp"), 2).alias("min_temp"),
        spark_round(avg("humidity"), 2).alias("avg_humidity"),
        spark_round(spark_sum("rain_1h"), 2).alias("total_precip"),
        spark_round(avg("wind_speed"), 2).alias("avg_wind"),
        spark_sum(when(col("rain_1h") > 0, 1).otherwise(0)).alias("rainy_hours"),
        spark_max("heatwave_indicator").alias("max_heatwave_risk"),
        spark_max("flood_indicator").alias("max_flood_risk"),
        count("*").alias("observation_count")
    ).withColumn("etl_timestamp", current_timestamp())
    
    print(f"\n📊 Daily Aggregations:")
    df_daily.show(10)
    
    # Write aggregations to HDFS
    processed_path = f"{CONFIG['hdfs_namenode']}{CONFIG['hdfs_processed_path']}/daily"
    try:
        df_daily.write.mode("overwrite").parquet(processed_path)
        print(f"   ✅ Daily aggregations saved to HDFS: {processed_path}")
    except Exception as e:
        print(f"   ⚠️ HDFS write warning: {e}")
    
    # Write to Cassandra (Serving Layer)
    if output_to_cassandra:
        cassandra_available = setup_cassandra_tables(spark)
        if cassandra_available:
            try:
                # Prepare for Cassandra schema
                df_for_cassandra = df_daily.select(
                    col("district"),
                    col("date"),
                    col("avg_temp"),
                    col("max_temp"),
                    col("min_temp"),
                    col("avg_humidity"),
                    col("total_precip"),
                    col("avg_wind"),
                    col("rainy_hours").cast(IntegerType()),
                    (col("max_heatwave_risk") * 10).cast(IntegerType()).alias("heatwave_risk_level"),
                    (col("max_flood_risk") * 10).cast(IntegerType()).alias("flood_risk_level"),
                    col("observation_count").cast(IntegerType()),
                    col("etl_timestamp")
                )
                
                df_for_cassandra.write \
                    .format("org.apache.spark.sql.cassandra") \
                    .options(table="weather_daily_summary", keyspace=CONFIG['cassandra_keyspace']) \
                    .mode("append") \
                    .save()
                print(f"   ✅ Batch views written to Cassandra")
            except Exception as e:
                print(f"   ⚠️ Cassandra batch write error: {e}")
    
    print("\n" + "="*70)
    print("  ✅ BATCH PROCESSING COMPLETE")
    print("="*70)
    print(f"   Records Processed: {record_count}")
    print(f"   Daily Aggregations: {df_daily.count()}")
    print(f"   HDFS Output: {processed_path}")


def list_hdfs_data(spark: SparkSession):
    """List data available in HDFS for the weather pipeline."""
    print("\n" + "="*70)
    print("  📂 HDFS DATA LAKE STATUS")
    print("="*70)
    
    if not check_hdfs_connection():
        print("❌ HDFS not available. Start with: docker-compose up -d namenode datanode")
        return
    
    print(f"  NameNode: {CONFIG['hdfs_namenode']}")
    print(f"  Web UI: http://localhost:9870")
    
    try:
        # Get Hadoop FileSystem
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.defaultFS", CONFIG['hdfs_namenode'])
        
        uri = spark._jvm.java.net.URI(CONFIG['hdfs_namenode'])
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
        
        paths_to_check = [
            (CONFIG['hdfs_raw_path'], "Raw Data (Data Lake)"),
            (CONFIG['hdfs_processed_path'], "Processed Data"),
            (CONFIG['hdfs_checkpoint_path'], "Streaming Checkpoints"),
        ]
        
        print(f"\n📂 HDFS Directory Structure:")
        for hdfs_path, description in paths_to_check:
            path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_path)
            if fs.exists(path):
                status = fs.listStatus(path)
                file_count = len(status)
                print(f"   ✓ {hdfs_path} - {description} ({file_count} items)")
                
                # List files/dirs
                for s in status[:5]:  # Show first 5
                    name = s.getPath().getName()
                    size = s.getLen() / (1024*1024)  # MB
                    print(f"      - {name} ({size:.2f} MB)")
                if len(status) > 5:
                    print(f"      ... and {len(status) - 5} more")
            else:
                print(f"   ✗ {hdfs_path} - {description} (not created)")
        
    except Exception as e:
        print(f"   ⚠️ Error listing HDFS: {e}")


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Spark ETL Pipeline for Weather Data (Lambda Architecture with HDFS)"
    )
    parser.add_argument(
        '--mode', '-m',
        choices=['all', 'extract', 'transform', 'load', 'stream', 'stream-kafka', 'batch-hdfs', 'list-hdfs'],
        default='stream-kafka',
        help='ETL mode: stream-kafka (Lambda), batch-hdfs (batch layer), list-hdfs (show HDFS data), stream, all, extract, transform, load'
    )
    parser.add_argument(
        '--input', '-i',
        help='Input path for transform/load modes'
    )
    parser.add_argument(
        '--output', '-o',
        default='data/etl_output',
        help='Output path for results'
    )
    parser.add_argument(
        '--interval', '-t',
        type=int,
        default=10,
        help='Interval in seconds for streaming mode (default: 10)'
    )
    parser.add_argument(
        '--no-cassandra',
        action='store_true',
        help='Skip Cassandra, save only to files'
    )
    
    args = parser.parse_args()
    
    # Banner
    print("\n" + "="*70)
    print("  ⚡ SPARK ETL PIPELINE FOR WEATHER DATA (Lambda Architecture)")
    print("="*70)
    print(f"  Mode: {args.mode}")
    if args.mode == 'stream-kafka':
        print(f"  Architecture: Kafka → Spark → HDFS + Cassandra (Lambda)")
    elif args.mode == 'batch-hdfs':
        print(f"  Architecture: HDFS → Spark Batch → Cassandra (Batch Layer)")
    elif args.mode == 'list-hdfs':
        print(f"  Architecture: HDFS Data Lake Status")
    else:
        print(f"  Architecture: API → Spark → Cassandra (Direct)")
    print(f"  Output: {args.output}")
    print(f"  Cassandra: {'Disabled' if args.no_cassandra else 'Enabled'}")
    print("="*70)
    
    # For modes that don't need API key
    api_key_not_required = ['stream-kafka', 'batch-hdfs', 'list-hdfs', 'load', 'transform']
    if args.mode not in api_key_not_required and not CONFIG['openweathermap_api_key']:
        print("\n❌ OPENWEATHERMAP_API_KEY environment variable not set!")
        print("   Set it with: $env:OPENWEATHERMAP_API_KEY='your_key'")
        sys.exit(1)
    
    # Create Spark session with appropriate connectors
    use_cassandra = not args.no_cassandra
    use_kafka = args.mode == 'stream-kafka'
    use_hdfs = args.mode in ['stream-kafka', 'batch-hdfs', 'list-hdfs']
    spark = create_spark_session(with_cassandra=use_cassandra, with_kafka=use_kafka, with_hdfs=use_hdfs)
    
    try:
        if args.mode == 'stream-kafka':
            run_kafka_streaming_etl(spark, args.interval)
        
        elif args.mode == 'batch-hdfs':
            run_batch_hdfs_etl(spark, output_to_cassandra=use_cassandra)
        
        elif args.mode == 'list-hdfs':
            list_hdfs_data(spark)
        
        elif args.mode == 'all':
            run_full_etl(spark, args.output, use_cassandra)
        
        elif args.mode == 'extract':
            extractor = WeatherExtractor(CONFIG['openweathermap_api_key'])
            df = extract_to_spark(spark, extractor)
            load_to_files(df, args.output)
        
        elif args.mode == 'transform':
            if not args.input:
                print("❌ --input required for transform mode")
                sys.exit(1)
            df = extract_from_files(spark, args.input)
            df_transformed = transform_weather_data(df)
            load_to_files(df_transformed, args.output)
        
        elif args.mode == 'load':
            if not args.input:
                print("❌ --input required for load mode")
                sys.exit(1)
            df = spark.read.parquet(args.input)
            if use_cassandra:
                setup_cassandra_tables(spark)
                load_to_cassandra(df, "weather_transformed")
            else:
                print("⚠️  Cassandra disabled, nothing to load")
        
        elif args.mode == 'stream':
            run_streaming_etl(spark, args.interval, args.output)
    
    finally:
        spark.stop()
        print("\n✅ Spark session closed")


if __name__ == "__main__":
    main()
