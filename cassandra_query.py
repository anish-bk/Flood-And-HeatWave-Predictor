"""
Cassandra Data Query Tool
=========================
Query weather observations and predictions stored in Cassandra.

Usage:
    python cassandra_query.py --help
    python cassandra_query.py observations --district Bara --limit 10
    python cassandra_query.py predictions --district Siraha --limit 5
    python cassandra_query.py summary --district Dhanusa
    python cassandra_query.py stats
    python cassandra_query.py export --district Bara --output weather_data.csv
"""

import argparse
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import json

try:
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.query import SimpleStatement
    CASSANDRA_AVAILABLE = True
except ImportError:
    CASSANDRA_AVAILABLE = False
    print("⚠️  cassandra-driver not installed. Run: pip install cassandra-driver")

# Cassandra Configuration
CASSANDRA_CONFIG = {
    'hosts': ['localhost'],
    'port': 9042,
    'keyspace': 'weather_monitoring'
}

# Districts
DISTRICTS = ['Bara', 'Dhanusa', 'Sarlahi', 'Parsa', 'Siraha']


class CassandraQueryClient:
    """Client for querying Cassandra weather data."""
    
    def __init__(self):
        self.cluster = None
        self.session = None
        self.connected = False
    
    def connect(self) -> bool:
        """Connect to Cassandra cluster."""
        if not CASSANDRA_AVAILABLE:
            print("❌ Cassandra driver not available")
            return False
        
        import time
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                self.cluster = Cluster(
                    CASSANDRA_CONFIG['hosts'],
                    port=CASSANDRA_CONFIG['port']
                )
                self.session = self.cluster.connect()
                
                # Small delay to ensure connection is stable
                time.sleep(0.2)
                
                # Check if keyspace exists
                rows = self.session.execute(
                    "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = %s",
                    [CASSANDRA_CONFIG['keyspace']]
                )
                if not rows.one():
                    if attempt < max_retries - 1:
                        time.sleep(0.5)
                        continue
                    print(f"⚠️  Keyspace '{CASSANDRA_CONFIG['keyspace']}' does not exist.")
                    print("   Start the weather dashboard first to create the schema.")
                    return False
                
                self.session.set_keyspace(CASSANDRA_CONFIG['keyspace'])
                self.connected = True
                print(f"✅ Connected to Cassandra ({CASSANDRA_CONFIG['hosts'][0]}:{CASSANDRA_CONFIG['port']})")
                return True
                
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(0.5)
                    continue
                print(f"❌ Failed to connect to Cassandra: {e}")
                return False
        
        return False
    
    def close(self):
        """Close connection."""
        if self.cluster:
            self.cluster.shutdown()
            self.connected = False
    
    def get_observations(self, district: str, limit: int = 20, 
                         hours_back: Optional[int] = None) -> List[Dict]:
        """Get weather observations for a district."""
        if not self.connected:
            return []
        
        try:
            if hours_back:
                start_time = datetime.utcnow() - timedelta(hours=hours_back)
                query = """
                    SELECT district, fetch_time, date, max_temp, min_temp, temp_range,
                           precipitation, humidity, wind_speed, pressure, cloudiness,
                           visibility, weather_desc, source
                    FROM weather_observations
                    WHERE district = %s AND fetch_time >= %s
                    ORDER BY fetch_time DESC
                    LIMIT %s
                """
                rows = self.session.execute(query, [district, start_time, limit])
            else:
                query = """
                    SELECT district, fetch_time, date, max_temp, min_temp, temp_range,
                           precipitation, humidity, wind_speed, pressure, cloudiness,
                           visibility, weather_desc, source
                    FROM weather_observations
                    WHERE district = %s
                    ORDER BY fetch_time DESC
                    LIMIT %s
                """
                rows = self.session.execute(query, [district, limit])
            
            results = []
            for row in rows:
                results.append({
                    'district': row.district,
                    'fetch_time': row.fetch_time.strftime('%Y-%m-%d %H:%M:%S') if row.fetch_time else None,
                    'date': row.date,
                    'max_temp': row.max_temp,
                    'min_temp': row.min_temp,
                    'temp_range': row.temp_range,
                    'precipitation': row.precipitation,
                    'humidity': row.humidity,
                    'wind_speed': row.wind_speed,
                    'pressure': row.pressure,
                    'cloudiness': row.cloudiness,
                    'visibility': row.visibility,
                    'weather_desc': row.weather_desc,
                    'source': row.source
                })
            return results
            
        except Exception as e:
            print(f"❌ Error querying observations: {e}")
            return []
    
    def get_predictions(self, district: str, limit: int = 20) -> List[Dict]:
        """Get predictions for a district."""
        if not self.connected:
            return []
        
        try:
            query = """
                SELECT district, prediction_time, max_temp, precipitation, humidity,
                       heatwave_probability, flood_probability, heatwave_risk, flood_risk
                FROM weather_predictions
                WHERE district = %s
                ORDER BY prediction_time DESC
                LIMIT %s
            """
            rows = self.session.execute(query, [district, limit])
            
            results = []
            for row in rows:
                results.append({
                    'district': row.district,
                    'prediction_time': row.prediction_time.strftime('%Y-%m-%d %H:%M:%S') if row.prediction_time else None,
                    'max_temp': row.max_temp,
                    'precipitation': row.precipitation,
                    'humidity': row.humidity,
                    'heatwave_prob': round(row.heatwave_probability, 4) if row.heatwave_probability else None,
                    'flood_prob': round(row.flood_probability, 4) if row.flood_probability else None,
                    'heatwave_risk': row.heatwave_risk,
                    'flood_risk': row.flood_risk
                })
            return results
            
        except Exception as e:
            print(f"❌ Error querying predictions: {e}")
            return []
    
    def get_daily_summary(self, district: str, days_back: int = 7) -> List[Dict]:
        """Get daily weather summary for a district."""
        if not self.connected:
            return []
        
        try:
            # Calculate date range
            end_date = datetime.utcnow().strftime('%Y-%m-%d')
            start_date = (datetime.utcnow() - timedelta(days=days_back)).strftime('%Y-%m-%d')
            
            query = """
                SELECT district, date, record_count, avg_temp, max_temp, min_temp,
                       total_precip, avg_humidity, heatwave_alerts, flood_alerts
                FROM daily_weather_summary
                WHERE district = %s AND date >= %s
                ORDER BY date DESC
            """
            rows = self.session.execute(query, [district, start_date])
            
            results = []
            for row in rows:
                results.append({
                    'district': row.district,
                    'date': row.date,
                    'record_count': row.record_count,
                    'avg_temp': round(row.avg_temp, 1) if row.avg_temp else None,
                    'max_temp': round(row.max_temp, 1) if row.max_temp else None,
                    'min_temp': round(row.min_temp, 1) if row.min_temp else None,
                    'total_precip': round(row.total_precip, 2) if row.total_precip else None,
                    'avg_humidity': round(row.avg_humidity, 1) if row.avg_humidity else None,
                    'heatwave_alerts': row.heatwave_alerts,
                    'flood_alerts': row.flood_alerts
                })
            return results
            
        except Exception as e:
            print(f"❌ Error querying daily summary: {e}")
            return []
    
    def get_all_observations_count(self) -> Dict[str, int]:
        """Get observation count per district."""
        if not self.connected:
            return {}
        
        counts = {}
        for district in DISTRICTS:
            try:
                query = "SELECT COUNT(*) as cnt FROM weather_observations WHERE district = %s"
                row = self.session.execute(query, [district]).one()
                counts[district] = row.cnt if row else 0
            except:
                counts[district] = 0
        return counts
    
    def get_all_predictions_count(self) -> Dict[str, int]:
        """Get prediction count per district."""
        if not self.connected:
            return {}
        
        counts = {}
        for district in DISTRICTS:
            try:
                query = "SELECT COUNT(*) as cnt FROM weather_predictions WHERE district = %s"
                row = self.session.execute(query, [district]).one()
                counts[district] = row.cnt if row else 0
            except:
                counts[district] = 0
        return counts
    
    def get_latest_observation(self, district: str) -> Optional[Dict]:
        """Get the most recent observation for a district."""
        observations = self.get_observations(district, limit=1)
        return observations[0] if observations else None
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get overall statistics."""
        obs_counts = self.get_all_observations_count()
        pred_counts = self.get_all_predictions_count()
        
        stats = {
            'total_observations': sum(obs_counts.values()),
            'total_predictions': sum(pred_counts.values()),
            'observations_by_district': obs_counts,
            'predictions_by_district': pred_counts,
            'districts_monitored': len([d for d in DISTRICTS if obs_counts.get(d, 0) > 0])
        }
        return stats
    
    def export_to_csv(self, district: str, output_file: str, limit: int = 1000) -> bool:
        """Export observations to CSV file."""
        try:
            observations = self.get_observations(district, limit=limit)
            if not observations:
                print(f"⚠️  No observations found for {district}")
                return False
            
            import csv
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                if observations:
                    writer = csv.DictWriter(f, fieldnames=observations[0].keys())
                    writer.writeheader()
                    writer.writerows(observations)
            
            print(f"✅ Exported {len(observations)} records to {output_file}")
            return True
            
        except Exception as e:
            print(f"❌ Export failed: {e}")
            return False


def print_table(data: List[Dict], title: str = ""):
    """Print data as a formatted table."""
    if not data:
        print("No data found.")
        return
    
    if title:
        print(f"\n{'='*60}")
        print(f"  {title}")
        print(f"{'='*60}")
    
    # Get column widths
    headers = list(data[0].keys())
    widths = {h: max(len(str(h)), max(len(str(row.get(h, ''))) for row in data)) for h in headers}
    
    # Limit column widths
    max_width = 20
    widths = {h: min(w, max_width) for h, w in widths.items()}
    
    # Print header
    header_line = " | ".join(str(h)[:widths[h]].ljust(widths[h]) for h in headers)
    print(f"\n{header_line}")
    print("-" * len(header_line))
    
    # Print rows
    for row in data:
        row_line = " | ".join(str(row.get(h, ''))[:widths[h]].ljust(widths[h]) for h in headers)
        print(row_line)
    
    print(f"\nTotal: {len(data)} records\n")


def print_stats(stats: Dict):
    """Print statistics in a nice format."""
    print("\n" + "="*60)
    print("  📊 CASSANDRA WEATHER DATA STATISTICS")
    print("="*60)
    
    print(f"\n📈 Overall Statistics:")
    print(f"   Total Observations: {stats['total_observations']}")
    print(f"   Total Predictions:  {stats['total_predictions']}")
    print(f"   Districts Active:   {stats['districts_monitored']}/{len(DISTRICTS)}")
    
    print(f"\n🏘️  Observations by District:")
    for district, count in stats['observations_by_district'].items():
        bar = "█" * min(count, 20)
        print(f"   {district:10s}: {count:5d} {bar}")
    
    print(f"\n🔮 Predictions by District:")
    for district, count in stats['predictions_by_district'].items():
        bar = "█" * min(count, 20)
        print(f"   {district:10s}: {count:5d} {bar}")
    
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Query weather data stored in Cassandra",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cassandra_query.py stats
  python cassandra_query.py observations --district Bara --limit 10
  python cassandra_query.py predictions --district Siraha
  python cassandra_query.py summary --district Dhanusa --days 7
  python cassandra_query.py export --district Bara --output bara_weather.csv
  python cassandra_query.py latest --district Parsa
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Show overall statistics')
    
    # Observations command
    obs_parser = subparsers.add_parser('observations', help='Query weather observations')
    obs_parser.add_argument('--district', '-d', required=True, choices=DISTRICTS,
                           help='District name')
    obs_parser.add_argument('--limit', '-l', type=int, default=20,
                           help='Maximum number of records (default: 20)')
    obs_parser.add_argument('--hours', '-H', type=int,
                           help='Only show records from last N hours')
    obs_parser.add_argument('--json', action='store_true',
                           help='Output as JSON')
    
    # Predictions command
    pred_parser = subparsers.add_parser('predictions', help='Query predictions')
    pred_parser.add_argument('--district', '-d', required=True, choices=DISTRICTS,
                            help='District name')
    pred_parser.add_argument('--limit', '-l', type=int, default=20,
                            help='Maximum number of records (default: 20)')
    pred_parser.add_argument('--json', action='store_true',
                            help='Output as JSON')
    
    # Summary command
    summary_parser = subparsers.add_parser('summary', help='Query daily summary')
    summary_parser.add_argument('--district', '-d', required=True, choices=DISTRICTS,
                               help='District name')
    summary_parser.add_argument('--days', type=int, default=7,
                               help='Number of days back (default: 7)')
    summary_parser.add_argument('--json', action='store_true',
                               help='Output as JSON')
    
    # Latest command
    latest_parser = subparsers.add_parser('latest', help='Get latest observation')
    latest_parser.add_argument('--district', '-d', required=True, choices=DISTRICTS,
                              help='District name')
    latest_parser.add_argument('--json', action='store_true',
                              help='Output as JSON')
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export data to CSV')
    export_parser.add_argument('--district', '-d', required=True, choices=DISTRICTS,
                              help='District name')
    export_parser.add_argument('--output', '-o', required=True,
                              help='Output CSV file path')
    export_parser.add_argument('--limit', '-l', type=int, default=1000,
                              help='Maximum records to export (default: 1000)')
    
    # All districts command
    all_parser = subparsers.add_parser('all', help='Show latest for all districts')
    all_parser.add_argument('--json', action='store_true',
                           help='Output as JSON')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Connect to Cassandra
    client = CassandraQueryClient()
    if not client.connect():
        print("\n💡 Make sure Cassandra is running:")
        print("   docker-compose up -d")
        return
    
    try:
        if args.command == 'stats':
            stats = client.get_statistics()
            print_stats(stats)
        
        elif args.command == 'observations':
            data = client.get_observations(
                args.district, 
                limit=args.limit,
                hours_back=args.hours
            )
            if args.json:
                print(json.dumps(data, indent=2))
            else:
                print_table(data, f"Weather Observations - {args.district}")
        
        elif args.command == 'predictions':
            data = client.get_predictions(args.district, limit=args.limit)
            if args.json:
                print(json.dumps(data, indent=2))
            else:
                print_table(data, f"Predictions - {args.district}")
        
        elif args.command == 'summary':
            data = client.get_daily_summary(args.district, days_back=args.days)
            if args.json:
                print(json.dumps(data, indent=2))
            else:
                print_table(data, f"Daily Summary - {args.district} (Last {args.days} days)")
        
        elif args.command == 'latest':
            data = client.get_latest_observation(args.district)
            if args.json:
                print(json.dumps(data, indent=2))
            else:
                if data:
                    print(f"\n🌡️  Latest Observation - {args.district}")
                    print("="*40)
                    for key, value in data.items():
                        print(f"  {key:20s}: {value}")
                else:
                    print(f"No observations found for {args.district}")
        
        elif args.command == 'export':
            client.export_to_csv(args.district, args.output, limit=args.limit)
        
        elif args.command == 'all':
            all_latest = []
            for district in DISTRICTS:
                latest = client.get_latest_observation(district)
                if latest:
                    all_latest.append(latest)
            
            if args.json:
                print(json.dumps(all_latest, indent=2))
            else:
                print_table(all_latest, "Latest Observations - All Districts")
    
    finally:
        client.close()


if __name__ == "__main__":
    print("\n" + "="*60)
    print("  🗄️  CASSANDRA WEATHER DATA QUERY TOOL")
    print("="*60)
    main()
