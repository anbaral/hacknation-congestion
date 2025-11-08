"""
Enhanced Traffic Warning Data Scraper for German Autobahns
Specialized script for comprehensive traffic warning and incident data collection
from German highway networks with detailed geographic information

Author: Traffic Analytics
Date: 2025
"""

import requests
import pandas as pd
import json
from datetime import datetime, timedelta
import time
from typing import Dict, List, Optional, Tuple, Any
import logging
from pathlib import Path
from collections import defaultdict
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
import hashlib

# Configure logging with more detail
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('traffic_warnings.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class EnhancedTrafficWarningScraper:
    """
    Enhanced scraper for comprehensive traffic warning data from German highways
    Collects detailed incident, warning, and hazard information with rich metadata
    """
    
    def __init__(self, output_dir: str = "traffic_warnings_data"):
        """
        Initialize the enhanced traffic warning scraper
        
        Args:
            output_dir: Directory to save scraped warning data
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Create subdirectories for organized data storage
        self.raw_dir = self.output_dir / "raw_data"
        self.processed_dir = self.output_dir / "processed_data"
        self.archive_dir = self.output_dir / "archive"
        
        for dir in [self.raw_dir, self.processed_dir, self.archive_dir]:
            dir.mkdir(exist_ok=True)
        
        # API endpoints
        self.autobahn_api_base = "https://verkehr.autobahn.de/o/autobahn"
        
        # Additional traffic data sources
        self.alternative_sources = {
            'bast': 'https://www.bast.de/DE/Verkehrstechnik/Fachthemen/v2-verkehrszaehlung',
            'adac': 'https://www.adac.de/verkehr',  # ADAC traffic info
            'bayern': 'https://www.bayerninfo.de',  # Bavaria traffic
        }
        
        # Headers for requests
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/html, application/xml',
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }
        
        # Data storage
        self.all_warnings = []
        self.warning_history = defaultdict(list)
        self.statistics = defaultdict(int)
        
        # Warning severity mapping
        self.severity_levels = {
            'danger': 5,      # Immediate danger
            'accident': 4,    # Accident occurred
            'congestion': 3,  # Traffic jam
            'roadworks': 2,   # Construction
            'info': 1        # General information
        }
        
    def get_all_autobahns(self, include_bundesstrassen: bool = False) -> List[str]:
        """
        Get comprehensive list of all German highways
        
        Args:
            include_bundesstrassen: Include federal roads (B-roads) in addition to autobahns
            
        Returns:
            List of highway identifiers
        """
        highways = []
        
        try:
            # Get autobahns from API
            response = requests.get(f"{self.autobahn_api_base}/", headers=self.headers, timeout=30)
            if response.status_code == 200:
                data = response.json()
                highways.extend(data.get('roads', []))
                logger.info(f"Found {len(highways)} autobahns from API")
        except Exception as e:
            logger.error(f"Error fetching autobahn list: {e}")
        
        # Add comprehensive autobahn list as fallback
        if not highways:
            # All German autobahns (A1-A999)
            highways = [f'A{i}' for i in range(1, 100) if i not in [12, 16, 18, 22, 32, 34, 35, 36, 37, 41, 47, 50, 51, 53, 54, 55, 56, 58, 68, 69, 75, 76, 77, 78, 79, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 97]]
        
        if include_bundesstrassen:
            # Add major federal roads
            bundesstrassen = [f'B{i}' for i in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 27, 31, 96, 173, 300]]
            highways.extend(bundesstrassen)
        
        return highways
    
    def get_traffic_warnings_detailed(self, highway: str, include_history: bool = True) -> List[Dict]:
        """
        Get detailed traffic warnings for a specific highway with enhanced metadata
        
        Args:
            highway: Highway identifier (e.g., 'A1')
            include_history: Include historical context for warnings
            
        Returns:
            List of detailed warning dictionaries
        """
        warnings = []
        
        try:
            # Get initial warning list
            url = f"{self.autobahn_api_base}/{highway}/services/warning"
            response = requests.get(url, headers=self.headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                warning_items = data.get('warning', [])
                
                logger.info(f"Found {len(warning_items)} warnings on {highway}")
                
                # Get detailed information for each warning
                for idx, warning in enumerate(warning_items):
                    try:
                        detail_url = f"{self.autobahn_api_base}/details/warning/{warning['identifier']}"
                        detail_response = requests.get(detail_url, headers=self.headers, timeout=30)
                        
                        if detail_response.status_code == 200:
                            details = detail_response.json()
                            
                            # Extract and enrich warning data
                            warning_data = self._process_warning_details(details, highway)
                            
                            # Add historical context if requested
                            if include_history:
                                warning_data['history'] = self._get_warning_history(warning['identifier'])
                            
                            # Calculate additional metrics
                            warning_data = self._enrich_warning_data(warning_data)
                            
                            warnings.append(warning_data)
                            
                            # Update statistics
                            self.statistics['total_warnings'] += 1
                            self.statistics[f'warnings_{highway}'] += 1
                            
                        # Rate limiting
                        time.sleep(0.1)
                        
                    except Exception as e:
                        logger.warning(f"Error processing warning {warning.get('identifier')}: {e}")
                        continue
                        
            else:
                logger.warning(f"Failed to get warnings for {highway}: Status {response.status_code}")
                
        except Exception as e:
            logger.error(f"Error fetching warnings for {highway}: {e}")
        
        return warnings
    
    def _process_warning_details(self, details: Dict, highway: str) -> Dict:
        """
        Process and structure warning details with all available metadata
        
        Args:
            details: Raw warning details from API
            highway: Highway identifier
            
        Returns:
            Structured warning dictionary
        """
        coord = details.get('coordinate', {})
        
        # Create comprehensive warning record
        warning_data = {
            # Basic identification
            'highway': highway,
            'warning_id': details.get('identifier'),
            'unique_hash': self._generate_hash(details),
            
            # Location information
            'latitude': coord.get('lat'),
            'longitude': coord.get('long'),
            'location_description': details.get('location', ''),
            
            # Warning content
            'title': details.get('title', ''),
            'subtitle': details.get('subtitle', ''),
            'description': ' '.join(details.get('description', [])) if isinstance(details.get('description'), list) else details.get('description', ''),
            
            # Temporal information
            'start_timestamp': details.get('startTimestamp'),
            'end_timestamp': details.get('endTimestamp'),
            'last_updated': datetime.now().isoformat(),
            'duration_minutes': self._calculate_duration(details.get('startTimestamp'), details.get('endTimestamp')),
            
            # Traffic impact
            'extent': details.get('extent', ''),  # Length of affected road section
            'direction': details.get('direction', ''),  # Direction of travel affected
            'impact': details.get('impact', {}),  # Impact details (delays, etc.)
            'delay_minutes': self._extract_delay(details.get('impact', {})),
            
            # Warning classification
            'display_type': details.get('displayType', ''),  # Type of warning display
            'category': details.get('category', ''),
            'severity': self._determine_severity(details),
            'is_roadworks': details.get('isRoadworks', False),
            'is_accident': self._check_if_accident(details),
            'is_congestion': self._check_if_congestion(details),
            
            # Additional metadata
            'affected_lanes': self._extract_affected_lanes(details),
            'speed_limit': self._extract_speed_limit(details),
            'detour_available': details.get('detour', False),
            'weather_related': self._check_weather_related(details),
            
            # Source information
            'source': 'Autobahn GmbH API',
            'raw_data': json.dumps(details)  # Store complete raw data
        }
        
        return warning_data
    
    def _generate_hash(self, details: Dict) -> str:
        """Generate unique hash for warning deduplication"""
        unique_string = f"{details.get('identifier')}_{details.get('startTimestamp')}"
        return hashlib.md5(unique_string.encode()).hexdigest()
    
    def _calculate_duration(self, start: str, end: str) -> Optional[int]:
        """Calculate duration of warning in minutes"""
        try:
            if start and end:
                start_dt = datetime.fromisoformat(start.replace('Z', '+00:00'))
                end_dt = datetime.fromisoformat(end.replace('Z', '+00:00'))
                return int((end_dt - start_dt).total_seconds() / 60)
        except:
            return None
        return None
    
    def _extract_delay(self, impact: Dict) -> Optional[int]:
        """Extract delay information from impact data"""
        if isinstance(impact, dict):
            return impact.get('delay', impact.get('delayInMinutes'))
        return None
    
    def _determine_severity(self, details: Dict) -> int:
        """
        Determine warning severity level (1-5)
        5 = Critical/Danger, 4 = Accident, 3 = Major delay, 2 = Minor delay, 1 = Info
        """
        text = (details.get('title', '') + ' ' + details.get('subtitle', '')).lower()
        
        if any(word in text for word in ['gefahr', 'danger', 'unfall', 'accident', 'vollsperrung']):
            return 5
        elif any(word in text for word in ['stau', 'congestion', 'verzögerung']):
            return 3
        elif details.get('isRoadworks'):
            return 2
        else:
            return 1
    
    def _check_if_accident(self, details: Dict) -> bool:
        """Check if warning is related to an accident"""
        text = str(details).lower()
        accident_keywords = ['unfall', 'accident', 'crash', 'kollision', 'verunglückt']
        return any(keyword in text for keyword in accident_keywords)
    
    def _check_if_congestion(self, details: Dict) -> bool:
        """Check if warning is related to traffic congestion"""
        text = str(details).lower()
        congestion_keywords = ['stau', 'congestion', 'traffic jam', 'stockender verkehr', 'zähfließend']
        return any(keyword in text for keyword in congestion_keywords)
    
    def _extract_affected_lanes(self, details: Dict) -> str:
        """Extract information about affected lanes"""
        desc = ' '.join(details.get('description', [])) if isinstance(details.get('description'), list) else str(details.get('description', ''))
        # Look for lane information in description
        if 'spur' in desc.lower() or 'lane' in desc.lower():
            return desc  # Would need more sophisticated parsing
        return ''
    
    def _extract_speed_limit(self, details: Dict) -> Optional[int]:
        """Extract temporary speed limit if mentioned"""
        text = str(details).lower()
        import re
        speed_pattern = r'(\d+)\s*km/h'
        match = re.search(speed_pattern, text)
        if match:
            return int(match.group(1))
        return None
    
    def _check_weather_related(self, details: Dict) -> bool:
        """Check if warning is weather-related"""
        text = str(details).lower()
        weather_keywords = ['regen', 'rain', 'schnee', 'snow', 'eis', 'ice', 'nebel', 'fog', 'sturm', 'storm', 'gewitter', 'wetter', 'weather']
        return any(keyword in text for keyword in weather_keywords)
    
    def _get_warning_history(self, warning_id: str) -> List[Dict]:
        """Get historical data for a specific warning if available"""
        return self.warning_history.get(warning_id, [])
    
    def _enrich_warning_data(self, warning_data: Dict) -> Dict:
        """Add calculated fields and enrichments to warning data"""
        # Add day of week and hour
        if warning_data.get('start_timestamp'):
            try:
                dt = datetime.fromisoformat(warning_data['start_timestamp'].replace('Z', '+00:00'))
                warning_data['day_of_week'] = dt.strftime('%A')
                warning_data['hour_of_day'] = dt.hour
                warning_data['is_weekend'] = dt.weekday() >= 5
                warning_data['is_rush_hour'] = dt.hour in [7, 8, 9, 16, 17, 18]
            except:
                pass
        
        # Add coordinate precision for mapping
        if warning_data.get('latitude') and warning_data.get('longitude'):
            warning_data['has_precise_location'] = True
            warning_data['coordinate_string'] = f"{warning_data['latitude']},{warning_data['longitude']}"
        else:
            warning_data['has_precise_location'] = False
        
        return warning_data
    
    def collect_comprehensive_warnings(self, 
                                      highways: List[str] = None,
                                      parallel: bool = True,
                                      save_intermediate: bool = True) -> pd.DataFrame:
        """
        Collect comprehensive warning data from multiple highways
        
        Args:
            highways: List of highways to process (None for all)
            parallel: Use parallel processing for faster collection
            save_intermediate: Save data after each highway
            
        Returns:
            DataFrame with all collected warnings
        """
        if highways is None:
            highways = self.get_all_autobahns()
        
        logger.info(f"Starting comprehensive collection for {len(highways)} highways")
        all_warnings = []
        
        if parallel:
            # Use thread pool for parallel processing
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(self.get_traffic_warnings_detailed, hw) for hw in highways]
                for future in futures:
                    warnings = future.result()
                    all_warnings.extend(warnings)
                    if save_intermediate and warnings:
                        self._save_intermediate(warnings)
        else:
            # Sequential processing
            for highway in highways:
                logger.info(f"Processing {highway}...")
                warnings = self.get_traffic_warnings_detailed(highway)
                all_warnings.extend(warnings)
                
                if save_intermediate and warnings:
                    self._save_intermediate(warnings)
                
                time.sleep(0.5)  # Rate limiting
        
        # Create DataFrame
        df = pd.DataFrame(all_warnings)
        
        # Add summary statistics
        self._generate_statistics(df)
        
        return df
    
    def _save_intermediate(self, warnings: List[Dict]):
        """Save intermediate results during collection"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = self.raw_dir / f"warnings_intermediate_{timestamp}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(warnings, f, indent=2, ensure_ascii=False)
    
    def analyze_warning_patterns(self, df: pd.DataFrame) -> Dict:
        """
        Analyze patterns in warning data
        
        Args:
            df: DataFrame with warning data
            
        Returns:
            Dictionary with analysis results
        """
        analysis = {}
        
        if not df.empty:
            # Frequency analysis
            analysis['total_warnings'] = len(df)
            analysis['highways_affected'] = df['highway'].nunique()
            analysis['warnings_per_highway'] = df['highway'].value_counts().to_dict()
            
            # Severity distribution
            if 'severity' in df.columns:
                analysis['severity_distribution'] = df['severity'].value_counts().to_dict()
                analysis['critical_warnings'] = len(df[df['severity'] >= 4])
            
            # Type distribution
            analysis['accidents'] = len(df[df['is_accident'] == True]) if 'is_accident' in df.columns else 0
            analysis['roadworks'] = len(df[df['is_roadworks'] == True]) if 'is_roadworks' in df.columns else 0
            analysis['congestions'] = len(df[df['is_congestion'] == True]) if 'is_congestion' in df.columns else 0
            
            # Temporal patterns
            if 'hour_of_day' in df.columns:
                analysis['hourly_distribution'] = df['hour_of_day'].value_counts().sort_index().to_dict()
            if 'day_of_week' in df.columns:
                analysis['daily_distribution'] = df['day_of_week'].value_counts().to_dict()
            
            # Geographic coverage
            valid_coords = df[(df['latitude'].notna()) & (df['longitude'].notna())]
            analysis['warnings_with_coordinates'] = len(valid_coords)
            analysis['coordinate_coverage_percent'] = (len(valid_coords) / len(df)) * 100 if len(df) > 0 else 0
            
            # Impact analysis
            if 'delay_minutes' in df.columns:
                delays = df['delay_minutes'].dropna()
                if not delays.empty:
                    analysis['average_delay_minutes'] = delays.mean()
                    analysis['max_delay_minutes'] = delays.max()
                    analysis['total_delay_hours'] = delays.sum() / 60
        
        return analysis
    
    def _generate_statistics(self, df: pd.DataFrame):
        """Generate and save statistics about collected data"""
        stats = self.analyze_warning_patterns(df)
        stats['collection_timestamp'] = datetime.now().isoformat()
        stats['data_source'] = 'Autobahn GmbH API'
        
        # Save statistics
        stats_file = self.processed_dir / f"statistics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Statistics saved to {stats_file}")
    
    def save_all_formats(self, df: pd.DataFrame, base_filename: str):
        """
        Save data in multiple formats for different use cases
        
        Args:
            df: DataFrame to save
            base_filename: Base name for output files
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # CSV for spreadsheet analysis
        csv_file = self.processed_dir / f"{base_filename}_{timestamp}.csv"
        df.to_csv(csv_file, index=False, encoding='utf-8')
        logger.info(f"Saved CSV: {csv_file}")
        
        # JSON for web applications
        json_file = self.processed_dir / f"{base_filename}_{timestamp}.json"
        df.to_json(json_file, orient='records', indent=2, force_ascii=False, date_format='iso')
        logger.info(f"Saved JSON: {json_file}")
        
        # GeoJSON for mapping
        geojson_file = self.processed_dir / f"{base_filename}_{timestamp}.geojson"
        self._save_as_geojson(df, geojson_file)
        logger.info(f"Saved GeoJSON: {geojson_file}")
        
        # Excel with multiple sheets
        excel_file = self.processed_dir / f"{base_filename}_{timestamp}.xlsx"
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            # Main data
            df.to_excel(writer, sheet_name='Traffic Warnings', index=False)
            
            # Summary by highway
            summary = df.groupby('highway').agg({
                'warning_id': 'count',
                'severity': 'mean',
                'delay_minutes': 'mean'
            }).round(2)
            summary.columns = ['warning_count', 'avg_severity', 'avg_delay_min']
            summary.to_excel(writer, sheet_name='Highway Summary')
            
            # Severity breakdown
            if 'severity' in df.columns:
                severity_breakdown = pd.crosstab(df['highway'], df['severity'])
                severity_breakdown.to_excel(writer, sheet_name='Severity Breakdown')
        
        logger.info(f"Saved Excel: {excel_file}")
    
    def _save_as_geojson(self, df: pd.DataFrame, filepath: Path):
        """Save DataFrame as GeoJSON for mapping applications"""
        features = []
        
        for _, row in df.iterrows():
            if pd.notna(row.get('latitude')) and pd.notna(row.get('longitude')):
                properties = row.to_dict()
                # Remove raw_data field for cleaner GeoJSON
                properties.pop('raw_data', None)
                
                # Convert timestamps to strings
                for key in ['start_timestamp', 'end_timestamp', 'last_updated']:
                    if key in properties and properties[key]:
                        properties[key] = str(properties[key])
                
                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [float(row['longitude']), float(row['latitude'])]
                    },
                    "properties": properties
                }
                features.append(feature)
        
        geojson = {
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "generated": datetime.now().isoformat(),
                "source": "Autobahn GmbH Traffic Warning API",
                "total_warnings": len(df),
                "warnings_with_coordinates": len(features)
            }
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(geojson, f, indent=2, ensure_ascii=False)
    
    def run_continuous_monitoring(self, highways: List[str], interval_minutes: int = 15, duration_hours: int = 1):
        """
        Run continuous monitoring of traffic warnings
        
        Args:
            highways: List of highways to monitor
            interval_minutes: Collection interval in minutes
            duration_hours: Total monitoring duration in hours
        """
        logger.info(f"Starting continuous monitoring for {duration_hours} hours")
        end_time = datetime.now() + timedelta(hours=duration_hours)
        
        while datetime.now() < end_time:
            logger.info(f"Collection cycle started at {datetime.now()}")
            
            # Collect current warnings
            df = self.collect_comprehensive_warnings(highways, parallel=True)
            
            # Save with timestamp
            self.save_all_formats(df, "traffic_warnings_monitoring")
            
            # Analyze patterns
            analysis = self.analyze_warning_patterns(df)
            logger.info(f"Current statistics: {analysis}")
            
            # Wait for next cycle
            logger.info(f"Waiting {interval_minutes} minutes until next collection...")
            time.sleep(interval_minutes * 60)
        
        logger.info("Continuous monitoring completed")


def main():
    """Main execution function with various collection modes"""
    
    # Initialize scraper
    scraper = EnhancedTrafficWarningScraper(output_dir="enhanced_traffic_warnings")
    
    print("\n" + "="*70)
    print("ENHANCED TRAFFIC WARNING DATA SCRAPER")
    print("="*70)
    print("\nSelect collection mode:")
    print("1. Quick collection (5 major autobahns)")
    print("2. Comprehensive collection (all autobahns)")
    print("3. Continuous monitoring (real-time updates)")
    print("4. Custom highways")
    
    choice = input("\nEnter your choice (1-4): ").strip()
    
    if choice == "1":
        # Quick collection
        print("\nStarting quick collection...")
        highways = ['A1', 'A2', 'A3', 'A7', 'A9']
        df = scraper.collect_comprehensive_warnings(highways)
        
    elif choice == "2":
        # Comprehensive collection
        print("\nStarting comprehensive collection (this may take several minutes)...")
        df = scraper.collect_comprehensive_warnings()
        
    elif choice == "3":
        # Continuous monitoring
        highways = input("Enter highways to monitor (comma-separated, e.g., A1,A2,A3): ").split(',')
        highways = [h.strip() for h in highways]
        duration = float(input("Enter monitoring duration in hours: "))
        interval = int(input("Enter update interval in minutes: "))
        scraper.run_continuous_monitoring(highways, interval, duration)
        return
        
    elif choice == "4":
        # Custom highways
        highways = input("Enter highways (comma-separated, e.g., A1,A2,B31): ").split(',')
        highways = [h.strip() for h in highways]
        df = scraper.collect_comprehensive_warnings(highways)
    
    else:
        print("Invalid choice. Running quick collection...")
        highways = [
            # Main long-distance routes
            'A1', 'A2', 'A3', 'A4', 'A5', 'A6', 'A7', 'A8', 'A9',
            
            # Regional routes (Berlin, East Germany)
            'A10', 'A11', 'A12', 'A13', 'A14', 'A15', 'A17', 'A19',
            
            # Regional routes (North Germany)
            'A20', 'A21', 'A23', 'A24', 'A25', 'A26', 'A27', 'A28', 'A29',
            
            # Regional routes (Lower Saxony, East Westphalia)
            'A30', 'A31', 'A33', 'A36', 'A37', 'A38', 'A39',
            
            # Regional routes (Rhine-Ruhr, Hesse)
            'A40', 'A42', 'A43', 'A44', 'A45', 'A46', 'A48', 'A49',
            
            # Regional routes (Rhine-Ruhr, Cologne)
            'A52', 'A57', 'A59',
            
            # Regional routes (Rhineland-Palatinate, Saarland, South Hesse)
            'A60', 'A61', 'A62', 'A63', 'A64', 'A65', 'A66', 'A67',
            
            # Regional routes (Franconia, Thuringia, Saxony)
            'A70', 'A71', 'A72', 'A73',
            
            # Regional routes (Baden-Württemberg)
            'A81',
            
            # Regional routes (Bavaria)
            'A92', 'A93', 'A94', 'A95', 'A96', 'A98', 'A99',
            
            # Urban & connector routes (Berlin)
            'A100', 'A103', 'A111', 'A113', 'A114', 'A115', 'A117',
            
            # Urban & connector routes (Hamburg/North)
            'A210', 'A215', 'A226', 'A250', 'A252', 'A253', 'A255', 'A261', 'A270', 'A280', 
            'A281', 'A293',
            
            # Urban & connector routes (Hanover/Central)
            'A352', 'A369', 'A376', 'A388', 'A391', 'A392', 'A395',
            
            # Urban & connector routes (Rhine-Ruhr)
            'A445', 'A448', 'A480', 'A485',
            
            # Urban & connector routes (Cologne/Lower Rhine)
            'A516', 'A524', 'A535', 'A540', 'A542', 'A544', 'A553', 'A555', 'A559', 
            'A560', 'A562', 'A565', 'A571', 'A573',
            
            # Urban & connector routes (Rhine-Main/Southwest)
            'A602', 'A620', 'A623', 'A643', 'A648', 'A650', 'A652', 'A656', 'A659', 
            'A661', 'A671', 'A672',
            
            # Urban & connector routes (Baden-Württemberg)
            'A831', 'A861', 'A864',
            
            # Urban & connector routes (Bavaria)
            'A952', 'A980', 'A995'
        ]

        df = scraper.collect_comprehensive_warnings(highways)
    
    # Save in all formats
    scraper.save_all_formats(df, "traffic_warnings_complete")
    
    # Display analysis
    analysis = scraper.analyze_warning_patterns(df)
    
    print("\n" + "="*70)
    print("COLLECTION SUMMARY")
    print("="*70)
    print(f"Total warnings collected: {analysis.get('total_warnings', 0)}")
    print(f"Highways covered: {analysis.get('highways_affected', 0)}")
    print(f"Critical warnings: {analysis.get('critical_warnings', 0)}")
    print(f"Accidents: {analysis.get('accidents', 0)}")
    print(f"Roadworks: {analysis.get('roadworks', 0)}")
    print(f"Traffic jams: {analysis.get('congestions', 0)}")
    print(f"Warnings with GPS coordinates: {analysis.get('warnings_with_coordinates', 0)} ({analysis.get('coordinate_coverage_percent', 0):.1f}%)")
    
    if 'average_delay_minutes' in analysis:
        print(f"Average delay: {analysis['average_delay_minutes']:.1f} minutes")
        print(f"Maximum delay: {analysis['max_delay_minutes']:.0f} minutes")
    
    print(f"\nData saved to: {scraper.output_dir}")
    print("\nFiles created:")
    for file in sorted(scraper.processed_dir.glob("*")):
        if file.is_file():
            size = file.stat().st_size / 1024
            print(f"  - {file.name} ({size:.1f} KB)")


if __name__ == "__main__":
    main()