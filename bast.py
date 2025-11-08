"""
BASt Real-Time Traffic Data Scraper for Germany
Scrapes real traffic data from Bundesanstalt für Straßenwesen (BASt) 
and Autobahn GmbH API including location coordinates (longitude/latitude)

Author: Traffic Data Analytics
Date: 2025
"""

import requests
import pandas as pd
import json
import zipfile
import io
import os
from datetime import datetime, timedelta
import time
from typing import Dict, List, Optional, Tuple
import logging
import re
from pathlib import Path
import xml.etree.ElementTree as ET
import csv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BAStRealTimeTrafficScraper:
    """Real-time scraper for BASt traffic data from German highways and federal roads"""
    
    def __init__(self, output_dir: str = "bast_realtime_data"):
        """
        Initialize the BASt real-time traffic scraper
        
        Args:
            output_dir: Directory to save scraped data
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # BASt official URLs
        self.base_url = "https://www.bast.de"
        self.data_url = f"{self.base_url}/DE/Publikationen/Daten/Verkehrstechnik"
        
        # Autobahn GmbH API endpoints (real-time data)
        self.autobahn_api_base = "https://verkehr.autobahn.de/o/autobahn"
        
        # Mobilithek API (successor to MDM)
        self.mobilithek_url = "https://mobilithek.info"
        
        # Headers for requests
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/html, application/xml',
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8'
        }
        
        # Station metadata storage
        self.stations_metadata = {}
        self.traffic_data = []
        
    def get_autobahn_list(self) -> List[str]:
        """
        Get list of all available Autobahns from API
        
        Returns:
            List of autobahn identifiers (A1, A2, A3, etc.)
        """
        try:
            response = requests.get(
                f"{self.autobahn_api_base}/",
                headers=self.headers,
                timeout=30
            )
            if response.status_code == 200:
                data = response.json()
                autobahns = data.get('roads', [])
                logger.info(f"Found {len(autobahns)} autobahns")
                return autobahns
            else:
                logger.warning(f"Failed to get autobahn list: {response.status_code}")
                # Fallback to common autobahns
                return ['A1', 'A2', 'A3', 'A4', 'A5', 'A6', 'A7', 'A8', 'A9', 'A10']
        except Exception as e:
            logger.error(f"Error fetching autobahn list: {e}")
            # Return most common autobahns as fallback
            return ['A1', 'A2', 'A3', 'A4', 'A5', 'A6', 'A7', 'A8', 'A9', 'A10']
    
    def get_webcam_locations(self, autobahn: str) -> pd.DataFrame:
        """
        Get webcam locations with coordinates for a specific autobahn
        
        Args:
            autobahn: Autobahn identifier (e.g., 'A1')
            
        Returns:
            DataFrame with webcam locations and coordinates
        """
        webcams = []
        try:
            url = f"{self.autobahn_api_base}/{autobahn}/services/webcam"
            response = requests.get(url, headers=self.headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                for webcam in data.get('webcam', []):
                    # Get detailed information
                    detail_url = f"{self.autobahn_api_base}/details/webcam/{webcam['identifier']}"
                    detail_response = requests.get(detail_url, headers=self.headers, timeout=30)
                    
                    if detail_response.status_code == 200:
                        details = detail_response.json()
                        coord = details.get('coordinate', {})
                        webcams.append({
                            'autobahn': autobahn,
                            'webcam_id': webcam.get('identifier'),
                            'title': details.get('title'),
                            'subtitle': details.get('subtitle'),
                            'latitude': coord.get('lat'),
                            'longitude': coord.get('long'),
                            'operator': details.get('operator'),
                            'direction': details.get('direction'),
                            'link_url': details.get('linkurl'),
                            'image_url': details.get('imageurl')
                        })
                    time.sleep(0.2)  # Rate limiting
                    
        except Exception as e:
            logger.error(f"Error fetching webcam data for {autobahn}: {e}")
            
        return pd.DataFrame(webcams)
    
    def get_traffic_warnings(self, autobahn: str) -> pd.DataFrame:
        """
        Get current traffic warnings and incidents for a specific autobahn
        
        Args:
            autobahn: Autobahn identifier (e.g., 'A1')
            
        Returns:
            DataFrame with traffic warnings including locations
        """
        warnings = []
        try:
            url = f"{self.autobahn_api_base}/{autobahn}/services/warning"
            response = requests.get(url, headers=self.headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                for warning in data.get('warning', []):
                    # Get detailed information
                    detail_url = f"{self.autobahn_api_base}/details/warning/{warning['identifier']}"
                    detail_response = requests.get(detail_url, headers=self.headers, timeout=30)
                    
                    if detail_response.status_code == 200:
                        details = detail_response.json()
                        coord = details.get('coordinate', {})
                        warnings.append({
                            'autobahn': autobahn,
                            'warning_id': warning.get('identifier'),
                            'title': details.get('title'),
                            'subtitle': details.get('subtitle'),
                            'description': ' '.join(details.get('description', [])),
                            'latitude': coord.get('lat'),
                            'longitude': coord.get('long'),
                            'is_roadworks': details.get('isRoadworks', False),
                            'start_timestamp': details.get('startTimestamp'),
                            'display_type': details.get('displayType'),
                            'extent': details.get('extent')
                        })
                    time.sleep(0.2)  # Rate limiting
                    
        except Exception as e:
            logger.error(f"Error fetching warning data for {autobahn}: {e}")
            
        return pd.DataFrame(warnings)
    
    def get_roadworks(self, autobahn: str) -> pd.DataFrame:
        """
        Get current roadworks for a specific autobahn
        
        Args:
            autobahn: Autobahn identifier (e.g., 'A1')
            
        Returns:
            DataFrame with roadworks including locations
        """
        roadworks = []
        try:
            url = f"{self.autobahn_api_base}/{autobahn}/services/roadworks"
            response = requests.get(url, headers=self.headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                for work in data.get('roadworks', []):
                    # Get detailed information
                    detail_url = f"{self.autobahn_api_base}/details/roadworks/{work['identifier']}"
                    detail_response = requests.get(detail_url, headers=self.headers, timeout=30)
                    
                    if detail_response.status_code == 200:
                        details = detail_response.json()
                        coord = details.get('coordinate', {})
                        roadworks.append({
                            'autobahn': autobahn,
                            'roadwork_id': work.get('identifier'),
                            'title': details.get('title'),
                            'subtitle': details.get('subtitle'),
                            'description': ' '.join(details.get('description', [])),
                            'latitude': coord.get('lat'),
                            'longitude': coord.get('long'),
                            'start_timestamp': details.get('startTimestamp'),
                            'display_type': details.get('displayType'),
                            'extent': details.get('extent'),
                            'direction': details.get('direction'),
                            'impact': details.get('impact', {})
                        })
                    time.sleep(0.2)  # Rate limiting
                    
        except Exception as e:
            logger.error(f"Error fetching roadworks data for {autobahn}: {e}")
            
        return pd.DataFrame(roadworks)
    
    def get_parking_areas(self, autobahn: str) -> pd.DataFrame:
        """
        Get parking areas with occupancy data for a specific autobahn
        
        Args:
            autobahn: Autobahn identifier (e.g., 'A1')
            
        Returns:
            DataFrame with parking areas including locations
        """
        parking_areas = []
        try:
            url = f"{self.autobahn_api_base}/{autobahn}/services/parking_lorry"
            response = requests.get(url, headers=self.headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                for parking in data.get('parking_lorry', []):
                    # Get detailed information
                    detail_url = f"{self.autobahn_api_base}/details/parking_lorry/{parking['identifier']}"
                    detail_response = requests.get(detail_url, headers=self.headers, timeout=30)
                    
                    if detail_response.status_code == 200:
                        details = detail_response.json()
                        coord = details.get('coordinate', {})
                        parking_areas.append({
                            'autobahn': autobahn,
                            'parking_id': parking.get('identifier'),
                            'title': details.get('title'),
                            'subtitle': details.get('subtitle'),
                            'description': ' '.join(details.get('description', [])),
                            'latitude': coord.get('lat'),
                            'longitude': coord.get('long'),
                            'is_blocked': details.get('isBlocked', 'unknown'),
                            'lorry_parking_feature_icons': details.get('lorryParkingFeatureIcons', []),
                            'extent': details.get('extent'),
                            'direction': details.get('direction')
                        })
                    time.sleep(0.2)  # Rate limiting
                    
        except Exception as e:
            logger.error(f"Error fetching parking data for {autobahn}: {e}")
            
        return pd.DataFrame(parking_areas)
    
    def get_electric_charging_stations(self, autobahn: str) -> pd.DataFrame:
        """
        Get electric charging stations for a specific autobahn
        
        Args:
            autobahn: Autobahn identifier (e.g., 'A1')
            
        Returns:
            DataFrame with charging stations including locations
        """
        charging_stations = []
        try:
            url = f"{self.autobahn_api_base}/{autobahn}/services/electric_charging_station"
            response = requests.get(url, headers=self.headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                for station in data.get('electric_charging_station', []):
                    # Get detailed information
                    detail_url = f"{self.autobahn_api_base}/details/electric_charging_station/{station['identifier']}"
                    detail_response = requests.get(detail_url, headers=self.headers, timeout=30)
                    
                    if detail_response.status_code == 200:
                        details = detail_response.json()
                        coord = details.get('coordinate', {})
                        charging_stations.append({
                            'autobahn': autobahn,
                            'station_id': station.get('identifier'),
                            'title': details.get('title'),
                            'subtitle': details.get('subtitle'),
                            'description': ' '.join(details.get('description', [])),
                            'latitude': coord.get('lat'),
                            'longitude': coord.get('long'),
                            'extent': details.get('extent'),
                            'direction': details.get('direction')
                        })
                    time.sleep(0.2)  # Rate limiting
                    
        except Exception as e:
            logger.error(f"Error fetching charging station data for {autobahn}: {e}")
            
        return pd.DataFrame(charging_stations)
    
    def get_closures(self, autobahn: str) -> pd.DataFrame:
        """
        Get road closures for a specific autobahn
        
        Args:
            autobahn: Autobahn identifier (e.g., 'A1')
            
        Returns:
            DataFrame with closures including locations
        """
        closures = []
        try:
            url = f"{self.autobahn_api_base}/{autobahn}/services/closure"
            response = requests.get(url, headers=self.headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                for closure in data.get('closure', []):
                    # Get detailed information
                    detail_url = f"{self.autobahn_api_base}/details/closure/{closure['identifier']}"
                    detail_response = requests.get(detail_url, headers=self.headers, timeout=30)
                    
                    if detail_response.status_code == 200:
                        details = detail_response.json()
                        coord = details.get('coordinate', {})
                        closures.append({
                            'autobahn': autobahn,
                            'closure_id': closure.get('identifier'),
                            'title': details.get('title'),
                            'subtitle': details.get('subtitle'),
                            'description': ' '.join(details.get('description', [])),
                            'latitude': coord.get('lat'),
                            'longitude': coord.get('long'),
                            'start_timestamp': details.get('startTimestamp'),
                            'extent': details.get('extent'),
                            'direction': details.get('direction')
                        })
                    time.sleep(0.2)  # Rate limiting
                    
        except Exception as e:
            logger.error(f"Error fetching closure data for {autobahn}: {e}")
            
        return pd.DataFrame(closures)
    
    def download_bast_hourly_data(self, year: int, month: int) -> bool:
        """
        Download BASt hourly traffic count data
        
        Args:
            year: Year of data
            month: Month of data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # BASt data is typically available as monthly ZIP files
            # Format: Rohdaten_YYYY_MM.zip or similar
            month_str = f"{month:02d}"
            year_str = str(year)
            
            # Try different possible URL patterns
            possible_urls = [
                f"{self.data_url}/Rohdaten_{year_str}_{month_str}.zip",
                f"{self.data_url}/DZ_{year_str}_{month_str}.zip",
                f"{self.data_url}/Stundenwerte_{year_str}_{month_str}.zip"
            ]
            
            for url in possible_urls:
                try:
                    logger.info(f"Trying to download from: {url}")
                    response = requests.get(url, headers=self.headers, timeout=60)
                    if response.status_code == 200:
                        # Save the ZIP file
                        filename = f"bast_data_{year_str}_{month_str}.zip"
                        filepath = self.output_dir / filename
                        with open(filepath, 'wb') as f:
                            f.write(response.content)
                        logger.info(f"Successfully downloaded BASt data to {filepath}")
                        
                        # Extract and process the data
                        self.process_bast_zip(filepath)
                        return True
                except Exception as e:
                    logger.warning(f"Failed to download from {url}: {e}")
                    
        except Exception as e:
            logger.error(f"Error downloading BASt data: {e}")
            
        return False
    
    def process_bast_zip(self, zip_filepath: Path):
        """
        Process downloaded BASt ZIP file to extract traffic data and metadata
        
        Args:
            zip_filepath: Path to the ZIP file
        """
        try:
            with zipfile.ZipFile(zip_filepath, 'r') as zf:
                # Extract all files
                extract_dir = self.output_dir / "extracted"
                extract_dir.mkdir(exist_ok=True)
                zf.extractall(extract_dir)
                
                # Look for metadata CSV files
                for filename in zf.namelist():
                    if filename.endswith('.csv'):
                        logger.info(f"Found CSV file: {filename}")
                        # Process CSV files here
                        # This would involve parsing station metadata with coordinates
                        
        except Exception as e:
            logger.error(f"Error processing ZIP file: {e}")
    
    def save_to_csv(self, df: pd.DataFrame, filename: str):
        """Save DataFrame to CSV file"""
        if not df.empty:
            filepath = self.output_dir / filename
            df.to_csv(filepath, index=False, encoding='utf-8')
            logger.info(f"Saved {len(df)} records to {filepath}")
        else:
            logger.warning(f"No data to save for {filename}")
    
    def save_to_json(self, df: pd.DataFrame, filename: str):
        """Save DataFrame to JSON file"""
        if not df.empty:
            filepath = self.output_dir / filename
            df.to_json(filepath, orient='records', indent=2, force_ascii=False)
            logger.info(f"Saved {len(df)} records to {filepath}")
    
    def save_to_geojson(self, df: pd.DataFrame, filename: str):
        """
        Save DataFrame to GeoJSON format for mapping applications
        
        Args:
            df: DataFrame with latitude and longitude columns
            filename: Output filename
        """
        if df.empty:
            logger.warning(f"No data to save for {filename}")
            return
            
        features = []
        for _, row in df.iterrows():
            if pd.notna(row.get('latitude')) and pd.notna(row.get('longitude')):
                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [row['longitude'], row['latitude']]
                    },
                    "properties": {k: v for k, v in row.items() 
                                 if k not in ['latitude', 'longitude']}
                }
                features.append(feature)
        
        geojson = {
            "type": "FeatureCollection",
            "features": features
        }
        
        filepath = self.output_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(geojson, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(features)} features to {filepath}")
    
    def run(self, limit_autobahns: int = None):
        """
        Main execution method to collect all real-time traffic data
        
        Args:
            limit_autobahns: Limit number of autobahns to process (None for all)
        """
        logger.info("Starting BASt real-time traffic data collection...")
        
        # Get list of autobahns
        autobahns = self.get_autobahn_list()
        
        if limit_autobahns:
            autobahns = autobahns[:limit_autobahns]
            logger.info(f"Processing {limit_autobahns} autobahns: {autobahns}")
        
        # Collect data for each autobahn
        all_webcams = []
        all_warnings = []
        all_roadworks = []
        all_parking = []
        all_charging = []
        all_closures = []
        
        for autobahn in autobahns:
            logger.info(f"Processing {autobahn}...")
            
            # Get webcam locations
            webcams_df = self.get_webcam_locations(autobahn)
            if not webcams_df.empty:
                all_webcams.append(webcams_df)
            
            # Get traffic warnings
            warnings_df = self.get_traffic_warnings(autobahn)
            if not warnings_df.empty:
                all_warnings.append(warnings_df)
            
            # Get roadworks
            roadworks_df = self.get_roadworks(autobahn)
            if not roadworks_df.empty:
                all_roadworks.append(roadworks_df)
            
            # Get parking areas
            parking_df = self.get_parking_areas(autobahn)
            if not parking_df.empty:
                all_parking.append(parking_df)
            
            # Get charging stations
            charging_df = self.get_electric_charging_stations(autobahn)
            if not charging_df.empty:
                all_charging.append(charging_df)
            
            # Get closures
            closures_df = self.get_closures(autobahn)
            if not closures_df.empty:
                all_closures.append(closures_df)
            
            # Rate limiting between autobahns
            time.sleep(1)
        
        # Combine and save all data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if all_webcams:
            webcams_combined = pd.concat(all_webcams, ignore_index=True)
            self.save_to_csv(webcams_combined, f'webcam_locations_{timestamp}.csv')
            self.save_to_json(webcams_combined, f'webcam_locations_{timestamp}.json')
            self.save_to_geojson(webcams_combined, f'webcam_locations_{timestamp}.geojson')
        
        if all_warnings:
            warnings_combined = pd.concat(all_warnings, ignore_index=True)
            self.save_to_csv(warnings_combined, f'traffic_warnings_{timestamp}.csv')
            self.save_to_json(warnings_combined, f'traffic_warnings_{timestamp}.json')
            self.save_to_geojson(warnings_combined, f'traffic_warnings_{timestamp}.geojson')
        
        if all_roadworks:
            roadworks_combined = pd.concat(all_roadworks, ignore_index=True)
            self.save_to_csv(roadworks_combined, f'roadworks_{timestamp}.csv')
            self.save_to_json(roadworks_combined, f'roadworks_{timestamp}.json')
            self.save_to_geojson(roadworks_combined, f'roadworks_{timestamp}.geojson')
        
        if all_parking:
            parking_combined = pd.concat(all_parking, ignore_index=True)
            self.save_to_csv(parking_combined, f'parking_areas_{timestamp}.csv')
            self.save_to_json(parking_combined, f'parking_areas_{timestamp}.json')
            self.save_to_geojson(parking_combined, f'parking_areas_{timestamp}.geojson')
        
        if all_charging:
            charging_combined = pd.concat(all_charging, ignore_index=True)
            self.save_to_csv(charging_combined, f'charging_stations_{timestamp}.csv')
            self.save_to_json(charging_combined, f'charging_stations_{timestamp}.json')
            self.save_to_geojson(charging_combined, f'charging_stations_{timestamp}.geojson')
        
        if all_closures:
            closures_combined = pd.concat(all_closures, ignore_index=True)
            self.save_to_csv(closures_combined, f'road_closures_{timestamp}.csv')
            self.save_to_json(closures_combined, f'road_closures_{timestamp}.json')
            self.save_to_geojson(closures_combined, f'road_closures_{timestamp}.geojson')
        
        # Try to download BASt hourly data
        current_date = datetime.now()
        self.download_bast_hourly_data(current_date.year, current_date.month)
        
        # Create summary
        self._create_summary(timestamp)
        
        logger.info("Data collection completed successfully!")
    
    def _create_summary(self, timestamp: str):
        """Create a summary report of collected data"""
        summary = {
            'timestamp': timestamp,
            'collection_date': datetime.now().isoformat(),
            'data_sources': [
                'Autobahn GmbH API',
                'BASt (Bundesanstalt für Straßenwesen)'
            ],
            'files_created': list(self.output_dir.glob(f'*_{timestamp}.*'))
        }
        
        # Count records in each file
        for file in self.output_dir.glob(f'*_{timestamp}.csv'):
            try:
                df = pd.read_csv(file)
                summary[f'{file.stem}_count'] = len(df)
            except:
                pass
        
        # Save summary
        summary_path = self.output_dir / f'collection_summary_{timestamp}.json'
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"Summary saved to {summary_path}")
        
        # Print summary to console
        print("\n" + "="*60)
        print("DATA COLLECTION SUMMARY")
        print("="*60)
        for key, value in summary.items():
            if not key.startswith('files'):
                print(f"{key}: {value}")


def main():
    """Main execution function"""
    # Create scraper instance
    scraper = BAStRealTimeTrafficScraper(output_dir="bast_realtime_traffic")
    
    # Run scraper with limit for demonstration (remove limit for full data)
    # Set to None to process all autobahns, or specify a number to limit
    scraper.run(limit_autobahns=5)  # Process first 5 autobahns
    
    print("\n=== Real-Time Traffic Data Collection Complete ===")
    print(f"Data saved to: {scraper.output_dir}")
    print("\nFiles created:")
    for file in sorted(scraper.output_dir.glob("*")):
        if file.is_file():
            size = file.stat().st_size / 1024  # Size in KB
            print(f"  - {file.name} ({size:.1f} KB)")


if __name__ == "__main__":
    main()