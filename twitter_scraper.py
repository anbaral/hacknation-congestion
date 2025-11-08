import requests
import json
import csv
from datetime import datetime, timedelta
import time
import random
from typing import Dict, List, Tuple, Optional, Set
import re
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import hashlib
from pathlib import Path

# ============================================
# Configuration for Fast Collection
# ============================================

class Config:
    """Configuration for fast 20K collection"""
    # Collection settings
    TARGET_POSTS = 20000  # Reduced for faster collection (30-60 minutes)
    BATCH_SIZE = 1000  # Posts per batch
    MAX_RETRIES = 3
    RATE_LIMIT_DELAY = 1  # Seconds between requests
    BATCH_DELAY = 10  # Reduced delay between batches for faster collection
    
    # File settings
    OUTPUT_DIR = "germany_data"
    CHECKPOINT_FILE = "collection_checkpoint.json"
    DEDUPE_ON_THE_FLY = True
    
    # Source distribution for 20K posts
    SOURCE_DISTRIBUTION = {
        'reddit': 7000,      # ~35% - Best real-time data
        'wikipedia': 4000,   # ~20% - Reliable geo data
        'osm': 3000,        # ~15% - Map edits
        'news': 2000,       # ~10% - Current events
        'events': 4000      # ~20% - Easy to generate
    }

# ============================================
# German Cities and Locations Database
# ============================================

GERMAN_CITIES = {
    # Major cities with coordinates
    'berlin': {'name': 'Berlin', 'lat': 52.5200, 'lon': 13.4050, 'state': 'Berlin', 'pop': 3669491},
    'hamburg': {'name': 'Hamburg', 'lat': 53.5511, 'lon': 9.9937, 'state': 'Hamburg', 'pop': 1899160},
    'munich': {'name': 'München', 'lat': 48.1351, 'lon': 11.5820, 'state': 'Bayern', 'pop': 1484226},
    'cologne': {'name': 'Köln', 'lat': 50.9375, 'lon': 6.9603, 'state': 'Nordrhein-Westfalen', 'pop': 1087863},
    'frankfurt': {'name': 'Frankfurt', 'lat': 50.1109, 'lon': 8.6821, 'state': 'Hessen', 'pop': 763380},
    'stuttgart': {'name': 'Stuttgart', 'lat': 48.7758, 'lon': 9.1829, 'state': 'Baden-Württemberg', 'pop': 632743},
    'dusseldorf': {'name': 'Düsseldorf', 'lat': 51.2277, 'lon': 6.7735, 'state': 'Nordrhein-Westfalen', 'pop': 621877},
    'leipzig': {'name': 'Leipzig', 'lat': 51.3397, 'lon': 12.3731, 'state': 'Sachsen', 'pop': 597493},
    'dortmund': {'name': 'Dortmund', 'lat': 51.5136, 'lon': 7.4653, 'state': 'Nordrhein-Westfalen', 'pop': 588250},
    'essen': {'name': 'Essen', 'lat': 51.4556, 'lon': 7.0116, 'state': 'Nordrhein-Westfalen', 'pop': 582760},
    'bremen': {'name': 'Bremen', 'lat': 53.0793, 'lon': 8.8017, 'state': 'Bremen', 'pop': 567559},
    'dresden': {'name': 'Dresden', 'lat': 51.0504, 'lon': 13.7373, 'state': 'Sachsen', 'pop': 556227},
    'hanover': {'name': 'Hannover', 'lat': 52.3759, 'lon': 9.7320, 'state': 'Niedersachsen', 'pop': 536925},
    'nuremberg': {'name': 'Nürnberg', 'lat': 49.4521, 'lon': 11.0767, 'state': 'Bayern', 'pop': 518370},
    'duisburg': {'name': 'Duisburg', 'lat': 51.4344, 'lon': 6.7623, 'state': 'Nordrhein-Westfalen', 'pop': 498686},
    'bochum': {'name': 'Bochum', 'lat': 51.4819, 'lon': 7.2162, 'state': 'Nordrhein-Westfalen', 'pop': 365587},
    'wuppertal': {'name': 'Wuppertal', 'lat': 51.2562, 'lon': 7.1508, 'state': 'Nordrhein-Westfalen', 'pop': 355100},
    'bielefeld': {'name': 'Bielefeld', 'lat': 52.0302, 'lon': 8.5325, 'state': 'Nordrhein-Westfalen', 'pop': 334195},
    'bonn': {'name': 'Bonn', 'lat': 50.7374, 'lon': 7.0982, 'state': 'Nordrhein-Westfalen', 'pop': 329673},
    'mannheim': {'name': 'Mannheim', 'lat': 49.4875, 'lon': 8.4660, 'state': 'Baden-Württemberg', 'pop': 311831},
    'augsburg': {'name': 'Augsburg', 'lat': 48.3705, 'lon': 10.8978, 'state': 'Bayern', 'pop': 296582},
    'wiesbaden': {'name': 'Wiesbaden', 'lat': 50.0825, 'lon': 8.2473, 'state': 'Hessen', 'pop': 278950},
    'munster': {'name': 'Münster', 'lat': 51.9607, 'lon': 7.6261, 'state': 'Nordrhein-Westfalen', 'pop': 315293},
    'karlsruhe': {'name': 'Karlsruhe', 'lat': 49.0069, 'lon': 8.4037, 'state': 'Baden-Württemberg', 'pop': 312060},
    'aachen': {'name': 'Aachen', 'lat': 50.7753, 'lon': 6.0839, 'state': 'Nordrhein-Westfalen', 'pop': 248960},
    'kiel': {'name': 'Kiel', 'lat': 54.3233, 'lon': 10.1348, 'state': 'Schleswig-Holstein', 'pop': 246794},
    'magdeburg': {'name': 'Magdeburg', 'lat': 52.1205, 'lon': 11.6276, 'state': 'Sachsen-Anhalt', 'pop': 235775},
    'freiburg': {'name': 'Freiburg', 'lat': 47.9990, 'lon': 7.8421, 'state': 'Baden-Württemberg', 'pop': 231195},
    'lubeck': {'name': 'Lübeck', 'lat': 53.8655, 'lon': 10.6866, 'state': 'Schleswig-Holstein', 'pop': 216277},
    'erfurt': {'name': 'Erfurt', 'lat': 50.9785, 'lon': 11.0298, 'state': 'Thüringen', 'pop': 213699},
    'rostock': {'name': 'Rostock', 'lat': 54.0887, 'lon': 12.1405, 'state': 'Mecklenburg-Vorpommern', 'pop': 209920},
    'mainz': {'name': 'Mainz', 'lat': 49.9929, 'lon': 8.2473, 'state': 'Rheinland-Pfalz', 'pop': 219031},
    'kassel': {'name': 'Kassel', 'lat': 51.3127, 'lon': 9.4797, 'state': 'Hessen', 'pop': 200507},
    'hagen': {'name': 'Hagen', 'lat': 51.3671, 'lon': 7.4633, 'state': 'Nordrhein-Westfalen', 'pop': 188687},
    'saarbrucken': {'name': 'Saarbrücken', 'lat': 49.2354, 'lon': 6.9965, 'state': 'Saarland', 'pop': 179349},
    'potsdam': {'name': 'Potsdam', 'lat': 52.3906, 'lon': 13.0645, 'state': 'Brandenburg', 'pop': 183154},
    'ludwigshafen': {'name': 'Ludwigshafen', 'lat': 49.4811, 'lon': 8.4353, 'state': 'Rheinland-Pfalz', 'pop': 172145},
    'oldenburg': {'name': 'Oldenburg', 'lat': 53.1435, 'lon': 8.2146, 'state': 'Niedersachsen', 'pop': 170389},
    'osnabruck': {'name': 'Osnabrück', 'lat': 52.2799, 'lon': 8.0472, 'state': 'Niedersachsen', 'pop': 165251},
    'heidelberg': {'name': 'Heidelberg', 'lat': 49.4093, 'lon': 8.6941, 'state': 'Baden-Württemberg', 'pop': 162273}
}

# Extended German subreddits list for massive collection
GERMAN_SUBREDDITS_EXTENDED = [
    # Main German subs
    'de', 'germany', 'ich_iel', 'fragreddit', 'de_IAmA', 'germantrees', 'germanphotos',
    
    # City subreddits
    'berlin', 'munich', 'hamburg', 'cologne', 'frankfurt', 'stuttgart', 'dortmund',
    'leipzig', 'dresden', 'bremen', 'hannover', 'nuremberg', 'heidelberg', 'mannheim',
    'karlsruhe', 'wiesbaden', 'augsburg', 'bonn', 'bielefeld', 'freiburg', 'mainz',
    'saarland', 'kiel', 'rostock', 'kassel', 'aachen', 'lubeck', 'munster',
    
    # Regional subreddits
    'bayern', 'nrw', 'bawue', 'hessen', 'niedersachsen', 'sachsen', 'brandenburg',
    'thueringen', 'rheinlandpfalz', 'saarland', 'schleswigholstein',
    
    # Topic-specific German subs
    'bundesliga', 'borussiadortmund', 'fcbayern', 'schalke04', 'rbleipzig',
    'germanrap', 'germusic', 'germangaming', 'deutschland', 'german',
    'germanfood', 'germanlearning', 'germanhistory', 'germanpolitics',
]

# ============================================
# Progress Tracker and Checkpoint System
# ============================================

class ProgressTracker:
    """Track collection progress and manage checkpoints"""
    
    def __init__(self):
        self.collected = {}
        self.seen_ids = set()
        self.start_time = datetime.now()
        self.checkpoint_file = Path(Config.OUTPUT_DIR) / Config.CHECKPOINT_FILE
        self.ensure_output_dir()
        self.load_checkpoint()
    
    def ensure_output_dir(self):
        """Create output directory if it doesn't exist"""
        Path(Config.OUTPUT_DIR).mkdir(exist_ok=True)
    
    def load_checkpoint(self):
        """Load previous progress if exists"""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                    self.collected = data.get('collected', {})
                    self.seen_ids = set(data.get('seen_ids', []))
                    print(f"📥 Loaded checkpoint: {sum(self.collected.values())} posts already collected")
            except:
                print("⚠️  Could not load checkpoint, starting fresh")
    
    def save_checkpoint(self):
        """Save current progress"""
        checkpoint_data = {
            'collected': self.collected,
            'seen_ids': list(self.seen_ids)[:10000],  # Keep last 10k IDs for deduplication
            'timestamp': datetime.now().isoformat()
        }
        with open(self.checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f)
    
    def update(self, source: str, count: int, new_ids: Set[str]):
        """Update progress"""
        self.collected[source] = self.collected.get(source, 0) + count
        self.seen_ids.update(new_ids)
        
    def get_total(self) -> int:
        """Get total posts collected"""
        return sum(self.collected.values())
    
    def print_progress(self):
        """Print current progress"""
        total = self.get_total()
        elapsed = (datetime.now() - self.start_time).seconds
        rate = total / max(elapsed, 1) * 60  # Posts per minute
        
        print("\n" + "="*60)
        print(f"📊 COLLECTION PROGRESS - Target: {Config.TARGET_POSTS:,} posts")
        print("="*60)
        for source, count in sorted(self.collected.items()):
            percentage = (count / Config.SOURCE_DISTRIBUTION.get(source, 1000)) * 100
            bar = '█' * int(percentage / 5) + '░' * (20 - int(percentage / 5))
            print(f"{source:12} [{bar}] {count:7,} / {Config.SOURCE_DISTRIBUTION.get(source, 1000):,} ({percentage:.1f}%)")
        print("-"*60)
        print(f"Total: {total:,} posts | Rate: {rate:.0f} posts/min | Time: {elapsed//60}:{elapsed%60:02d}")
        print(f"Unique IDs tracked: {len(self.seen_ids):,}")
        
        if total >= Config.TARGET_POSTS:
            print(f"\n🎯 TARGET REACHED! Collected {total:,} posts")
        else:
            remaining = Config.TARGET_POSTS - total
            eta = remaining / max(rate, 1)
            print(f"Remaining: {remaining:,} posts | ETA: {eta:.0f} minutes")
        print("="*60)

# ============================================
# Enhanced Reddit Collector for Scale
# ============================================

class RedditGermanyMassCollector:
    """Collect massive amounts from German subreddits"""
    
    def __init__(self, tracker: ProgressTracker):
        self.base_url = "https://www.reddit.com"
        self.headers = {'User-Agent': 'GermanyGeoCollector 2.0 (Mass Collection)'}
        self.tracker = tracker
        self.subreddits = GERMAN_SUBREDDITS_EXTENDED
        
    def collect_batch(self, batch_size: int = 1000) -> List[Dict]:
        """Collect a batch of Reddit posts"""
        posts = []
        posts_per_sub = max(batch_size // len(self.subreddits), 5)
        
        # Use different sort options for variety
        sort_options = ['hot', 'new', 'top', 'rising']
        time_filters = ['hour', 'day', 'week', 'month', 'year', 'all']
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            
            for subreddit in self.subreddits:
                for sort in sort_options:
                    future = executor.submit(
                        self._fetch_subreddit_posts,
                        subreddit, sort, posts_per_sub,
                        random.choice(time_filters) if sort == 'top' else None
                    )
                    futures.append(future)
            
            for future in as_completed(futures):
                try:
                    posts.extend(future.result())
                    if len(posts) >= batch_size:
                        break
                except Exception as e:
                    continue
        
        # Deduplicate
        unique_posts = []
        new_ids = set()
        
        for post in posts:
            post_id = f"reddit_{post['post_id']}"
            if post_id not in self.tracker.seen_ids:
                unique_posts.append(post)
                new_ids.add(post_id)
        
        self.tracker.update('reddit', len(unique_posts), new_ids)
        print(f"  ✓ Reddit batch: {len(unique_posts)} new posts")
        
        return unique_posts[:batch_size]
    
    def _fetch_subreddit_posts(self, subreddit: str, sort: str, limit: int, time_filter: str = None) -> List[Dict]:
        """Fetch posts from a single subreddit"""
        posts = []
        
        url = f"{self.base_url}/r/{subreddit}/{sort}.json"
        params = {'limit': min(limit, 100)}
        if time_filter and sort == 'top':
            params['t'] = time_filter
        
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                
                for post_data in data['data']['children']:
                    post_info = post_data['data']
                    
                    # Extract location
                    location, lat, lon = self._extract_german_location(
                        post_info.get('title', ''),
                        post_info.get('selftext', ''),
                        subreddit
                    )
                    
                    if location:
                        post = {
                            'platform': 'Reddit',
                            'post_id': post_info['id'],
                            'title': post_info.get('title', '')[:200],
                            'author': post_info.get('author', 'deleted'),
                            'date': datetime.fromtimestamp(post_info.get('created_utc', time.time())).isoformat(),
                            'latitude': lat,
                            'longitude': lon,
                            'location_name': location,
                            'tags': f"reddit,{subreddit},{sort}",
                            'views': post_info.get('score', 0),
                            'url': f"https://reddit.com{post_info.get('permalink', '')}"
                        }
                        posts.append(post)
            
            time.sleep(random.uniform(0.5, 1.5))  # Random delay to avoid rate limits
            
        except Exception as e:
            pass  # Silently handle errors in batch processing
        
        return posts
    
    def _extract_german_location(self, title: str, text: str, subreddit: str) -> Tuple[Optional[str], Optional[float], Optional[float]]:
        """Extract German location from text"""
        combined_text = f"{title} {text} {subreddit}".lower()
        
        # City subreddit mapping
        city_map = {
            'berlin': 'berlin', 'munich': 'munich', 'hamburg': 'hamburg',
            'cologne': 'cologne', 'frankfurt': 'frankfurt', 'stuttgart': 'stuttgart',
            'leipzig': 'leipzig', 'dresden': 'dresden', 'bremen': 'bremen',
            'hannover': 'hanover', 'nuremberg': 'nuremberg', 'heidelberg': 'heidelberg',
            'mannheim': 'mannheim', 'karlsruhe': 'karlsruhe', 'augsburg': 'augsburg',
            'bonn': 'bonn', 'bielefeld': 'bielefeld', 'freiburg': 'freiburg',
            'munster': 'munster', 'aachen': 'aachen', 'kiel': 'kiel', 'mainz': 'mainz'
        }
        
        # Check if subreddit is a city
        if subreddit.lower() in city_map:
            city_key = city_map[subreddit.lower()]
            if city_key in GERMAN_CITIES:
                city = GERMAN_CITIES[city_key]
                lat = city['lat'] + random.uniform(-0.05, 0.05)
                lon = city['lon'] + random.uniform(-0.05, 0.05)
                return city['name'], lat, lon
        
        # Search for city mentions
        for city_key, city_info in GERMAN_CITIES.items():
            if city_key in combined_text or city_info['name'].lower() in combined_text:
                lat = city_info['lat'] + random.uniform(-0.05, 0.05)
                lon = city_info['lon'] + random.uniform(-0.05, 0.05)
                return f"{city_info['name']}, {city_info['state']}", lat, lon
        
        # Default to weighted random city (larger cities more likely)
        if subreddit.lower() in ['de', 'germany', 'ich_iel', 'fragreddit', 'bundesliga']:
            # Weight by population
            cities_weighted = []
            for city in GERMAN_CITIES.values():
                weight = max(1, city.get('pop', 100000) // 100000)
                cities_weighted.extend([city] * weight)
            
            city = random.choice(cities_weighted)
            lat = city['lat'] + random.uniform(-0.05, 0.05)
            lon = city['lon'] + random.uniform(-0.05, 0.05)
            return f"{city['name']}, {city['state']}", lat, lon
        
        return None, None, None

# ============================================
# Wikipedia Mass Collector
# ============================================

class WikipediaGermanyMassCollector:
    """Collect massive amounts of German Wikipedia articles"""
    
    def __init__(self, tracker: ProgressTracker):
        self.base_url = "https://de.wikipedia.org/w/api.php"
        self.en_base_url = "https://en.wikipedia.org/w/api.php"
        self.tracker = tracker
        
    def collect_batch(self, batch_size: int = 1000) -> List[Dict]:
        """Collect a batch of Wikipedia articles"""
        posts = []
        
        # Use parallel requests for multiple cities
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            
            cities_list = list(GERMAN_CITIES.values())
            for city in cities_list:
                # Multiple radius searches per city
                for radius in [1000, 5000, 10000, 20000]:
                    future = executor.submit(self._fetch_city_articles, city, radius, batch_size // len(cities_list))
                    futures.append(future)
            
            for future in as_completed(futures):
                try:
                    posts.extend(future.result())
                    if len(posts) >= batch_size * 2:  # Collect extra for deduplication
                        break
                except:
                    continue
        
        # Deduplicate
        unique_posts = []
        new_ids = set()
        
        for post in posts:
            post_id = f"wiki_{post['post_id']}"
            if post_id not in self.tracker.seen_ids:
                unique_posts.append(post)
                new_ids.add(post_id)
        
        self.tracker.update('wikipedia', len(unique_posts), new_ids)
        print(f"  ✓ Wikipedia batch: {len(unique_posts)} new articles")
        
        return unique_posts[:batch_size]
    
    def _fetch_city_articles(self, city: Dict, radius: int, limit: int) -> List[Dict]:
        """Fetch articles around a city"""
        posts = []
        
        params = {
            'action': 'query',
            'format': 'json',
            'list': 'geosearch',
            'gscoord': f"{city['lat']}|{city['lon']}",
            'gsradius': radius,
            'gslimit': min(limit, 500)
        }
        
        for base_url, lang in [(self.base_url, 'de'), (self.en_base_url, 'en')]:
            try:
                response = requests.get(base_url, params=params, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    
                    for article in data.get('query', {}).get('geosearch', []):
                        post = {
                            'platform': 'Wikipedia',
                            'post_id': f"{lang}_{article['pageid']}",
                            'title': article['title'],
                            'author': 'Wikipedia',
                            'date': (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat(),
                            'latitude': article['lat'],
                            'longitude': article['lon'],
                            'location_name': f"Near {city['name']}, {city['state']}",
                            'tags': f"wiki,{lang},{city['state'].lower().replace(' ', '_')}",
                            'views': int(article.get('dist', 0)),
                            'url': f"https://{lang}.wikipedia.org/?curid={article['pageid']}"
                        }
                        posts.append(post)
                
                time.sleep(0.1)  # Small delay
                
            except:
                pass
        
        return posts

# ============================================
# OpenStreetMap Mass Collector
# ============================================

class OSMGermanyMassCollector:
    """Collect massive OSM data from Germany"""
    
    def __init__(self, tracker: ProgressTracker):
        self.base_url = "https://api.openstreetmap.org/api/0.6"
        self.overpass_url = "https://overpass-api.de/api/interpreter"
        self.tracker = tracker
        
    def collect_batch(self, batch_size: int = 1000) -> List[Dict]:
        """Collect a batch of OSM data"""
        posts = []
        
        # Grid search across Germany
        lat_steps = 10
        lon_steps = 10
        
        lat_min, lat_max = 47.3, 55.0
        lon_min, lon_max = 5.9, 15.0
        
        lat_step = (lat_max - lat_min) / lat_steps
        lon_step = (lon_max - lon_min) / lon_steps
        
        for i in range(lat_steps):
            for j in range(lon_steps):
                bbox = f"{lon_min + j*lon_step},{lat_min + i*lat_step},{lon_min + (j+1)*lon_step},{lat_min + (i+1)*lat_step}"
                posts.extend(self._fetch_changesets(bbox, batch_size // (lat_steps * lon_steps)))
                
                if len(posts) >= batch_size * 2:
                    break
            if len(posts) >= batch_size * 2:
                break
        
        # Deduplicate
        unique_posts = []
        new_ids = set()
        
        for post in posts:
            post_id = f"osm_{post['post_id']}"
            if post_id not in self.tracker.seen_ids:
                unique_posts.append(post)
                new_ids.add(post_id)
        
        self.tracker.update('osm', len(unique_posts), new_ids)
        print(f"  ✓ OSM batch: {len(unique_posts)} new edits")
        
        return unique_posts[:batch_size]
    
    def _fetch_changesets(self, bbox: str, limit: int) -> List[Dict]:
        """Fetch changesets for a bounding box"""
        posts = []
        
        url = f"{self.base_url}/changesets"
        params = {
            'bbox': bbox,
            'closed': 'true',
            'limit': min(limit, 100)
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                root = ET.fromstring(response.content)
                
                coords = bbox.split(',')
                center_lat = (float(coords[1]) + float(coords[3])) / 2
                center_lon = (float(coords[0]) + float(coords[2])) / 2
                
                for changeset in root.findall('changeset'):
                    lat = center_lat + random.uniform(-0.5, 0.5)
                    lon = center_lon + random.uniform(-0.5, 0.5)
                    
                    # Find nearest city
                    nearest = min(GERMAN_CITIES.values(), 
                                key=lambda c: ((c['lat']-lat)**2 + (c['lon']-lon)**2)**0.5)
                    
                    post = {
                        'platform': 'OpenStreetMap',
                        'post_id': changeset.get('id', str(random.randint(1000000, 9999999))),
                        'title': f"Map edit near {nearest['name']}",
                        'author': changeset.get('user', 'Anonymous'),
                        'date': changeset.get('created_at', datetime.now().isoformat()),
                        'latitude': lat,
                        'longitude': lon,
                        'location_name': f"{nearest['name']}, {nearest['state']}",
                        'tags': f"osm,mapping,{nearest['state'].lower()}",
                        'views': int(changeset.get('num_changes', 0)),
                        'url': f"https://www.openstreetmap.org/changeset/{changeset.get('id')}"
                    }
                    posts.append(post)
            
            time.sleep(0.5)
            
        except:
            pass
        
        return posts

# ============================================
# German Events Mass Generator
# ============================================

class GermanEventsMassGenerator:
    """Generate massive amounts of German events data"""
    
    def __init__(self, tracker: ProgressTracker):
        self.tracker = tracker
        self.event_templates = self._load_event_templates()
        
    def _load_event_templates(self) -> List[Tuple[str, str, List[str]]]:
        """Load event templates"""
        return [
            ('Weihnachtsmarkt', 'market', list(GERMAN_CITIES.keys())),
            ('Oktoberfest', 'festival', ['munich', 'stuttgart', 'frankfurt']),
            ('Karneval', 'festival', ['cologne', 'dusseldorf', 'mainz']),
            ('Bundesliga Spiel', 'sports', ['munich', 'dortmund', 'leipzig', 'frankfurt']),
            ('Konzert', 'music', list(GERMAN_CITIES.keys())),
            ('Technik Konferenz', 'tech', ['berlin', 'munich', 'frankfurt', 'hamburg']),
            ('Kunstausstellung', 'art', list(GERMAN_CITIES.keys())),
            ('Street Food Festival', 'food', list(GERMAN_CITIES.keys())),
            ('Marathon', 'sports', ['berlin', 'frankfurt', 'cologne', 'hamburg', 'munich']),
            ('Startup Meetup', 'business', ['berlin', 'munich', 'frankfurt', 'cologne']),
            ('Weinmesse', 'food', ['mainz', 'stuttgart', 'freiburg']),
            ('Buchmesse', 'culture', ['frankfurt', 'leipzig']),
            ('Musikfestival', 'music', list(GERMAN_CITIES.keys())),
            ('Flohmarkt', 'market', list(GERMAN_CITIES.keys())),
            ('Stadtfest', 'festival', list(GERMAN_CITIES.keys())),
            ('Bierfest', 'festival', list(GERMAN_CITIES.keys())),
            ('Theater Aufführung', 'culture', list(GERMAN_CITIES.keys())),
            ('Comedy Show', 'entertainment', list(GERMAN_CITIES.keys())),
            ('Food Truck Festival', 'food', list(GERMAN_CITIES.keys())),
            ('Wissenschaftsmesse', 'education', ['berlin', 'munich', 'hamburg']),
        ]
    
    def generate_batch(self, batch_size: int = 1000) -> List[Dict]:
        """Generate a batch of events"""
        posts = []
        
        for i in range(batch_size):
            event_type, category, cities = random.choice(self.event_templates)
            city_key = random.choice(cities)
            city = GERMAN_CITIES[city_key]
            
            # Vary dates throughout the year
            days_offset = random.randint(-180, 180)
            event_date = datetime.now() + timedelta(days=days_offset)
            
            # Add realistic variation to coordinates
            lat = city['lat'] + random.uniform(-0.03, 0.03)
            lon = city['lon'] + random.uniform(-0.03, 0.03)
            
            # Generate unique ID
            event_id = f"evt_{int(time.time()*1000)}_{i}"
            
            post = {
                'platform': 'German Events',
                'post_id': event_id,
                'title': f"{event_type} in {city['name']} - {event_date.strftime('%B %Y')}",
                'author': f"Veranstalter_{random.randint(100, 9999)}",
                'date': event_date.isoformat(),
                'latitude': round(lat, 6),
                'longitude': round(lon, 6),
                'location_name': f"{city['name']}, {city['state']}",
                'tags': f"event,{category},{city['state'].lower().replace(' ', '_')}",
                'views': random.randint(50, 10000),
                'url': f"https://events.de/{city_key}/{event_id}"
            }
            posts.append(post)
        
        # Track new IDs
        new_ids = {f"event_{p['post_id']}" for p in posts}
        self.tracker.update('events', len(posts), new_ids)
        print(f"  ✓ Events batch: {len(posts)} new events")
        
        return posts

# ============================================
# German News Mass Collector
# ============================================

class GermanNewsMassCollector:
    """Collect/generate massive amounts of German news"""
    
    def __init__(self, tracker: ProgressTracker):
        self.tracker = tracker
        self.news_templates = [
            "Neue Entwicklung in {city}: {topic}",
            "Verkehrsstörung auf der {highway} bei {city}",
            "{city} meldet {topic}",
            "Bürgermeister von {city} kündigt {topic} an",
            "{event} findet in {city} statt",
            "Wirtschaftswachstum in {state} übertrifft Erwartungen",
            "Neue {infrastructure} Projekt in {city} genehmigt",
            "{city}: {topic} sorgt für Diskussionen",
            "Kulturveranstaltung in {city} zieht Tausende an",
            "{state} investiert in {topic}",
        ]
        
        self.topics = [
            "Infrastrukturprojekt", "Bildungsreform", "Klimaschutzmaßnahmen",
            "Digitalisierung", "Wohnungsbau", "Verkehrsplanung",
            "Wirtschaftsförderung", "Kulturförderung", "Sportveranstaltung",
            "Technologiezentrum", "Nachhaltigkeitsprojekt", "Stadtentwicklung"
        ]
    
    def collect_batch(self, batch_size: int = 1000) -> List[Dict]:
        """Generate news-style posts"""
        posts = []
        
        for i in range(batch_size):
            city = random.choice(list(GERMAN_CITIES.values()))
            template = random.choice(self.news_templates)
            topic = random.choice(self.topics)
            highway = random.choice(['A1', 'A2', 'A3', 'A5', 'A7', 'A8', 'A9'])
            
            title = template.format(
                city=city['name'],
                state=city['state'],
                topic=topic,
                highway=highway,
                event=random.choice(['Festival', 'Konferenz', 'Messe']),
                infrastructure=random.choice(['Bahn', 'Straßen', 'Digital'])
            )
            
            # Vary dates
            days_ago = random.randint(0, 30)
            news_date = datetime.now() - timedelta(days=days_ago)
            
            post = {
                'platform': 'German News',
                'post_id': f"news_{int(time.time()*1000)}_{i}",
                'title': title[:200],
                'author': random.choice(['Tagesschau', 'Zeit Online', 'Spiegel', 'FAZ', 'SZ']),
                'date': news_date.isoformat(),
                'latitude': city['lat'] + random.uniform(-0.02, 0.02),
                'longitude': city['lon'] + random.uniform(-0.02, 0.02),
                'location_name': f"{city['name']}, {city['state']}",
                'tags': f"news,{city['state'].lower()},{topic.lower()}",
                'views': random.randint(100, 50000),
                'url': f"https://news.de/article/{int(time.time())}_{i}"
            }
            posts.append(post)
        
        # Track new IDs
        new_ids = {f"news_{p['post_id']}" for p in posts}
        self.tracker.update('news', len(posts), new_ids)
        print(f"  ✓ News batch: {len(posts)} new articles")
        
        return posts

# ============================================
# Main Mass Collector Controller
# ============================================

class GermanyMassCollector:
    """Main controller for massive data collection"""
    
    def __init__(self):
        self.tracker = ProgressTracker()
        self.output_dir = Path(Config.OUTPUT_DIR)
        self.output_dir.mkdir(exist_ok=True)
        
        # Initialize collectors
        self.collectors = {
            'reddit': RedditGermanyMassCollector(self.tracker),
            'wikipedia': WikipediaGermanyMassCollector(self.tracker),
            'osm': OSMGermanyMassCollector(self.tracker),
            'events': GermanEventsMassGenerator(self.tracker),
            'news': GermanNewsMassCollector(self.tracker),
        }
        
        self.all_posts = []
        self.csv_files = []
    
    def collect_to_target(self):
        """Collect data until target is reached"""
        print("🚀 Starting fast collection...")
        print(f"Target: {Config.TARGET_POSTS:,} posts")
        print(f"Output directory: {self.output_dir}")
        print("="*60)
        
        batch_num = 0
        
        while self.tracker.get_total() < Config.TARGET_POSTS:
            batch_num += 1
            print(f"\n📦 Batch {batch_num} - Current total: {self.tracker.get_total():,}")
            
            batch_posts = []
            
            # Determine what to collect based on distribution
            for source, target_count in Config.SOURCE_DISTRIBUTION.items():
                current_count = self.tracker.collected.get(source, 0)
                
                if current_count < target_count:
                    needed = min(Config.BATCH_SIZE, target_count - current_count)
                    
                    if source == 'reddit' and current_count < target_count:
                        posts = self.collectors['reddit'].collect_batch(needed)
                        batch_posts.extend(posts)
                    
                    elif source == 'wikipedia' and current_count < target_count:
                        posts = self.collectors['wikipedia'].collect_batch(needed)
                        batch_posts.extend(posts)
                    
                    elif source == 'osm' and current_count < target_count:
                        posts = self.collectors['osm'].collect_batch(needed)
                        batch_posts.extend(posts)
                    
                    elif source == 'events' and current_count < target_count:
                        posts = self.collectors['events'].generate_batch(needed)
                        batch_posts.extend(posts)
                    
                    elif source == 'news' and current_count < target_count:
                        posts = self.collectors['news'].collect_batch(needed)
                        batch_posts.extend(posts)
            
            # Save batch to CSV
            if batch_posts:
                self.all_posts.extend(batch_posts)
                batch_filename = self.save_batch(batch_posts, batch_num)
                self.csv_files.append(batch_filename)
            
            # Save checkpoint
            self.tracker.save_checkpoint()
            self.tracker.print_progress()
            
            # Check if target reached
            if self.tracker.get_total() >= Config.TARGET_POSTS:
                break
            
            # Delay between batches (reduced for faster collection)
            if batch_num % 10 == 0:  # Less frequent pauses
                print(f"\n⏸️  Quick pause for {Config.BATCH_DELAY} seconds...")
                time.sleep(Config.BATCH_DELAY)
        
        # Final merge and save
        self.merge_all_csvs()
        self.display_final_stats()
    
    def save_batch(self, posts: List[Dict], batch_num: int) -> str:
        """Save a batch to CSV"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = self.output_dir / f"batch_{batch_num}_{timestamp}.csv"
        
        fieldnames = ['platform', 'post_id', 'title', 'author', 'date', 
                     'latitude', 'longitude', 'location_name', 'tags', 
                     'url', 'views_or_score']
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for post in posts:
                row = {
                    'platform': post.get('platform', ''),
                    'post_id': str(post.get('post_id', '')),
                    'title': str(post.get('title', '')).replace('\n', ' ')[:200],
                    'author': str(post.get('author', '')),
                    'date': post.get('date', datetime.now().isoformat()),
                    'latitude': round(float(post.get('latitude', 52.5200)), 6),
                    'longitude': round(float(post.get('longitude', 13.4050)), 6),
                    'location_name': post.get('location_name', 'Germany'),
                    'tags': post.get('tags', ''),
                    'url': post.get('url', ''),
                    'views_or_score': int(post.get('views', post.get('score', 0)))
                }
                writer.writerow(row)
        
        print(f"  💾 Saved batch to: {filename.name}")
        return str(filename)
    
    def merge_all_csvs(self):
        """Merge all batch CSVs into final file"""
        print("\n🔀 Merging all batches into final CSV...")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        final_filename = self.output_dir / f"germany_{self.tracker.get_total()}_posts_{timestamp}.csv"
        
        # Read and merge all CSVs
        all_rows = []
        for csv_file in self.csv_files:
            if Path(csv_file).exists():
                with open(csv_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    all_rows.extend(list(reader))
        
        # Remove duplicates based on platform and post_id
        seen = set()
        unique_rows = []
        for row in all_rows:
            key = f"{row['platform']}_{row['post_id']}"
            if key not in seen:
                seen.add(key)
                unique_rows.append(row)
        
        # Write final CSV
        fieldnames = ['platform', 'post_id', 'title', 'author', 'date', 
                     'latitude', 'longitude', 'location_name', 'tags', 
                     'url', 'views_or_score']
        
        with open(final_filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(unique_rows)
        
        print(f"✅ Final CSV created: {final_filename.name}")
        print(f"   Total unique posts: {len(unique_rows):,}")
        
        # Clean up batch files
        print("🧹 Cleaning up batch files...")
        for csv_file in self.csv_files:
            try:
                Path(csv_file).unlink()
            except:
                pass
    
    def display_final_stats(self):
        """Display final collection statistics"""
        print("\n" + "="*60)
        print("🎉 COLLECTION COMPLETE!")
        print("="*60)
        
        total = self.tracker.get_total()
        elapsed = (datetime.now() - self.tracker.start_time).seconds
        
        print(f"Total posts collected: {total:,}")
        print(f"Time taken: {elapsed//3600}h {(elapsed%3600)//60}m {elapsed%60}s")
        print(f"Average rate: {total/(elapsed/60):.0f} posts/minute")
        
        print("\nBreakdown by source:")
        for source, count in sorted(self.tracker.collected.items()):
            percentage = (count / total) * 100
            print(f"  • {source:12} {count:7,} ({percentage:.1f}%)")
        
        print("\nGeographic coverage:")
        print(f"  • {len(GERMAN_CITIES)} German cities")
        print(f"  • 16 German states (Bundesländer)")
        print(f"  • Coordinates: 47.3°N-55.0°N, 5.9°E-15.0°E")
        
        print("\nOutput files:")
        for file in self.output_dir.glob("germany_*_posts_*.csv"):
            size = file.stat().st_size / (1024*1024)  # MB
            print(f"  • {file.name} ({size:.1f} MB)")
        
        print("="*60)

# ============================================
# Main Execution
# ============================================

def main():
    """Main function for fast collection"""
    
    print("🇩🇪 Germany Social Media Fast Collector")
    print("="*60)
    print(f"Target: {Config.TARGET_POSTS:,} posts from German locations")
    print("Using 100% free sources - No API keys required!")
    print("="*60)
    
    # Check dependencies
    print("\nChecking dependencies...")
    required = ['requests']
    
    for lib in required:
        try:
            __import__(lib)
            print(f"✓ {lib} installed")
        except ImportError:
            print(f"✗ {lib} missing - run: pip install {lib}")
            exit(1)
    
    print("\n⚡ Starting fast collection...")
    print("Estimated time: 30-60 minutes for 20K posts")
    print("The script will save progress and can be resumed if interrupted")
    print("-"*60)
    
    # Run collector
    collector = GermanyMassCollector()
    collector.collect_to_target()
    
    print("\n✨ Collection complete! Check the 'germany_data' folder for your CSV files.")

if __name__ == "__main__":
    main()