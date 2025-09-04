#!/usr/bin/env python3
"""
PSA API Scraper - Simplified version
Scrapes PSA auction price data and loads to BigQuery
"""

import requests
import time
import json
import pandas as pd
from datetime import datetime
import logging
import os
import sys
from pathlib import Path
from typing import List, Dict, Optional
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import urllib3
from dotenv import load_dotenv

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class PSAScraper:
    def __init__(self):
        self.base_url = "https://www.psacard.com/api/psa/auctionprices/spec"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Referer': 'https://www.psacard.com/auctionprices'
        })
        self.session.verify = False
        
        # Create temp directory for JSON caching
        self.temp_dir = self.create_temp_directory()
        logger.info(f"📁 Using cache directory: {self.temp_dir}")
        
        # Load card list
        self.cards = pd.read_csv('psa_card_list.csv')
        
        # PSA Grades
        self.grades = [
            {"value": "10", "label": "PSA 10"},
            {"value": "9", "label": "PSA 9"}, 
            {"value": "8.5", "label": "PSA 8.5"},
            {"value": "8", "label": "PSA 8"},
            {"value": "7.5", "label": "PSA 7.5"},
            {"value": "7", "label": "PSA 7"},
            {"value": "6.5", "label": "PSA 6.5"},
            {"value": "6", "label": "PSA 6"},
            {"value": "5.5", "label": "PSA 5.5"},
            {"value": "5", "label": "PSA 5"},
            {"value": "4.5", "label": "PSA 4.5"},
            {"value": "4", "label": "PSA 4"},
            {"value": "3.5", "label": "PSA 3.5"},
            {"value": "3", "label": "PSA 3"},
            {"value": "2.5", "label": "PSA 2.5"},
            {"value": "2", "label": "PSA 2"},
            {"value": "1.5", "label": "PSA 1.5"},
            {"value": "1", "label": "PSA 1"},
            {"value": "0", "label": "Auth"}
        ]
        
        # BigQuery setup
        self.project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'rising-environs-456314-a3')
        self.dataset_id = os.getenv('BIGQUERY_DATASET', 'tcg_data')
        self.table_id = 'psa_auction_prices'
        self.client = bigquery.Client(project=self.project_id)
    
    def create_temp_directory(self) -> Path:
        """Create temp directory with YYMMDD format"""
        date_str = datetime.now().strftime("%y%m%d")
        temp_dir = Path(f"temp_{date_str}")
        temp_dir.mkdir(exist_ok=True)
        return temp_dir
    
    def fetch_data(self, item_id: str, grade: str) -> tuple[Optional[Dict], bool]:
        """Fetch auction data from PSA API or load from cache
        Returns: (data, from_cache) - data is the JSON response, from_cache is True if loaded from cache
        """
        # Create filename for JSON cache
        json_filename = self.temp_dir / f"{item_id}_{grade}.json"
        
        # Check if JSON file already exists
        if json_filename.exists():
            logger.info(f"📂 Loading cached data: {json_filename.name}")
            try:
                with open(json_filename, 'r') as f:
                    data = json.load(f)
                # Check if it's empty (404 case)
                if not data:
                    logger.info(f"⚠️ Cached empty result for ID {item_id}, Grade {grade}")
                    return None, True
                logger.info(f"✅ Cached ID {item_id}, Grade {grade}: {len(data.get('highestDailySales', []))} sales")
                return data, True
            except Exception as e:
                logger.error(f"❌ Failed to load cache {json_filename}: {e}")
                # Continue to fetch from API if cache load fails
        
        # Fetch from API if not cached
        url = f"{self.base_url}/{item_id}/chartData"
        params = {'g': grade, 'time_range': 0}
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                
                # Save response to JSON file
                try:
                    with open(json_filename, 'w') as f:
                        json.dump(data, f, indent=2)
                    logger.info(f"💾 Saved response to: {json_filename.name}")
                except Exception as e:
                    logger.error(f"⚠️ Failed to save cache {json_filename}: {e}")
                
                logger.info(f"✅ API ID {item_id}, Grade {grade}: {len(data.get('highestDailySales', []))} sales")
                return data, False
            elif response.status_code == 404:
                logger.warning(f"⚠️ No data for ID {item_id}, Grade {grade}")
                # Save empty result to avoid re-fetching
                try:
                    with open(json_filename, 'w') as f:
                        json.dump({}, f)
                except:
                    pass
                return None, False
        except Exception as e:
            logger.error(f"❌ Failed API call ID {item_id}, Grade {grade}: {e}")
            return None, False
    
    def process_data(self, raw_data: Dict, item_id: str, grade: str, card_name: str) -> List[Dict]:
        """Process API response into structured records"""
        if not raw_data:
            return []
        
        records = []
        timestamp = datetime.now().isoformat()
        grade_label = next((g['label'] for g in self.grades if g['value'] == grade), f'Grade {grade}')
        
        # Summary record
        summary = raw_data.get('historicalItemAuctionSummary', {})
        if summary:
            records.append({
                'item_id': item_id,
                'card_name': card_name,
                'grade': grade,
                'grade_label': grade_label,
                'record_type': 'summary',
                'total_sales_count': summary.get('numberOfSales', 0),
                'average_price': summary.get('averagePrice'),
                'median_price': summary.get('medianPrice'),
                'min_price': summary.get('minPrice'),
                'max_price': summary.get('maxPrice'),
                'sale_date': None,
                'sale_price': None,
                'scraped_at': timestamp,
                'data_source': 'psa_api'
            })
        
        # Individual sales
        sales = raw_data.get('historicalAuctionInfo', {}).get('highestDailySales', [])
        for sale in sales:
            records.append({
                'item_id': item_id,
                'card_name': card_name,
                'grade': grade,
                'grade_label': grade_label,
                'record_type': 'sale',
                'total_sales_count': None,
                'average_price': None,
                'median_price': None,
                'min_price': None,
                'max_price': None,
                'sale_date': sale.get('dateOfSale'),
                'sale_price': sale.get('price'),
                'scraped_at': timestamp,
                'data_source': 'psa_api'
            })
        
        return records
    
    def create_table(self):
        """Create BigQuery table if it doesn't exist"""
        table_ref = self.client.dataset(self.dataset_id).table(self.table_id)
        
        try:
            self.client.get_table(table_ref)
            logger.info(f"Table {self.dataset_id}.{self.table_id} exists")
            return True
        except NotFound:
            schema = [
                bigquery.SchemaField("item_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("card_name", "STRING"),
                bigquery.SchemaField("grade", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("grade_label", "STRING"),
                bigquery.SchemaField("record_type", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("total_sales_count", "INTEGER"),
                bigquery.SchemaField("average_price", "FLOAT"),
                bigquery.SchemaField("median_price", "FLOAT"),
                bigquery.SchemaField("min_price", "FLOAT"),
                bigquery.SchemaField("max_price", "FLOAT"),
                bigquery.SchemaField("sale_date", "STRING"),
                bigquery.SchemaField("sale_price", "FLOAT"),
                bigquery.SchemaField("scraped_at", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("data_source", "STRING", mode="REQUIRED"),
            ]
            
            table = bigquery.Table(table_ref, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="scraped_at"
            )
            
            self.client.create_table(table)
            logger.info(f"✅ Created table {self.dataset_id}.{self.table_id}")
            return True
    
    def load_to_bigquery(self, data: List[Dict]):
        """Load data to BigQuery"""
        if not data:
            return
        
        self.create_table()
        table_ref = self.client.dataset(self.dataset_id).table(self.table_id)
        
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.ignore_unknown_values = True
        
        job = self.client.load_table_from_json(data, table_ref, job_config=job_config)
        job.result()
        logger.info(f"✅ Loaded {len(data)} records to BigQuery")
    
    def scrape_all(self, test_mode=False):
        """Main scraping function"""
        cards_to_scrape = self.cards.head(1) if test_mode else self.cards
        grades_to_scrape = self.grades[:3] if test_mode else self.grades
        
        logger.info(f"🚀 Starting scrape: {len(cards_to_scrape)} cards, {len(grades_to_scrape)} grades")
        
        all_data = []
        total_combinations = len(cards_to_scrape) * len(grades_to_scrape)
        current = 0
        cache_hits = 0
        api_calls = 0
        
        last_api_call = False  # Track if last request was an API call
        
        for _, card in cards_to_scrape.iterrows():
            card_id = str(card['card_id'])
            card_name = card['card_name']
            
            for grade in grades_to_scrape:
                current += 1
                logger.info(f"📊 [{current}/{total_combinations}] {card_name} - {grade['label']}")
                
                # Fetch and process
                raw_data, from_cache = self.fetch_data(card_id, grade['value'])
                
                # Track cache vs API statistics
                if from_cache:
                    cache_hits += 1
                else:
                    api_calls += 1
                
                # Rate limiting - only wait if last request was an API call (not from cache)
                if not from_cache and last_api_call:
                    logger.info("⏱️ Waiting 30 seconds to avoid rate limiting...")
                    time.sleep(30)
                
                # Update API call tracker
                last_api_call = not from_cache
                
                if raw_data:
                    records = self.process_data(raw_data, card_id, grade['value'], card_name)
                    all_data.extend(records)
                    
                    # Batch upload every 100 records
                    if len(all_data) >= 100:
                        self.load_to_bigquery(all_data)
                        all_data = []
        
        # Upload remaining data
        if all_data:
            self.load_to_bigquery(all_data)
        
        # Display cache statistics
        logger.info(f"📊 Cache Statistics:")
        logger.info(f"  - Cache hits: {cache_hits}")
        logger.info(f"  - API calls: {api_calls}")
        logger.info(f"  - Cache hit rate: {cache_hits/total_combinations*100:.1f}%")
        if api_calls > 0:
            logger.info(f"  - Estimated time saved: {cache_hits * 30} seconds")
        
        logger.info("🎯 Scraping complete!")

def main():
    scraper = PSAScraper()
    
    # Run in test mode (1 card, 3 grades) or full mode
    test_mode = len(sys.argv) > 1 and sys.argv[1] == 'test'
    scraper.scrape_all(test_mode=test_mode)

if __name__ == "__main__":
    main()