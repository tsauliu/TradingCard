#!/usr/bin/env python3
"""PSA API Scraper - Simplified version with caching"""

import requests
import time
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
import os
import sys
from google.cloud import bigquery
from dotenv import load_dotenv
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

class PSAScraper:
    def __init__(self):
        # API setup
        self.base_url = "https://www.psacard.com/api/psa/auctionprices/spec"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Referer': 'https://www.psacard.com/auctionprices'
        })
        self.session.verify = False
        
        # Cache directory
        self.cache_dir = Path(f"temp_{datetime.now().strftime('%y%m%d')}")
        self.cache_dir.mkdir(exist_ok=True)
        
        # Data files
        self.cards = pd.read_csv('psa_card_list.csv')
        self.grades = [
            "10", "9", "8.5", "8", "7.5", "7", "6.5", "6", "5.5", "5",
            "4.5", "4", "3.5", "3", "2.5", "2", "1.5", "1", "0"
        ]
        
        # BigQuery
        self.bq_client = bigquery.Client(project=os.getenv('GOOGLE_CLOUD_PROJECT'))
        self.table_id = f"{os.getenv('BIGQUERY_DATASET')}.psa_auction_prices"
    
    def fetch(self, item_id, grade):
        """Fetch from cache or API"""
        cache_file = self.cache_dir / f"{item_id}_{grade}.json"
        
        # Try cache first
        if cache_file.exists():
            with open(cache_file) as f:
                data = json.load(f)
                return data if data else None, True  # True = from cache
        
        # Fetch from API
        try:
            resp = self.session.get(f"{self.base_url}/{item_id}/chartData", 
                                   params={'g': grade, 'time_range': 0}, timeout=30)
            data = resp.json() if resp.status_code == 200 else {}
            with open(cache_file, 'w') as f:
                json.dump(data, f, indent=2)
            return data if data else None, False  # False = from API
        except:
            return None, False
    
    def process(self, data, item_id, grade, card_name):
        """Convert API data to records"""
        if not data:
            return []
        
        records = []
        timestamp = datetime.now().isoformat()
        
        # Summary record
        summary = data.get('historicalItemAuctionSummary', {})
        if summary:
            records.append({
                'item_id': item_id,
                'card_name': card_name,
                'grade': grade,
                'record_type': 'summary',
                'total_sales_count': summary.get('numberOfSales'),
                'average_price': summary.get('averagePrice'),
                'median_price': summary.get('medianPrice'),
                'min_price': summary.get('minPrice'),
                'max_price': summary.get('maxPrice'),
                'sale_date': None,
                'sale_price': None,
                'scraped_at': timestamp
            })
        
        # Individual sales
        for sale in data.get('historicalAuctionInfo', {}).get('highestDailySales', []):
            records.append({
                'item_id': item_id,
                'card_name': card_name,
                'grade': grade,
                'record_type': 'sale',
                'sale_date': sale.get('dateOfSale'),
                'sale_price': sale.get('price'),
                'scraped_at': timestamp
            })
        
        return records
    
    def upload(self, records):
        """Upload to BigQuery"""
        if not records:
            return
        
        # Create table if needed
        try:
            self.bq_client.get_table(self.table_id)
        except:
            schema = [
                bigquery.SchemaField("item_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("card_name", "STRING"),
                bigquery.SchemaField("grade", "STRING"),
                bigquery.SchemaField("record_type", "STRING"),
                bigquery.SchemaField("total_sales_count", "INTEGER"),
                bigquery.SchemaField("average_price", "FLOAT"),
                bigquery.SchemaField("median_price", "FLOAT"),
                bigquery.SchemaField("min_price", "FLOAT"),
                bigquery.SchemaField("max_price", "FLOAT"),
                bigquery.SchemaField("sale_date", "STRING"),
                bigquery.SchemaField("sale_price", "FLOAT"),
                bigquery.SchemaField("scraped_at", "TIMESTAMP"),
            ]
            table = bigquery.Table(self.table_id, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="scraped_at")
            self.bq_client.create_table(table)
        
        # Load data
        config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        self.bq_client.load_table_from_json(records, self.table_id, job_config=config).result()
    
    def run(self, test=False):
        """Main scraping function"""
        cards = self.cards.head(1) if test else self.cards
        grades = self.grades[:3] if test else self.grades
        
        all_records = []
        need_wait = False
        
        for _, card in cards.iterrows():
            card_id = str(card['card_id'])
            card_name = card['card_name']
            
            for grade in grades:
                # Wait between API calls
                if need_wait:
                    time.sleep(30)
                
                # Fetch and process
                data, from_cache = self.fetch(card_id, grade)
                need_wait = not from_cache  # Only wait after API calls
                
                if data:
                    all_records.extend(self.process(data, card_id, grade, card_name))
                    
                    # Batch upload
                    if len(all_records) >= 100:
                        self.upload(all_records)
                        all_records = []
        
        # Upload remaining
        self.upload(all_records)
        print("Done!")

if __name__ == "__main__":
    PSAScraper().run(test='test' in sys.argv)