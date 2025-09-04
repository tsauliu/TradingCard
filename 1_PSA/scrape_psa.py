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
                return data if data else None
        
        # Fetch from API
        try:
            resp = self.session.get(f"{self.base_url}/{item_id}/chartData", 
                                   params={'g': grade, 'time_range': 0}, timeout=30)
            data = resp.json() if resp.status_code == 200 else {}
            with open(cache_file, 'w') as f:
                json.dump(data, f, indent=2)
            time.sleep(30)  # Rate limit after API call
            return data if data else None
        except:
            return None
    
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
                'grade_label': f'PSA {grade}',
                'record_type': 'summary',
                'total_sales_count': summary.get('numberOfSales'),
                'average_price': summary.get('averagePrice'),
                'median_price': summary.get('medianPrice'),
                'min_price': summary.get('minPrice'),
                'max_price': summary.get('maxPrice'),
                'std_deviation': None,
                'date_range_start': None,
                'date_range_end': None,
                'sale_date': None,
                'sale_price': None,
                'scraped_at': timestamp,
                'data_source': 'psa_api',
                'card_set': None,
                'card_year': None,
                'card_variant': None,
                'psa_url': None,
                'lifecycle_sales_count': summary.get('numberOfSales')
            })
        
        # Individual sales
        for sale in data.get('historicalAuctionInfo', {}).get('highestDailySales', []):
            records.append({
                'item_id': item_id,
                'card_name': card_name,
                'grade': grade,
                'grade_label': f'PSA {grade}',
                'record_type': 'sale',
                'total_sales_count': None,
                'average_price': None,
                'median_price': None,
                'min_price': None,
                'max_price': None,
                'std_deviation': None,
                'date_range_start': None,
                'date_range_end': None,
                'sale_date': sale.get('dateOfSale'),
                'sale_price': sale.get('price'),
                'scraped_at': timestamp,
                'data_source': 'psa_api',
                'card_set': None,
                'card_year': None,
                'card_variant': None,
                'psa_url': None,
                'lifecycle_sales_count': None
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
                bigquery.SchemaField("item_id", "STRING"),
                bigquery.SchemaField("grade", "STRING"),
                bigquery.SchemaField("grade_label", "STRING"),
                bigquery.SchemaField("record_type", "STRING"),
                bigquery.SchemaField("total_sales_count", "INTEGER"),
                bigquery.SchemaField("average_price", "FLOAT"),
                bigquery.SchemaField("median_price", "FLOAT"),
                bigquery.SchemaField("min_price", "FLOAT"),
                bigquery.SchemaField("max_price", "FLOAT"),
                bigquery.SchemaField("std_deviation", "FLOAT"),
                bigquery.SchemaField("date_range_start", "STRING"),
                bigquery.SchemaField("date_range_end", "STRING"),
                bigquery.SchemaField("sale_date", "STRING"),
                bigquery.SchemaField("sale_price", "FLOAT"),
                bigquery.SchemaField("scraped_at", "TIMESTAMP"),
                bigquery.SchemaField("data_source", "STRING"),
                bigquery.SchemaField("card_name", "STRING"),
                bigquery.SchemaField("card_set", "STRING"),
                bigquery.SchemaField("card_year", "INTEGER"),
                bigquery.SchemaField("card_variant", "STRING"),
                bigquery.SchemaField("psa_url", "STRING"),
                bigquery.SchemaField("lifecycle_sales_count", "INTEGER"),
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
        
        total_cards = len(cards)
        total_grades = len(grades)
        total_combinations = total_cards * total_grades
        
        print(f"Starting scraper: {total_cards} cards × {total_grades} grades = {total_combinations} API calls")
        print(f"Estimated runtime: ~{total_combinations * 30 / 60:.1f} minutes")
        
        all_records = []
        completed = 0
        
        for card_idx, (_, card) in enumerate(cards.iterrows(), 1):
            card_id = str(card['card_id'])
            card_name = card['card_name']
            print(f"\n[Card {card_idx}/{total_cards}] {card_name} (ID: {card_id})")
            
            for grade_idx, grade in enumerate(grades, 1):
                completed += 1
                progress = (completed / total_combinations) * 100
                print(f"  [{completed}/{total_combinations}] Grade {grade} - {progress:.1f}% complete", end="")
                
                # Fetch and process
                data = self.fetch(card_id, grade)
                
                if data:
                    records = self.process(data, card_id, grade, card_name)
                    all_records.extend(records)
                    print(f" - {len(records)} records extracted")
                    
                    # Batch upload
                    if len(all_records) >= 100:
                        print(f"  Uploading batch of {len(all_records)} records to BigQuery...")
                        self.upload(all_records)
                        all_records = []
                else:
                    print(" - No data")
        
        # Upload remaining
        if all_records:
            print(f"\nUploading final batch of {len(all_records)} records to BigQuery...")
            self.upload(all_records)
        
        print(f"\n{'='*50}")
        print(f"Scraping complete! Processed {total_combinations} combinations")
        print(f"{'='*50}")

if __name__ == "__main__":
    PSAScraper().run(test='test' in sys.argv)