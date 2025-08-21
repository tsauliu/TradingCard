#!/usr/bin/env python3
"""
PSA API Scraper with BigQuery Integration

Scrapes PSA auction price data using their API endpoints for all grades
and loads the data into BigQuery for analysis.
"""

import requests
import time
import json
import pandas as pd
from datetime import datetime
import logging
import os
import sys
from typing import List, Dict, Any, Optional
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import traceback

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('psa_api_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class PSAAPIScraper:
    """PSA API scraper with BigQuery integration"""
    
    def __init__(self):
        self.base_url = "https://www.psacard.com/api/psa/auctionprices/spec"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Referer': 'https://www.psacard.com/auctionprices'
        })
        self.session.verify = False  # Disable SSL verification
        
        # Disable SSL warnings
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        # PSA Grades mapping
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
        self.client = None
        
        # Initialize BigQuery client
        try:
            self.client = bigquery.Client(project=self.project_id)
            logger.info(f"BigQuery client initialized for project: {self.project_id}")
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}")
            
    def fetch_auction_data(self, item_id: str, grade: str, max_retries: int = 3) -> Optional[Dict]:
        """Fetch auction data for a specific item ID and grade"""
        
        url = f"{self.base_url}/{item_id}/chartData"
        params = {
            'g': grade,
            'time_range': 0  # All time data
        }
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Fetching data for ID {item_id}, Grade {grade} (attempt {attempt + 1})")
                
                response = self.session.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    logger.info(f"✅ Success: ID {item_id}, Grade {grade} - {len(data.get('highestDailySales', []))} sales")
                    return data
                elif response.status_code == 404:
                    logger.warning(f"⚠️ No data found for ID {item_id}, Grade {grade} (404)")
                    return None
                else:
                    logger.warning(f"⚠️ HTTP {response.status_code} for ID {item_id}, Grade {grade}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ Request failed for ID {item_id}, Grade {grade}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)
                    
        logger.error(f"❌ Failed to fetch data for ID {item_id}, Grade {grade} after {max_retries} attempts")
        return None
    
    def process_auction_data(self, raw_data: Dict, item_id: str, grade: str) -> List[Dict]:
        """Process raw API response into structured data"""
        
        processed_records = []
        
        if not raw_data:
            return processed_records
        
        # Extract basic info
        historical_info = raw_data.get('historicalAuctionInfo', {})
        historical_summary = raw_data.get('historicalItemAuctionSummary', {})
        sales_data = historical_info.get('highestDailySales', [])
        
        # Create summary record
        summary_record = {
            'item_id': item_id,
            'grade': grade,
            'grade_label': next((g['label'] for g in self.grades if g['value'] == grade), f'Grade {grade}'),
            'record_type': 'summary',
            'total_sales_count': historical_summary.get('numberOfSales', 0),
            'average_price': historical_summary.get('averagePrice'),
            'median_price': historical_summary.get('medianPrice'), 
            'min_price': historical_summary.get('minPrice'),
            'max_price': historical_summary.get('maxPrice'),
            'std_deviation': historical_summary.get('stdDeviation'),
            'date_range_start': historical_info.get('startDate'),
            'date_range_end': historical_info.get('endDate'),
            'sale_date': None,
            'sale_price': None,
            'scraped_at': datetime.now().isoformat(),
            'data_source': 'psa_api'
        }
        processed_records.append(summary_record)
        
        # Create individual sale records
        for sale in sales_data:
            sale_record = {
                'item_id': item_id,
                'grade': grade,
                'grade_label': next((g['label'] for g in self.grades if g['value'] == grade), f'Grade {grade}'),
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
                'scraped_at': datetime.now().isoformat(),
                'data_source': 'psa_api'
            }
            processed_records.append(sale_record)
        
        logger.info(f"Processed {len(processed_records)} records for ID {item_id}, Grade {grade}")
        return processed_records
    
    def create_bigquery_table(self):
        """Create BigQuery table with proper schema"""
        
        if not self.client:
            logger.error("BigQuery client not initialized")
            return False
            
        table_ref = self.client.dataset(self.dataset_id).table(self.table_id)
        
        # Check if table exists
        try:
            self.client.get_table(table_ref)
            logger.info(f"Table {self.dataset_id}.{self.table_id} already exists")
            return True
        except NotFound:
            logger.info(f"Creating table {self.dataset_id}.{self.table_id}")
        
        # Define schema
        schema = [
            bigquery.SchemaField("item_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("grade", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("grade_label", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("record_type", "STRING", mode="REQUIRED"),  # 'summary' or 'sale'
            bigquery.SchemaField("total_sales_count", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("average_price", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("median_price", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("min_price", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("max_price", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("std_deviation", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("date_range_start", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("date_range_end", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("sale_date", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("sale_price", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("scraped_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("data_source", "STRING", mode="REQUIRED"),
        ]
        
        # Create table
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="scraped_at"
        )
        
        try:
            table = self.client.create_table(table)
            logger.info(f"✅ Created table {self.dataset_id}.{self.table_id}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to create table: {e}")
            return False
    
    def load_to_bigquery(self, data: List[Dict]) -> bool:
        """Load processed data to BigQuery"""
        
        if not self.client:
            logger.error("BigQuery client not initialized")
            return False
            
        if not data:
            logger.info("No data to load")
            return True
        
        # Create table if it doesn't exist
        if not self.create_bigquery_table():
            return False
        
        table_ref = self.client.dataset(self.dataset_id).table(self.table_id)
        
        # Configure load job
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.ignore_unknown_values = True
        
        try:
            # Convert to JSON string
            json_data = "\n".join([json.dumps(record) for record in data])
            
            # Load data
            job = self.client.load_table_from_json(
                data, table_ref, job_config=job_config
            )
            
            # Wait for job to complete
            job.result()
            
            logger.info(f"✅ Loaded {len(data)} records to BigQuery table {self.dataset_id}.{self.table_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to load data to BigQuery: {e}")
            logger.error(traceback.format_exc())
            return False
    
    def scrape_item_all_grades(self, item_id: str, delay: int = 10) -> List[Dict]:
        """Scrape auction data for all grades of a specific item"""
        
        logger.info(f"🚀 Starting scrape for item ID: {item_id}")
        all_data = []
        
        for i, grade in enumerate(self.grades):
            grade_value = grade['value']
            grade_label = grade['label']
            
            logger.info(f"📊 Processing {grade_label} ({i+1}/{len(self.grades)})")
            
            # Add delay between requests (except for first request)
            if i > 0:
                logger.info(f"⏱️ Waiting {delay} seconds...")
                time.sleep(delay)
            
            # Fetch data
            raw_data = self.fetch_auction_data(item_id, grade_value)
            
            if raw_data:
                # Process data
                processed_data = self.process_auction_data(raw_data, item_id, grade_value)
                all_data.extend(processed_data)
            else:
                logger.warning(f"No data found for {grade_label}")
        
        logger.info(f"🎯 Completed scraping for item {item_id}: {len(all_data)} total records")
        return all_data
    
    def save_to_csv(self, data: List[Dict], filename: str = None):
        """Save data to CSV file"""
        
        if not data:
            logger.warning("No data to save to CSV")
            return
        
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"psa_auction_data_{timestamp}.csv"
        
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False)
        logger.info(f"💾 Saved {len(data)} records to {filename}")
        
    def run_single_item_test(self, item_id: str = "544027"):
        """Run test with single item ID"""
        
        logger.info(f"🧪 Starting test run with item ID: {item_id}")
        
        try:
            # Test with just first 3 grades for now
            original_grades = self.grades
            self.grades = self.grades[:3]  # Just PSA 10, 9, 8.5
            
            # Scrape data for test grades
            data = self.scrape_item_all_grades(item_id, delay=5)
            
            # Restore original grades
            self.grades = original_grades
            
            if not data:
                logger.error("No data scraped!")
                return
            
            # Save to CSV
            self.save_to_csv(data, f"psa_test_{item_id}.csv")
            
            # Load to BigQuery
            if self.load_to_bigquery(data):
                logger.info("✅ Test completed successfully!")
            else:
                logger.error("❌ BigQuery load failed")
                
        except Exception as e:
            logger.error(f"❌ Test failed: {e}")
            logger.error(traceback.format_exc())

def main():
    """Main function"""
    
    scraper = PSAAPIScraper()
    
    # Test with ID 544027 (Charizard card)
    scraper.run_single_item_test("544027")

if __name__ == "__main__":
    main()