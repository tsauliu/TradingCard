#!/usr/bin/env python3
"""
PSA Production API Scraper with BigQuery Integration
Enhanced version with duplicate prevention, rate limiting, and production features
"""

import requests
import time
import json
import pandas as pd
from datetime import datetime, date
import logging
import os
import sys
from typing import List, Dict, Any, Optional, Set, Tuple
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import traceback
import csv

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('psa_production_scraper.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class PSAProductionScraper:
    """Enhanced PSA API scraper with production features"""
    
    def __init__(self, dry_run: bool = False):
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
        
        # PSA Grades mapping (all 19 grades)
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
        self.dry_run = dry_run
        
        # Tracking
        self.processed_combinations: Set[Tuple[str, str]] = set()
        self.existing_combinations: Set[Tuple[str, str]] = set()
        self.total_records_processed = 0
        self.total_api_calls = 0
        self.start_time = datetime.now()
        
        # Initialize BigQuery client
        if not dry_run:
            try:
                self.client = bigquery.Client(project=self.project_id)
                logger.info(f"BigQuery client initialized for project: {self.project_id}")
            except Exception as e:
                logger.error(f"Failed to initialize BigQuery client: {e}")
        else:
            logger.info("DRY RUN MODE - BigQuery client not initialized")
            
    def load_existing_combinations(self) -> None:
        """Load existing item_id/grade combinations from BigQuery to prevent duplicates"""
        
        if self.dry_run or not self.client:
            logger.info("Skipping duplicate check (dry run or no client)")
            return
            
        try:
            # Check if table exists
            table_ref = self.client.dataset(self.dataset_id).table(self.table_id)
            try:
                self.client.get_table(table_ref)
            except NotFound:
                logger.info("BigQuery table doesn't exist yet - no existing combinations to load")
                return
            
            # Query for existing combinations scraped today
            today = date.today().isoformat()
            query = f"""
            SELECT DISTINCT item_id, grade
            FROM `{self.project_id}.{self.dataset_id}.{self.table_id}`
            WHERE DATE(scraped_at) = '{today}'
            AND record_type = 'summary'
            """
            
            logger.info("Checking for existing combinations scraped today...")
            result = self.client.query(query)
            
            for row in result:
                self.existing_combinations.add((row.item_id, row.grade))
            
            logger.info(f"Found {len(self.existing_combinations)} existing combinations for today")
            
        except Exception as e:
            logger.error(f"Failed to load existing combinations: {e}")
            # Continue anyway - better to have duplicates than miss data

    def load_top_cards(self, limit: int = 10) -> List[Dict[str, str]]:
        """Load top N cards from psa_card_list.csv"""
        
        cards = []
        try:
            with open('psa_card_list.csv', 'r') as file:
                reader = csv.DictReader(file)
                for i, row in enumerate(reader):
                    if i >= limit:
                        break
                    cards.append({
                        'name': row['card_name'],
                        'id': row['card_id'],
                        'url': row['url']
                    })
            
            logger.info(f"Loaded {len(cards)} cards for processing")
            return cards
            
        except Exception as e:
            logger.error(f"Failed to load card list: {e}")
            return []

    def is_already_processed(self, item_id: str, grade: str) -> bool:
        """Check if item_id/grade combination was already processed"""
        
        combination = (item_id, grade)
        
        # Check if already processed in this session
        if combination in self.processed_combinations:
            return True
            
        # Check if exists in BigQuery from today
        if combination in self.existing_combinations:
            return True
            
        return False

    def fetch_auction_data(self, item_id: str, grade: str, max_retries: int = 5) -> Optional[Dict]:
        """Fetch auction data with enhanced error handling and retries"""
        
        url = f"{self.base_url}/{item_id}/chartData"
        params = {
            'g': grade,
            'time_range': 0  # All time data
        }
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Fetching data for ID {item_id}, Grade {grade} (attempt {attempt + 1}/{max_retries})")
                
                if self.dry_run:
                    logger.info("DRY RUN - Simulating API call")
                    time.sleep(2)  # Simulate network delay
                    return {"simulated": True, "item_id": item_id, "grade": grade}
                
                response = self.session.get(url, params=params, timeout=30)
                self.total_api_calls += 1
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        if data is None:
                            logger.warning(f"⚠️ API returned null data for ID {item_id}, Grade {grade}")
                            return None
                        
                        # Safely extract sales count
                        historical_info = data.get('historicalAuctionInfo') if isinstance(data, dict) else {}
                        daily_sales = historical_info.get('highestDailySales') if isinstance(historical_info, dict) else []
                        sales_count = len(daily_sales) if isinstance(daily_sales, list) else 0
                        
                        logger.info(f"✅ Success: ID {item_id}, Grade {grade} - {sales_count} sales")
                        return data
                    except (json.JSONDecodeError, ValueError) as e:
                        logger.error(f"❌ Invalid JSON response for ID {item_id}, Grade {grade}: {e}")
                        return None
                elif response.status_code == 404:
                    logger.warning(f"⚠️ No data found for ID {item_id}, Grade {grade} (404)")
                    return None
                else:
                    logger.warning(f"⚠️ HTTP {response.status_code} for ID {item_id}, Grade {grade}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ Request failed for ID {item_id}, Grade {grade}: {e}")
                
                # Exponential backoff for retries
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + 5  # 5, 7, 11, 19 seconds
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    
        logger.error(f"❌ Failed to fetch data for ID {item_id}, Grade {grade} after {max_retries} attempts")
        return None

    def process_auction_data(self, raw_data: Dict, item_id: str, grade: str) -> List[Dict]:
        """Process raw API response into structured data with validation"""
        
        processed_records = []
        
        if not raw_data:
            return processed_records
        
        # Handle dry run simulation
        if raw_data.get("simulated"):
            return [{
                'item_id': item_id,
                'grade': grade,
                'grade_label': f'Grade {grade}',
                'record_type': 'summary',
                'total_sales_count': 100,  # Simulated
                'scraped_at': datetime.now().isoformat(),
                'data_source': 'psa_api_simulation'
            }]
        
        # Extract data with validation
        historical_info = raw_data.get('historicalAuctionInfo', {})
        historical_summary = raw_data.get('historicalItemAuctionSummary', {})
        sales_data = historical_info.get('highestDailySales', [])
        
        # Validate essential data
        if not isinstance(sales_data, list):
            logger.warning(f"Invalid sales data format for {item_id}, grade {grade}")
            return processed_records
        
        # Create summary record
        summary_record = {
            'item_id': str(item_id),
            'grade': str(grade),
            'grade_label': next((g['label'] for g in self.grades if g['value'] == grade), f'Grade {grade}'),
            'record_type': 'summary',
            'total_sales_count': historical_summary.get('numberOfSales', 0),
            'average_price': self._safe_float(historical_summary.get('averagePrice')),
            'median_price': self._safe_float(historical_summary.get('medianPrice')), 
            'min_price': self._safe_float(historical_summary.get('minPrice')),
            'max_price': self._safe_float(historical_summary.get('maxPrice')),
            'std_deviation': self._safe_float(historical_summary.get('stdDeviation')),
            'date_range_start': historical_info.get('startDate'),
            'date_range_end': historical_info.get('endDate'),
            'sale_date': None,
            'sale_price': None,
            'scraped_at': datetime.now().isoformat(),
            'data_source': 'psa_api'
        }
        processed_records.append(summary_record)
        
        # Create individual sale records (limit to prevent excessive data)
        max_sales = min(len(sales_data), 1000)  # Limit to 1000 sales per grade
        for sale in sales_data[:max_sales]:
            if not isinstance(sale, dict):
                continue
                
            sale_record = {
                'item_id': str(item_id),
                'grade': str(grade),
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
                'sale_price': self._safe_float(sale.get('price')),
                'scraped_at': datetime.now().isoformat(),
                'data_source': 'psa_api'
            }
            processed_records.append(sale_record)
        
        if max_sales < len(sales_data):
            logger.info(f"Limited sales records to {max_sales} out of {len(sales_data)} for {item_id}, grade {grade}")
        
        logger.info(f"Processed {len(processed_records)} records for ID {item_id}, Grade {grade}")
        return processed_records

    def _safe_float(self, value) -> Optional[float]:
        """Safely convert value to float"""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def create_bigquery_table(self):
        """Create BigQuery table with proper schema"""
        
        if self.dry_run or not self.client:
            logger.info("Skipping table creation (dry run or no client)")
            return True
            
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
            bigquery.SchemaField("record_type", "STRING", mode="REQUIRED"),
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
        
        if self.dry_run:
            logger.info(f"DRY RUN - Would load {len(data)} records to BigQuery")
            return True
            
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

    def save_checkpoint(self, data: List[Dict], item_name: str):
        """Save checkpoint data to CSV"""
        
        if not data:
            return
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"checkpoint_{item_name}_{timestamp}.csv"
        
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False)
        logger.info(f"💾 Checkpoint saved: {filename} ({len(data)} records)")

    def scrape_card_all_grades(self, card: Dict[str, str], delay: int = 35) -> List[Dict]:
        """Scrape auction data for all grades of a specific card"""
        
        item_id = card['id']
        card_name = card['name']
        
        logger.info(f"🚀 Starting scrape for card: {card_name} (ID: {item_id})")
        all_data = []
        
        for i, grade in enumerate(self.grades):
            grade_value = grade['value']
            grade_label = grade['label']
            
            # Check if already processed
            if self.is_already_processed(item_id, grade_value):
                logger.info(f"⏭️ Skipping {grade_label} - already processed")
                continue
            
            logger.info(f"📊 Processing {grade_label} ({i+1}/{len(self.grades)})")
            
            # Add delay between requests (except for first request)
            if i > 0:
                logger.info(f"⏱️ Rate limiting: waiting {delay} seconds...")
                time.sleep(delay)
            
            # Fetch data
            raw_data = self.fetch_auction_data(item_id, grade_value)
            
            if raw_data:
                # Process data
                processed_data = self.process_auction_data(raw_data, item_id, grade_value)
                all_data.extend(processed_data)
                
                # Mark as processed
                self.processed_combinations.add((item_id, grade_value))
                self.total_records_processed += len(processed_data)
            else:
                logger.warning(f"No data found for {grade_label}")
        
        logger.info(f"🎯 Completed scraping for {card_name}: {len(all_data)} total records")
        return all_data

    def run_production_scrape(self):
        """Run production scrape for top 10 cards"""
        
        logger.info("🏭 Starting PRODUCTION PSA scrape")
        logger.info(f"Mode: {'DRY RUN' if self.dry_run else 'LIVE RUN'}")
        
        # Load existing combinations to prevent duplicates
        self.load_existing_combinations()
        
        # Load top 10 cards
        cards = self.load_top_cards(10)
        if not cards:
            logger.error("Failed to load card list")
            return
        
        total_combinations = len(cards) * len(self.grades)
        logger.info(f"📊 Processing {len(cards)} cards × {len(self.grades)} grades = {total_combinations} combinations")
        
        all_scraped_data = []
        
        for card_idx, card in enumerate(cards):
            card_name = card['name']
            
            logger.info(f"\n{'='*60}")
            logger.info(f"🎴 Processing card {card_idx + 1}/{len(cards)}: {card_name}")
            logger.info(f"{'='*60}")
            
            # Scrape all grades for this card
            card_data = self.scrape_card_all_grades(card)
            
            if card_data:
                all_scraped_data.extend(card_data)
                
                # Checkpoint save every 2 cards
                if (card_idx + 1) % 2 == 0:
                    self.save_checkpoint(all_scraped_data, f"cards_1_to_{card_idx + 1}")
                    
                    # Load to BigQuery
                    if self.load_to_bigquery(all_scraped_data):
                        logger.info(f"✅ Checkpoint {card_idx + 1}: Loaded {len(all_scraped_data)} records to BigQuery")
                        all_scraped_data = []  # Reset for next batch
                    else:
                        logger.error(f"❌ Checkpoint {card_idx + 1}: Failed to load to BigQuery")
            
            # Calculate ETA
            elapsed = datetime.now() - self.start_time
            remaining_cards = len(cards) - (card_idx + 1)
            if card_idx > 0:
                avg_time_per_card = elapsed.total_seconds() / (card_idx + 1)
                eta_seconds = avg_time_per_card * remaining_cards
                eta = datetime.now().timestamp() + eta_seconds
                eta_formatted = datetime.fromtimestamp(eta).strftime("%H:%M:%S")
                logger.info(f"📈 Progress: {card_idx + 1}/{len(cards)} cards | ETA: {eta_formatted}")
        
        # Load any remaining data
        if all_scraped_data:
            self.load_to_bigquery(all_scraped_data)
        
        # Final summary
        self._print_summary()

    def _print_summary(self):
        """Print final summary report"""
        
        elapsed = datetime.now() - self.start_time
        
        logger.info(f"\n{'='*60}")
        logger.info("📊 SCRAPING SUMMARY REPORT")
        logger.info(f"{'='*60}")
        logger.info(f"⏱️ Total runtime: {elapsed}")
        logger.info(f"🌐 Total API calls: {self.total_api_calls}")
        logger.info(f"📄 Total records processed: {self.total_records_processed}")
        logger.info(f"⏭️ Combinations skipped (duplicates): {len(self.existing_combinations)}")
        logger.info(f"✅ Combinations processed this session: {len(self.processed_combinations)}")
        logger.info(f"📊 Average time per API call: {elapsed.total_seconds() / max(self.total_api_calls, 1):.1f}s")
        logger.info(f"💾 Data destination: {'DRY RUN (no data saved)' if self.dry_run else f'{self.project_id}.{self.dataset_id}.{self.table_id}'}")
        logger.info(f"{'='*60}")

def main():
    """Main function"""
    
    import argparse
    parser = argparse.ArgumentParser(description='PSA Production Scraper')
    parser.add_argument('--dry-run', action='store_true', help='Run in dry-run mode (no API calls or BigQuery writes)')
    args = parser.parse_args()
    
    scraper = PSAProductionScraper(dry_run=args.dry_run)
    scraper.run_production_scrape()

if __name__ == "__main__":
    main()