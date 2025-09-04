#!/usr/bin/env python3
"""
TCG Data Processor - Analyzes JSON files and uploads to BigQuery
"""

import json
import os
import re
import logging
import sys
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime, timezone
import time

import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, WriteDisposition
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f'/logs/tcg_upload_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class TCGDataProcessor:
    """Main class for processing TCG data and uploading to BigQuery"""
    
    def __init__(self, project_id: str = None, dataset_id: str = "tcg_data", 
                 table_id: str = "tcg_prices_bda", json_directory: str = "./product_details"):
        """Initialize the processor with BigQuery configuration"""
        self.project_id = project_id or os.getenv("GOOGLE_CLOUD_PROJECT")
        self.dataset_id = dataset_id or os.getenv("BIGQUERY_DATASET", "tcg_data")
        self.table_id = table_id
        self.json_directory = json_directory
        
        # Validate configuration
        if not self.project_id:
            raise ValueError("GOOGLE_CLOUD_PROJECT environment variable not set")
        
        if not os.path.exists(self.json_directory):
            raise ValueError(f"Directory {self.json_directory} does not exist")
        
        logger.info(f"Configuration: Project={self.project_id}, Dataset={self.dataset_id}, Table={self.table_id}")
        logger.info(f"JSON Directory: {self.json_directory}")
        
        # Initialize BigQuery client
        self.client = bigquery.Client(project=self.project_id)
        self.table_ref = self.client.dataset(self.dataset_id).table(self.table_id)
        
        # Initialize or verify table
        self._setup_table()
    
    def _setup_table(self):
        """Create BigQuery table if it doesn't exist"""
        try:
            table = self.client.get_table(self.table_ref)
            logger.info(f"Table {self.table_id} exists with {table.num_rows:,} rows")
        except Exception:
            schema = self._get_table_schema()
            table = bigquery.Table(self.table_ref, schema=schema)
            table = self.client.create_table(table)
            logger.info(f"Created table {self.table_id}")
    
    @staticmethod
    def _get_table_schema():
        """Define BigQuery table schema"""
        return [
            bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("sku_id", "STRING"),
            bigquery.SchemaField("variant", "STRING"),
            bigquery.SchemaField("language", "STRING"),
            bigquery.SchemaField("condition", "STRING"),
            bigquery.SchemaField("average_daily_quantity_sold", "STRING"),
            bigquery.SchemaField("average_daily_transaction_count", "STRING"),
            bigquery.SchemaField("total_quantity_sold", "STRING"),
            bigquery.SchemaField("total_transaction_count", "STRING"),
            bigquery.SchemaField("bucket_start_date", "DATE"),
            bigquery.SchemaField("market_price", "FLOAT"),
            bigquery.SchemaField("quantity_sold", "INTEGER"),
            bigquery.SchemaField("low_sale_price", "FLOAT"),
            bigquery.SchemaField("low_sale_price_with_shipping", "FLOAT"),
            bigquery.SchemaField("high_sale_price", "FLOAT"),
            bigquery.SchemaField("high_sale_price_with_shipping", "FLOAT"),
            bigquery.SchemaField("transaction_count", "INTEGER"),
            bigquery.SchemaField("file_processed_at", "TIMESTAMP")
        ]
    
    @staticmethod
    def _extract_product_id(filename: str) -> str:
        """Extract product ID from filename (e.g., '481225.0.json' -> '481225')"""
        match = re.match(r'^(\d+)(?:\.0)?\.json$', filename)
        if match:
            return match.group(1)
        raise ValueError(f"Invalid filename format: {filename}")
    
    def _process_json_file(self, file_path: Path) -> List[Dict[str, Any]]:
        """Process a single JSON file and return flattened records"""
        product_id = self._extract_product_id(file_path.name)
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning(f"Invalid JSON file {file_path.name}: {e}")
            return []
        
        records = []
        results = data.get('result', [])
        
        for result in results:
            # Base product information
            base_info = {
                'product_id': product_id,
                'sku_id': result.get('skuId'),
                'variant': result.get('variant'),
                'language': result.get('language'),
                'condition': result.get('condition'),
                'average_daily_quantity_sold': result.get('averageDailyQuantitySold'),
                'average_daily_transaction_count': result.get('averageDailyTransactionCount'),
                'total_quantity_sold': result.get('totalQuantitySold'),
                'total_transaction_count': result.get('totalTransactionCount'),
                'file_processed_at': datetime.now(timezone.utc)
            }
            
            # Process price history buckets
            buckets = result.get('buckets', [])
            if not buckets:
                records.append(base_info)
            else:
                for bucket in buckets:
                    record = base_info.copy()
                    record.update({
                        'bucket_start_date': bucket.get('bucketStartDate'),
                        'market_price': bucket.get('marketPrice'),
                        'quantity_sold': bucket.get('quantitySold'),
                        'low_sale_price': bucket.get('lowSalePrice'),
                        'low_sale_price_with_shipping': bucket.get('lowSalePriceWithShipping'),
                        'high_sale_price': bucket.get('highSalePrice'),
                        'high_sale_price_with_shipping': bucket.get('highSalePriceWithShipping'),
                        'transaction_count': bucket.get('transactionCount')
                    })
                    records.append(record)
        
        return records
    
    def process_all_files(self) -> pd.DataFrame:
        """Process all JSON files and return a DataFrame"""
        json_path = Path(self.json_directory)
        json_files = list(json_path.glob("*.json"))
        total_files = len(json_files)
        
        logger.info(f"Found {total_files:,} JSON files to process")
        
        all_records = []
        processed_count = 0
        error_count = 0
        start_time = time.time()
        
        for json_file in json_files:
            try:
                records = self._process_json_file(json_file)
                all_records.extend(records)
                processed_count += 1
                
                # Progress update every 1000 files
                if processed_count % 1000 == 0:
                    elapsed = time.time() - start_time
                    rate = processed_count / elapsed if elapsed > 0 else 0
                    remaining = (total_files - processed_count) / rate if rate > 0 else 0
                    logger.info(f"Progress: {processed_count:,}/{total_files:,} files "
                               f"({processed_count/total_files*100:.1f}%) | "
                               f"Rate: {rate:.1f} files/sec | "
                               f"ETA: {remaining/60:.1f} min | "
                               f"Records: {len(all_records):,}")
                    
            except Exception as e:
                error_count += 1
                if error_count <= 10:
                    logger.error(f"Error processing {json_file.name}: {e}")
                continue
        
        elapsed = time.time() - start_time
        logger.info(f"Processing completed: {processed_count:,} files, "
                   f"{len(all_records):,} records, {error_count:,} errors, "
                   f"Time: {elapsed/60:.1f} minutes")
        
        # Convert to DataFrame and clean data
        df = pd.DataFrame(all_records)
        
        if df.empty:
            logger.warning("No records to upload")
            return df
        
        # Data type conversions
        logger.info("Converting data types...")
        
        numeric_columns = ['market_price', 'low_sale_price', 'low_sale_price_with_shipping',
                          'high_sale_price', 'high_sale_price_with_shipping']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        integer_columns = ['quantity_sold', 'transaction_count']
        for col in integer_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        
        # Convert dates
        if 'bucket_start_date' in df.columns:
            df['bucket_start_date'] = pd.to_datetime(df['bucket_start_date'], errors='coerce')
        
        if 'file_processed_at' in df.columns:
            df['file_processed_at'] = pd.to_datetime(df['file_processed_at'], errors='coerce')
        
        logger.info(f"DataFrame ready: {len(df):,} rows × {len(df.columns)} columns")
        return df
    
    def upload_to_bigquery(self, df: pd.DataFrame, mode: str = "replace"):
        """Upload DataFrame to BigQuery"""
        if df.empty:
            logger.warning("No data to upload")
            return
        
        logger.info(f"Uploading {len(df):,} records to BigQuery (mode={mode})")
        
        # Configure load job
        write_disposition = WriteDisposition.WRITE_TRUNCATE if mode == "replace" else WriteDisposition.WRITE_APPEND
        
        job_config = LoadJobConfig(
            write_disposition=write_disposition,
            autodetect=False,
            schema=self._get_table_schema(),
            max_bad_records=1000,
            ignore_unknown_values=True
        )
        
        start_time = time.time()
        
        # Load data
        job = self.client.load_table_from_dataframe(df, self.table_ref, job_config=job_config)
        
        logger.info("Waiting for BigQuery upload to complete...")
        job.result()  # Wait for completion
        
        elapsed = time.time() - start_time
        
        # Get final table info
        table = self.client.get_table(self.table_ref)
        
        logger.info(f"Upload completed: {table.num_rows:,} total rows in table, "
                   f"Time: {elapsed/60:.1f} minutes, "
                   f"Rate: {len(df)/elapsed:.0f} records/sec")
    
    def run(self, mode: str = "replace"):
        """Main execution method"""
        logger.info("="*60)
        logger.info("Starting TCG data processing pipeline")
        logger.info("="*60)
        
        try:
            # Process all files
            df = self.process_all_files()
            
            if not df.empty:
                # Upload to BigQuery
                self.upload_to_bigquery(df, mode=mode)
                logger.info("Pipeline completed successfully")
            else:
                logger.warning("No data processed")
                
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
        finally:
            logger.info("="*60)


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Process TCG data and upload to BigQuery')
    parser.add_argument('--project', help='GCP project ID', default=None)
    parser.add_argument('--dataset', help='BigQuery dataset ID', default='tcg_data')
    parser.add_argument('--table', help='BigQuery table ID', default='tcg_prices_bda')
    parser.add_argument('--directory', help='JSON files directory', default='./product_details')
    parser.add_argument('--mode', choices=['replace', 'append'], default='replace',
                       help='Upload mode: replace all data or append')
    
    args = parser.parse_args()
    
    try:
        processor = TCGDataProcessor(
            project_id=args.project,
            dataset_id=args.dataset,
            table_id=args.table,
            json_directory=args.directory
        )
        processor.run(mode=args.mode)
        
    except Exception as e:
        logger.error(f"Failed to run processor: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()