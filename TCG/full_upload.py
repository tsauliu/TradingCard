#!/usr/bin/env python3
"""
Enhanced bulk upload script with optimal batching strategy based on speed test results.
Processes 29,486 JSON files in batches with comprehensive logging and progress tracking.
"""

import json
import os
import re
from pathlib import Path
from typing import List, Dict, Any
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, WriteDisposition
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
import time
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'bigquery_upload_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)

def extract_product_id(filename: str) -> str:
    """Extract product ID from filename like '481225.0.json' -> '481225'"""
    match = re.match(r'^(\d+)\.0\.json$', filename)
    if match:
        return match.group(1)
    raise ValueError(f"Invalid filename format: {filename}")

def process_json_file(file_path: Path) -> List[Dict[str, Any]]:
    """Process a single JSON file and return flattened records"""
    product_id = extract_product_id(file_path.name)
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise ValueError(f"Invalid JSON file: {e}")
    
    records = []
    results = data.get('result')
    if not results:
        return records  # Empty file, skip
    
    for result in results:
        # Extract base product info
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
        
        # Process buckets (price history)
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

def create_bigquery_table_schema():
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

def process_file_batch(json_files: List[Path], batch_num: int, total_batches: int) -> pd.DataFrame:
    """Process a batch of JSON files"""
    logger.info(f"🔄 Processing batch {batch_num}/{total_batches} ({len(json_files):,} files)")
    
    all_records = []
    error_count = 0
    start_time = time.time()
    
    for i, json_file in enumerate(json_files):
        try:
            records = process_json_file(json_file)
            all_records.extend(records)
            
            if (i + 1) % 100 == 0:
                logger.info(f"   Processed {i+1:,}/{len(json_files):,} files in batch {batch_num}")
                
        except Exception as e:
            error_count += 1
            if error_count <= 5:  # Log first 5 errors per batch
                logger.warning(f"   Error processing {json_file.name}: {e}")
            continue
    
    elapsed = time.time() - start_time
    logger.info(f"✅ Batch {batch_num} completed: {len(json_files):,} files → {len(all_records):,} records in {elapsed:.1f}s")
    
    if error_count > 5:
        logger.warning(f"   Total errors in batch {batch_num}: {error_count}")
    
    # Convert to DataFrame
    if not all_records:
        return pd.DataFrame()
    
    df = pd.DataFrame(all_records)
    
    # Data type conversions
    numeric_columns = [
        'market_price', 'low_sale_price', 'low_sale_price_with_shipping',
        'high_sale_price', 'high_sale_price_with_shipping'
    ]
    
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
    
    return df

def upload_to_bigquery(client, table_ref, df, upload_num: int, total_uploads: int):
    """Upload DataFrame to BigQuery with retry logic"""
    if df.empty:
        logger.warning(f"⚠️  No data to upload for batch {upload_num}")
        return
    
    logger.info(f"🚀 Upload {upload_num}/{total_uploads}: Uploading {len(df):,} records to BigQuery")
    
    # Use APPEND mode for subsequent uploads after the first
    write_mode = WriteDisposition.WRITE_TRUNCATE if upload_num == 1 else WriteDisposition.WRITE_APPEND
    
    job_config = LoadJobConfig(
        write_disposition=write_mode,
        autodetect=False,
        schema=create_bigquery_table_schema(),
        max_bad_records=1000,
        ignore_unknown_values=True
    )
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            start_time = time.time()
            job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job.result()  # Wait for completion
            
            elapsed = time.time() - start_time
            rate = len(df) / elapsed if elapsed > 0 else 0
            
            logger.info(f"✅ Upload {upload_num} completed: {len(df):,} records in {elapsed:.1f}s ({rate:.0f} records/sec)")
            break
            
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                logger.warning(f"⚠️  Upload {upload_num} attempt {attempt+1} failed: {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                logger.error(f"❌ Upload {upload_num} failed after {max_retries} attempts: {e}")
                raise
    
    # Rate limiting delay
    if upload_num < total_uploads:
        logger.info("⏳ Rate limiting: waiting 3 seconds before next upload...")
        time.sleep(3)

def main():
    """Enhanced bulk loading with optimal batch strategy"""
    PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
    DATASET_ID = os.getenv("BIGQUERY_DATASET", "tcg_data")
    TABLE_ID = "tcg_prices_bda"
    JSON_DIRECTORY = "./product_details"
    
    # Optimal batch sizes from speed test
    FILE_BATCH_SIZE = 1000  # Files per processing batch
    UPLOAD_RECORD_BATCH_SIZE = 25000  # Records per BigQuery upload
    
    if not PROJECT_ID:
        logger.error("❌ GOOGLE_CLOUD_PROJECT environment variable not set")
        return
    
    if not os.path.exists(JSON_DIRECTORY):
        logger.error(f"❌ Directory {JSON_DIRECTORY} does not exist")
        return
    
    logger.info("🚀 Starting enhanced bulk upload process")
    logger.info(f"Project: {PROJECT_ID}")
    logger.info(f"Dataset: {DATASET_ID}")
    logger.info(f"Table: {TABLE_ID}")
    logger.info(f"Source: {JSON_DIRECTORY}")
    logger.info(f"File batch size: {FILE_BATCH_SIZE:,}")
    logger.info(f"Upload batch size: {UPLOAD_RECORD_BATCH_SIZE:,}")
    logger.info("=" * 80)
    
    # Get all JSON files
    json_path = Path(JSON_DIRECTORY)
    all_json_files = list(json_path.glob("*.json"))
    total_files = len(all_json_files)
    
    logger.info(f"📁 Found {total_files:,} JSON files to process")
    
    # Split into batches for processing
    file_batches = [all_json_files[i:i + FILE_BATCH_SIZE] 
                   for i in range(0, total_files, FILE_BATCH_SIZE)]
    
    logger.info(f"📊 Split into {len(file_batches)} processing batches of {FILE_BATCH_SIZE:,} files each")
    
    try:
        # Initialize BigQuery client
        logger.info("🔗 Connecting to BigQuery...")
        client = bigquery.Client(project=PROJECT_ID)
        table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
        
        # Create table if it doesn't exist
        try:
            table = client.get_table(table_ref)
            logger.info(f"✅ Using existing table {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
        except Exception:
            schema = create_bigquery_table_schema()
            table = bigquery.Table(table_ref, schema=schema)
            table = client.create_table(table)
            logger.info(f"✅ Created table {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
        
        # Process and upload in batches
        total_records = 0
        total_uploads = 0
        current_batch_records = []
        overall_start = time.time()
        
        for batch_num, file_batch in enumerate(file_batches, 1):
            # Process file batch
            df = process_file_batch(file_batch, batch_num, len(file_batches))
            
            if df.empty:
                continue
            
            # Add to current upload batch
            current_batch_records.append(df)
            batch_records = sum(len(df) for df in current_batch_records)
            
            # Upload when we have enough records or it's the last batch
            if batch_records >= UPLOAD_RECORD_BATCH_SIZE or batch_num == len(file_batches):
                if current_batch_records:
                    # Combine DataFrames
                    combined_df = pd.concat(current_batch_records, ignore_index=True)
                    total_uploads += 1
                    
                    # Upload to BigQuery
                    estimated_total_uploads = ((total_files * 239) // UPLOAD_RECORD_BATCH_SIZE) + 1
                    upload_to_bigquery(client, table_ref, combined_df, total_uploads, estimated_total_uploads)
                    
                    total_records += len(combined_df)
                    current_batch_records = []
                    
                    # Progress update
                    elapsed = time.time() - overall_start
                    processed_files = batch_num * FILE_BATCH_SIZE
                    if processed_files > total_files:
                        processed_files = total_files
                        
                    logger.info(f"📈 Progress: {processed_files:,}/{total_files:,} files ({processed_files/total_files*100:.1f}%) | "
                              f"Records: {total_records:,} | "
                              f"Time: {elapsed/60:.1f} min | "
                              f"Uploads: {total_uploads}")
        
        # Final statistics
        final_elapsed = time.time() - overall_start
        table = client.get_table(table_ref)
        
        logger.info("=" * 80)
        logger.info("🎉 UPLOAD COMPLETED SUCCESSFULLY!")
        logger.info(f"📊 Final Statistics:")
        logger.info(f"   Files processed: {total_files:,}")
        logger.info(f"   Records uploaded: {table.num_rows:,}")
        logger.info(f"   Total uploads: {total_uploads}")
        logger.info(f"   Total time: {final_elapsed/60:.1f} minutes ({final_elapsed/3600:.2f} hours)")
        logger.info(f"   Average rate: {total_files/final_elapsed:.1f} files/sec")
        logger.info(f"   Table: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
        
    except Exception as e:
        logger.error(f"❌ Error during bulk upload: {e}")
        raise

if __name__ == "__main__":
    main()