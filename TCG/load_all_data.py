#!/usr/bin/env python3
"""
Load all JSON files to BigQuery with progress tracking and better error handling
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

# Load environment variables
load_dotenv()

def extract_product_id(filename: str) -> str:
    """Extract product ID from filename like '481225.0.json' -> '481225'"""
    match = re.match(r'^(\d+)\.0\.json$', filename)
    if match:
        return match.group(1)
    raise ValueError(f"Invalid filename format: {filename}")

def process_json_file(file_path: Path) -> List[Dict[str, Any]]:
    """Process a single JSON file and return flattened records"""
    product_id = extract_product_id(file_path.name)
    
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    records = []
    
    for result in data.get('result', []):
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
        for bucket in result.get('buckets', []):
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

def upload_batch_to_bigquery(client, table_ref, records):
    """Upload a batch of records to BigQuery"""
    if not records:
        return
    
    # Convert to DataFrame for easier handling
    df = pd.DataFrame(records)
    
    # Convert string numbers to appropriate types
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
    
    # Convert bucket_start_date to datetime
    if 'bucket_start_date' in df.columns:
        df['bucket_start_date'] = pd.to_datetime(df['bucket_start_date'], errors='coerce')
    
    # Ensure file_processed_at is datetime
    if 'file_processed_at' in df.columns:
        df['file_processed_at'] = pd.to_datetime(df['file_processed_at'], errors='coerce')
    
    # Configure load job
    job_config = LoadJobConfig(
        write_disposition=WriteDisposition.WRITE_APPEND,
        autodetect=False,
        schema=create_bigquery_table_schema()
    )
    
    # Load data
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for job to complete

def clear_existing_data(client, table_ref):
    """Clear existing data from the table"""
    try:
        query = f"DELETE FROM `{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}` WHERE TRUE"
        job = client.query(query)
        job.result()
        print("✅ Cleared existing data from table")
    except Exception as e:
        print(f"Note: Could not clear existing data: {e}")

def main():
    """Load all JSON files to BigQuery"""
    # Configuration from environment variables
    PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
    DATASET_ID = os.getenv("BIGQUERY_DATASET", "tcg_data")
    TABLE_ID = "tcg_prices_bda"
    JSON_DIRECTORY = "./product_details"
    BATCH_SIZE = 500  # Smaller batch size for better progress tracking
    
    # Validate required environment variables
    if not PROJECT_ID:
        print("Error: GOOGLE_CLOUD_PROJECT environment variable not set")
        return
    
    # Validate directory exists
    if not os.path.exists(JSON_DIRECTORY):
        print(f"Error: Directory {JSON_DIRECTORY} does not exist")
        return
    
    print(f"🚀 Starting BigQuery load process...")
    print(f"Project: {PROJECT_ID}")
    print(f"Dataset: {DATASET_ID}")
    print(f"Table: {TABLE_ID}")
    print(f"Source directory: {JSON_DIRECTORY}")
    
    try:
        # Initialize BigQuery client
        client = bigquery.Client(project=PROJECT_ID)
        
        # Get table reference
        table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
        
        # Clear existing data (except our test data)
        print("🧹 Clearing existing data...")
        clear_existing_data(client, table_ref)
        
        # Create table if it doesn't exist
        try:
            table = client.get_table(table_ref)
            print(f"✅ Table {PROJECT_ID}.{DATASET_ID}.{TABLE_ID} exists")
        except Exception:
            schema = create_bigquery_table_schema()
            table = bigquery.Table(table_ref, schema=schema)
            table = client.create_table(table)
            print(f"✅ Created table {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
        
        # Get all JSON files
        json_path = Path(JSON_DIRECTORY)
        json_files = list(json_path.glob("*.json"))
        total_files = len(json_files)
        
        print(f"📁 Found {total_files} JSON files to process")
        
        all_records = []
        processed_count = 0
        error_count = 0
        total_records = 0
        start_time = time.time()
        
        for i, json_file in enumerate(json_files):
            try:
                records = process_json_file(json_file)
                all_records.extend(records)
                processed_count += 1
                total_records += len(records)
                
                # Progress update every 100 files
                if processed_count % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = processed_count / elapsed if elapsed > 0 else 0
                    remaining = (total_files - processed_count) / rate if rate > 0 else 0
                    print(f"📊 Progress: {processed_count}/{total_files} files ({processed_count/total_files*100:.1f}%) | "
                          f"Rate: {rate:.1f} files/sec | "
                          f"ETA: {remaining/60:.1f} min | "
                          f"Records: {total_records:,}")
                
                # Upload in batches
                if len(all_records) >= BATCH_SIZE:
                    upload_batch_to_bigquery(client, table_ref, all_records)
                    print(f"✅ Uploaded batch of {len(all_records):,} records")
                    all_records = []
                    
            except Exception as e:
                error_count += 1
                print(f"❌ Error processing {json_file.name}: {e}")
                if error_count % 10 == 0:
                    print(f"⚠️  Total errors so far: {error_count}")
                continue
        
        # Upload remaining records
        if all_records:
            upload_batch_to_bigquery(client, table_ref, all_records)
            print(f"✅ Uploaded final batch of {len(all_records):,} records")
        
        elapsed = time.time() - start_time
        print(f"\n🎉 Load completed!")
        print(f"📈 Stats:")
        print(f"   - Files processed: {processed_count:,}/{total_files:,}")
        print(f"   - Total records: {total_records:,}")
        print(f"   - Errors: {error_count:,}")
        print(f"   - Time: {elapsed/60:.1f} minutes")
        print(f"   - Rate: {processed_count/elapsed:.1f} files/sec")
        
    except Exception as e:
        print(f"❌ Error during BigQuery load: {e}")
        raise

if __name__ == "__main__":
    main()