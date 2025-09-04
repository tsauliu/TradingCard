#!/usr/bin/env python3
"""
Optimized bulk loading: Process all JSONs locally first, then single BigQuery upload
This avoids rate limits and is much faster than individual uploads
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
    """Extract product ID from filename like '481225.0.json' or '481225.json' -> '481225'"""
    match = re.match(r'^(\d+)(?:\.0)?\.json$', filename)
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
            # If no buckets, still create one record with base info
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

def process_all_files_locally(json_directory: str) -> pd.DataFrame:
    """Process all JSON files locally and return a single DataFrame"""
    json_path = Path(json_directory)
    json_files = list(json_path.glob("*.json"))
    total_files = len(json_files)
    
    print(f"📁 Processing {total_files:,} JSON files locally...")
    
    all_records = []
    processed_count = 0
    error_count = 0
    start_time = time.time()
    
    for i, json_file in enumerate(json_files):
        try:
            records = process_json_file(json_file)
            all_records.extend(records)
            processed_count += 1
            
            # Progress update every 1000 files
            if processed_count % 1000 == 0:
                elapsed = time.time() - start_time
                rate = processed_count / elapsed if elapsed > 0 else 0
                remaining = (total_files - processed_count) / rate if rate > 0 else 0
                print(f"📊 Progress: {processed_count:,}/{total_files:,} files ({processed_count/total_files*100:.1f}%) | "
                      f"Rate: {rate:.1f} files/sec | "
                      f"ETA: {remaining/60:.1f} min | "
                      f"Records: {len(all_records):,}")
                
        except Exception as e:
            error_count += 1
            if error_count <= 10:  # Only show first 10 errors
                print(f"❌ Error processing {json_file.name}: {e}")
            elif error_count % 100 == 0:
                print(f"⚠️  Total errors so far: {error_count}")
            continue
    
    elapsed = time.time() - start_time
    print(f"\n✅ Local processing completed!")
    print(f"📈 Stats:")
    print(f"   - Files processed: {processed_count:,}/{total_files:,}")
    print(f"   - Total records: {len(all_records):,}")
    print(f"   - Errors: {error_count:,}")
    print(f"   - Time: {elapsed/60:.1f} minutes")
    print(f"   - Rate: {processed_count/elapsed:.1f} files/sec")
    
    # Convert to DataFrame
    print(f"🔄 Converting {len(all_records):,} records to DataFrame...")
    df = pd.DataFrame(all_records)
    
    if df.empty:
        print("❌ No records to upload!")
        return df
    
    # Data type conversions
    print("🔧 Converting data types...")
    
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
    
    print(f"✅ DataFrame ready: {len(df):,} rows × {len(df.columns)} columns")
    return df

def upload_dataframe_to_bigquery(client, table_ref, df):
    """Upload the entire DataFrame to BigQuery in one operation"""
    print(f"🚀 Uploading {len(df):,} records to BigQuery...")
    
    # Configure load job for better performance
    job_config = LoadJobConfig(
        write_disposition=WriteDisposition.WRITE_TRUNCATE,  # Replace all data
        autodetect=False,
        schema=create_bigquery_table_schema(),
        # Optimize for large uploads
        max_bad_records=1000,  # Allow some bad records
        ignore_unknown_values=True
    )
    
    start_time = time.time()
    
    # Load data
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    
    # Wait for completion with progress
    print("⏳ Waiting for BigQuery upload to complete...")
    job.result()  # This blocks until complete
    
    elapsed = time.time() - start_time
    
    # Get final table info
    table = client.get_table(table_ref)
    
    print(f"✅ Upload completed successfully!")
    print(f"📊 Upload stats:")
    print(f"   - Records uploaded: {table.num_rows:,}")
    print(f"   - Upload time: {elapsed/60:.1f} minutes")
    print(f"   - Rate: {table.num_rows/elapsed:.0f} records/sec")

def main():
    """Main bulk loading function"""
    # Configuration from environment variables
    PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
    DATASET_ID = os.getenv("BIGQUERY_DATASET", "tcg_data")
    TABLE_ID = "tcg_prices_bda"
    JSON_DIRECTORY = "/tmp/product_details"
    
    # Validate required environment variables
    if not PROJECT_ID:
        print("❌ Error: GOOGLE_CLOUD_PROJECT environment variable not set")
        return
    
    # Validate directory exists
    if not os.path.exists(JSON_DIRECTORY):
        print(f"❌ Error: Directory {JSON_DIRECTORY} does not exist")
        return
    
    print(f"🚀 Starting optimized bulk load process...")
    print(f"Project: {PROJECT_ID}")
    print(f"Dataset: {DATASET_ID}")
    print(f"Table: {TABLE_ID}")
    print(f"Source directory: {JSON_DIRECTORY}")
    print("="*60)
    
    try:
        # Step 1: Process all files locally
        df = process_all_files_locally(JSON_DIRECTORY)
        
        if df.empty:
            print("❌ No data to upload")
            return
        
        print("="*60)
        
        # Step 2: Initialize BigQuery client
        print("🔗 Connecting to BigQuery...")
        client = bigquery.Client(project=PROJECT_ID)
        
        # Get table reference
        table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
        
        # Create table if it doesn't exist
        try:
            table = client.get_table(table_ref)
            print(f"✅ Table {PROJECT_ID}.{DATASET_ID}.{TABLE_ID} exists")
        except Exception:
            schema = create_bigquery_table_schema()
            table = bigquery.Table(table_ref, schema=schema)
            table = client.create_table(table)
            print(f"✅ Created table {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
        
        # Step 3: Single bulk upload
        upload_dataframe_to_bigquery(client, table_ref, df)
        
        print("="*60)
        print("🎉 Bulk load completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during bulk load: {e}")
        raise

if __name__ == "__main__":
    main()