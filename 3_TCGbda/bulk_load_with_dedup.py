#!/usr/bin/env python3
"""
Enhanced bulk load script with proper deduplication strategy.
Uses MERGE to handle duplicates while preserving language variants, conditions, etc.
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
        buckets = result.get('buckets', [])
        if not buckets:
            # If no buckets, create one record with base info
            records.append(base_info)
        else:
            for bucket in buckets:
                record = base_info.copy()
                record.update({
                    'market_price': bucket.get('marketPrice'),
                    'quantity_sold': bucket.get('quantitySold'),
                    'low_sale_price': bucket.get('lowSalePrice'),
                    'low_sale_price_with_shipping': bucket.get('lowSalePriceWithShipping'),
                    'high_sale_price': bucket.get('highSalePrice'),
                    'high_sale_price_with_shipping': bucket.get('highSalePriceWithShipping'),
                    'transaction_count': bucket.get('transactionCount'),
                    'bucket_start_date': bucket.get('bucketStartDate')
                })
                records.append(record)
    
    return records

def create_bigquery_table_schema():
    """Create BigQuery table schema"""
    return [
        bigquery.SchemaField("product_id", "STRING"),
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
                      f"Rate: {rate:.1f} files/sec | ETA: {remaining/60:.1f} min | Records: {len(all_records):,}")
                
        except Exception as e:
            print(f"❌ Error processing {json_file.name}: {e}")
            error_count += 1
            continue
    
    elapsed = time.time() - start_time
    
    print(f"\n✅ Local processing completed!")
    print(f"📈 Stats:")
    print(f"   - Files processed: {processed_count:,}/{total_files:,}")
    print(f"   - Total records: {len(all_records):,}")
    print(f"   - Errors: {error_count}")
    print(f"   - Time: {elapsed/60:.1f} minutes")
    print(f"   - Rate: {processed_count/elapsed:.1f} files/sec")
    
    if not all_records:
        print("❌ No records to process!")
        return pd.DataFrame()
    
    return pd.DataFrame(all_records)

def upload_with_merge(client, project_id: str, dataset_id: str, table_id: str, df: pd.DataFrame):
    """Upload data using MERGE to handle deduplication properly"""
    
    # Create staging table name
    staging_table_id = f"{table_id}_staging_{int(time.time())}"
    
    print(f"🔄 Creating staging table: {staging_table_id}")
    
    # Create staging table
    staging_table_ref = client.dataset(dataset_id).table(staging_table_id)
    
    # Upload to staging table
    job_config = LoadJobConfig(
        write_disposition=WriteDisposition.WRITE_TRUNCATE,
        autodetect=False,
        schema=create_bigquery_table_schema()
    )
    
    print(f"⬆️  Uploading {len(df):,} records to staging table...")
    start_time = time.time()
    
    job = client.load_table_from_dataframe(df, staging_table_ref, job_config=job_config)
    job.result()  # Wait for completion
    
    upload_time = time.time() - start_time
    print(f"✅ Staging upload completed in {upload_time/60:.1f} minutes")
    
    # Perform MERGE operation
    print(f"🔄 Merging data with deduplication...")
    
    merge_query = f"""
    MERGE `{project_id}.{dataset_id}.{table_id}` AS target
    USING `{project_id}.{dataset_id}.{staging_table_id}` AS source
    ON target.product_id = source.product_id
       AND target.sku_id = source.sku_id
       AND target.language = source.language
       AND target.variant = source.variant
       AND target.condition = source.condition
       AND target.bucket_start_date = source.bucket_start_date
    WHEN MATCHED THEN
      UPDATE SET 
        average_daily_quantity_sold = source.average_daily_quantity_sold,
        average_daily_transaction_count = source.average_daily_transaction_count,
        total_quantity_sold = source.total_quantity_sold,
        total_transaction_count = source.total_transaction_count,
        market_price = source.market_price,
        quantity_sold = source.quantity_sold,
        low_sale_price = source.low_sale_price,
        low_sale_price_with_shipping = source.low_sale_price_with_shipping,
        high_sale_price = source.high_sale_price,
        high_sale_price_with_shipping = source.high_sale_price_with_shipping,
        transaction_count = source.transaction_count,
        file_processed_at = source.file_processed_at
    WHEN NOT MATCHED THEN
      INSERT (product_id, sku_id, variant, language, condition, average_daily_quantity_sold,
              average_daily_transaction_count, total_quantity_sold, total_transaction_count,
              bucket_start_date, market_price, quantity_sold, low_sale_price,
              low_sale_price_with_shipping, high_sale_price, high_sale_price_with_shipping,
              transaction_count, file_processed_at)
      VALUES (source.product_id, source.sku_id, source.variant, source.language, source.condition,
              source.average_daily_quantity_sold, source.average_daily_transaction_count,
              source.total_quantity_sold, source.total_transaction_count, source.bucket_start_date,
              source.market_price, source.quantity_sold, source.low_sale_price,
              source.low_sale_price_with_shipping, source.high_sale_price,
              source.high_sale_price_with_shipping, source.transaction_count, source.file_processed_at)
    """
    
    merge_start = time.time()
    merge_job = client.query(merge_query)
    merge_job.result()  # Wait for completion
    
    merge_time = time.time() - merge_start
    print(f"✅ MERGE completed in {merge_time/60:.1f} minutes")
    
    # Clean up staging table
    print(f"🗑️  Cleaning up staging table...")
    client.delete_table(staging_table_ref)
    
    return merge_job.num_dml_affected_rows

def main():
    """Main function with deduplication"""
    # Configuration
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
    
    print(f"🚀 Starting bulk load with deduplication...")
    print(f"Project: {PROJECT_ID}")
    print(f"Dataset: {DATASET_ID}")
    print(f"Table: {TABLE_ID}")
    print(f"Source directory: {JSON_DIRECTORY}")
    print("="*60)
    
    try:
        # Step 1: Process all files locally
        df = process_all_files_locally(JSON_DIRECTORY)
        
        if df.empty:
            print("❌ No data to upload!")
            return
        
        print(f"🔄 Converting {len(df):,} records to DataFrame...")
        
        # Data type conversions
        print(f"🔧 Converting data types...")
        
        # Convert numeric columns
        numeric_columns = [
            'market_price', 'low_sale_price', 'low_sale_price_with_shipping',
            'high_sale_price', 'high_sale_price_with_shipping'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Convert integer columns
        integer_columns = ['quantity_sold', 'transaction_count']
        for col in integer_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        
        # Convert date columns
        if 'bucket_start_date' in df.columns:
            df['bucket_start_date'] = pd.to_datetime(df['bucket_start_date'], errors='coerce')
        
        # Ensure timestamp column
        if 'file_processed_at' in df.columns:
            df['file_processed_at'] = pd.to_datetime(df['file_processed_at'], errors='coerce')
        
        print(f"✅ DataFrame ready: {len(df):,} rows × {len(df.columns)} columns")
        print("="*60)
        
        # Step 2: Connect to BigQuery
        print(f"🔗 Connecting to BigQuery...")
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
        
        # Step 3: Upload with MERGE deduplication
        affected_rows = upload_with_merge(client, PROJECT_ID, DATASET_ID, TABLE_ID, df)
        
        # Get final stats
        table = client.get_table(table_ref)
        print("="*60)
        print(f"🎉 Bulk load with deduplication completed!")
        print(f"📊 Final stats:")
        print(f"   - Rows affected by MERGE: {affected_rows}")
        print(f"   - Total rows in table: {table.num_rows:,}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

if __name__ == "__main__":
    main()