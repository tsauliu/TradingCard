#!/usr/bin/env python3
"""
Speed test script: Process first 10 JSON files to estimate processing time
and BigQuery upload performance for the full dataset.
"""

import json
import os
import re
import time
from pathlib import Path
from typing import List, Dict, Any
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, WriteDisposition
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv

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

def process_test_files(json_directory: str, limit: int = 10) -> pd.DataFrame:
    """Process first N JSON files and return DataFrame with timing info"""
    json_path = Path(json_directory)
    json_files = list(json_path.glob("*.json"))[:limit]
    
    print(f"🧪 Processing first {limit} JSON files for speed test...")
    
    all_records = []
    processing_times = []
    file_sizes = []
    
    start_time = time.time()
    
    for i, json_file in enumerate(json_files):
        file_start = time.time()
        file_size = json_file.stat().st_size
        
        try:
            records = process_json_file(json_file)
            all_records.extend(records)
            
            file_time = time.time() - file_start
            processing_times.append(file_time)
            file_sizes.append(file_size)
            
            print(f"  File {i+1}: {json_file.name} | "
                  f"Size: {file_size:,} bytes | "
                  f"Records: {len(records):,} | "
                  f"Time: {file_time:.3f}s")
                
        except Exception as e:
            print(f"❌ Error processing {json_file.name}: {e}")
            continue
    
    total_processing_time = time.time() - start_time
    
    print(f"\n📊 Processing Summary:")
    print(f"   Files processed: {len(processing_times)}")
    print(f"   Total records: {len(all_records):,}")
    print(f"   Total time: {total_processing_time:.2f}s")
    print(f"   Average time per file: {sum(processing_times)/len(processing_times):.3f}s")
    print(f"   Average records per file: {len(all_records)/len(processing_times):.1f}")
    print(f"   Processing rate: {len(processing_times)/total_processing_time:.1f} files/sec")
    
    # Convert to DataFrame
    print(f"\n🔄 Converting to DataFrame...")
    df_start = time.time()
    df = pd.DataFrame(all_records)
    
    if df.empty:
        print("❌ No records to process!")
        return df
    
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
    
    df_time = time.time() - df_start
    print(f"   DataFrame conversion: {df_time:.2f}s")
    print(f"   Final shape: {len(df):,} rows × {len(df.columns)} columns")
    
    return df, {
        'files_processed': len(processing_times),
        'total_records': len(all_records),
        'processing_time': total_processing_time,
        'df_conversion_time': df_time,
        'avg_time_per_file': sum(processing_times)/len(processing_times),
        'avg_records_per_file': len(all_records)/len(processing_times),
        'processing_rate': len(processing_times)/total_processing_time,
        'avg_file_size': sum(file_sizes)/len(file_sizes)
    }

def upload_test_to_bigquery(client, table_ref, df):
    """Upload test DataFrame to BigQuery and measure time"""
    print(f"\n🚀 Uploading {len(df):,} records to BigQuery (test)...")
    
    job_config = LoadJobConfig(
        write_disposition=WriteDisposition.WRITE_TRUNCATE,
        autodetect=False,
        schema=create_bigquery_table_schema(),
        max_bad_records=100
    )
    
    upload_start = time.time()
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    
    print("⏳ Waiting for upload to complete...")
    job.result()  # Wait for completion
    
    upload_time = time.time() - upload_start
    
    # Get final table info
    table = client.get_table(table_ref)
    
    print(f"✅ Upload completed!")
    print(f"   Records in table: {table.num_rows:,}")
    print(f"   Upload time: {upload_time:.2f}s")
    print(f"   Upload rate: {table.num_rows/upload_time:.0f} records/sec")
    
    return {
        'upload_time': upload_time,
        'records_uploaded': table.num_rows,
        'upload_rate': table.num_rows/upload_time
    }

def main():
    """Run speed test with first 10 files"""
    # Configuration
    PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
    DATASET_ID = os.getenv("BIGQUERY_DATASET", "tcg_data")
    TABLE_ID = "tcg_prices_bda_test"  # Use test table
    JSON_DIRECTORY = "./product_details"
    TEST_FILES = 10
    
    # Validate setup
    if not PROJECT_ID:
        print("❌ Error: GOOGLE_CLOUD_PROJECT environment variable not set")
        return
    
    if not os.path.exists(JSON_DIRECTORY):
        print(f"❌ Error: Directory {JSON_DIRECTORY} does not exist")
        return
    
    print(f"🧪 SPEED TEST - Processing {TEST_FILES} files")
    print(f"Project: {PROJECT_ID}")
    print(f"Dataset: {DATASET_ID}")
    print(f"Test Table: {TABLE_ID}")
    print(f"Source: {JSON_DIRECTORY}")
    print("="*60)
    
    try:
        # Step 1: Process files
        df, processing_stats = process_test_files(JSON_DIRECTORY, TEST_FILES)
        
        if df.empty:
            print("❌ No data to upload")
            return
        
        # Step 2: Upload to BigQuery
        print("="*40)
        client = bigquery.Client(project=PROJECT_ID)
        table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
        
        # Create/get table
        try:
            table = client.get_table(table_ref)
            print(f"✅ Using existing test table")
        except Exception:
            schema = create_bigquery_table_schema()
            table = bigquery.Table(table_ref, schema=schema)
            table = client.create_table(table)
            print(f"✅ Created test table")
        
        upload_stats = upload_test_to_bigquery(client, table_ref, df)
        
        # Step 3: Calculate projections
        print("\n" + "="*60)
        print("📈 PROJECTIONS FOR FULL DATASET (29,486 files)")
        
        total_files = 29486
        
        # Processing projections
        total_processing_time = (processing_stats['avg_time_per_file'] * total_files) / 60  # minutes
        total_records = int(processing_stats['avg_records_per_file'] * total_files)
        
        # Upload projections (assuming batches of 25,000 records)
        batch_size = 25000
        num_batches = total_records // batch_size + (1 if total_records % batch_size else 0)
        total_upload_time = (upload_stats['upload_time'] * num_batches) / 60  # minutes
        
        # Add rate limiting delays (2 seconds between batches)
        rate_limit_time = (num_batches * 2) / 60  # minutes
        
        print(f"📊 Processing Estimates:")
        print(f"   Total files: {total_files:,}")
        print(f"   Estimated records: {total_records:,}")
        print(f"   Processing time: {total_processing_time:.1f} minutes")
        print(f"   Records per file: {processing_stats['avg_records_per_file']:.1f}")
        
        print(f"\n📤 Upload Estimates:")
        print(f"   Upload batches needed: {num_batches:,} (at {batch_size:,} records/batch)")
        print(f"   Upload time: {total_upload_time:.1f} minutes")
        print(f"   Rate limiting delays: {rate_limit_time:.1f} minutes")
        print(f"   **Total estimated time: {(total_processing_time + total_upload_time + rate_limit_time):.1f} minutes ({(total_processing_time + total_upload_time + rate_limit_time)/60:.1f} hours)**")
        
        print(f"\n💡 Recommended Batch Strategy:")
        print(f"   File processing batches: 1,000 files (takes ~{processing_stats['avg_time_per_file']*1000/60:.1f} min)")
        print(f"   BigQuery upload batches: {batch_size:,} records")
        print(f"   Rate limiting: 2-3 seconds between uploads")
        
    except Exception as e:
        print(f"❌ Error during speed test: {e}")
        raise

if __name__ == "__main__":
    main()