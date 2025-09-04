#!/usr/bin/env python3
"""
Load a small subset of test data to BigQuery for testing
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

def upload_records_to_bigquery(client, table_ref, records):
    """Upload records to BigQuery"""
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
    
    print(f"Loaded {len(records)} records to BigQuery")

def main():
    """Load test data to BigQuery"""
    # Configuration from environment variables
    PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
    DATASET_ID = os.getenv("BIGQUERY_DATASET", "tcg_data")
    TABLE_ID = "tcg_prices_bda"
    JSON_DIRECTORY = "./product_details"
    
    # Test files to load
    test_files = ["481225.0.json", "100495.0.json", "42349.0.json"]
    
    # Validate required environment variables
    if not PROJECT_ID:
        print("Error: GOOGLE_CLOUD_PROJECT environment variable not set")
        return
    
    print(f"Loading test data to BigQuery...")
    print(f"Project: {PROJECT_ID}")
    print(f"Dataset: {DATASET_ID}")
    print(f"Table: {TABLE_ID}")
    
    try:
        # Initialize BigQuery client
        client = bigquery.Client(project=PROJECT_ID)
        
        # Get table reference
        table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
        
        # Create table if it doesn't exist
        try:
            table = client.get_table(table_ref)
            print(f"Table {PROJECT_ID}.{DATASET_ID}.{TABLE_ID} already exists")
        except Exception:
            schema = create_bigquery_table_schema()
            table = bigquery.Table(table_ref, schema=schema)
            table = client.create_table(table)
            print(f"Created table {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
        
        # Process test files
        json_path = Path(JSON_DIRECTORY)
        all_records = []
        
        for filename in test_files:
            file_path = json_path / filename
            if file_path.exists():
                try:
                    records = process_json_file(file_path)
                    all_records.extend(records)
                    print(f"Processed {filename}: {len(records)} records")
                except Exception as e:
                    print(f"Error processing {filename}: {e}")
            else:
                print(f"File not found: {filename}")
        
        # Upload all records
        if all_records:
            upload_records_to_bigquery(client, table_ref, all_records)
            print(f"Successfully loaded {len(all_records)} total records to BigQuery!")
        else:
            print("No records to upload")
            
    except Exception as e:
        print(f"Error during BigQuery load: {e}")
        raise

if __name__ == "__main__":
    main()