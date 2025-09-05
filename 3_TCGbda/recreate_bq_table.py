#!/usr/bin/env python3
"""
Script to clean and recreate the BigQuery table with new schema
"""

import os
import sys
from google.cloud import bigquery
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def recreate_table():
    """Delete and recreate the tcg_prices_bda table with new schema"""
    
    # Configuration
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    dataset_id = "tcg_data"
    table_id = "tcg_prices_bda"
    
    if not project_id:
        logger.error("GOOGLE_CLOUD_PROJECT environment variable not set")
        sys.exit(1)
    
    logger.info(f"Project: {project_id}")
    logger.info(f"Dataset: {dataset_id}")
    logger.info(f"Table: {table_id}")
    
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)
    
    # Step 1: Check if table exists and get info
    try:
        existing_table = client.get_table(table_ref)
        logger.info(f"✓ Found existing table with {existing_table.num_rows:,} rows")
        logger.info("  Current schema fields:")
        for field in existing_table.schema:
            logger.info(f"    - {field.name} ({field.field_type})")
    except Exception as e:
        logger.info(f"Table doesn't exist or error accessing it: {e}")
        existing_table = None
    
    # Step 2: Delete existing table
    if existing_table:
        confirmation = input("\n⚠️  WARNING: This will DELETE ALL DATA in tcg_prices_bda table!\n"
                           "Type 'DELETE' to confirm: ")
        
        if confirmation != 'DELETE':
            logger.info("Aborted - confirmation not received")
            sys.exit(0)
        
        try:
            client.delete_table(table_ref)
            logger.info("✓ Successfully deleted existing table")
        except Exception as e:
            logger.error(f"Failed to delete table: {e}")
            sys.exit(1)
    
    # Step 3: Define new schema with scrape_date
    schema = [
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
        bigquery.SchemaField("scrape_date", "DATE", mode="REQUIRED"),  # NEW FIELD
        bigquery.SchemaField("source_file", "STRING", mode="NULLABLE")
    ]
    
    # Step 4: Create new table with schema and partitioning
    try:
        new_table = bigquery.Table(table_ref, schema=schema)
        
        # Configure partitioning by scrape_date
        new_table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="scrape_date",  # Partition by scrape_date column
            expiration_ms=None,   # No automatic partition expiration
        )
        
        # Add clustering for better query performance
        new_table.clustering_fields = ["product_id", "language", "condition"]
        
        # Set table description
        new_table.description = (
            "TCG price data from BDA with daily partitioning by scrape_date. "
            "Clustered by product_id, language, and condition for optimal query performance."
        )
        
        new_table = client.create_table(new_table)
        logger.info("✓ Successfully created new PARTITIONED table with updated schema")
        logger.info(f"  - Partitioned by: scrape_date (DAY)")
        logger.info(f"  - Clustered by: product_id, language, condition")
    except Exception as e:
        logger.error(f"Failed to create table: {e}")
        sys.exit(1)
    
    # Step 5: Verify new table
    try:
        final_table = client.get_table(table_ref)
        logger.info("\n✅ Table recreated successfully!")
        logger.info(f"   - Total rows: {final_table.num_rows:,}")
        logger.info(f"   - Total size: {final_table.num_bytes:,} bytes")
        
        # Show partitioning info
        logger.info("\n📊 Partitioning Configuration:")
        if final_table.time_partitioning:
            logger.info(f"   - Partition field: {final_table.time_partitioning.field}")
            logger.info(f"   - Partition type: {final_table.time_partitioning.type_}")
            logger.info(f"   - Partition expiration: {'None' if not final_table.time_partitioning.expiration_ms else f'{final_table.time_partitioning.expiration_ms/86400000} days'}")
        
        if final_table.clustering_fields:
            logger.info(f"   - Clustering fields: {', '.join(final_table.clustering_fields)}")
        
        logger.info("\n📋 Schema fields:")
        for field in final_table.schema:
            required = " (REQUIRED)" if field.mode == "REQUIRED" else ""
            logger.info(f"     • {field.name} ({field.field_type}){required}")
        
        # Highlight the key changes
        logger.info("\n🔄 Key improvements:")
        logger.info("   - ✅ Added: scrape_date (DATE, REQUIRED) - partition field")
        logger.info("   - ✅ Added: Daily partitioning for faster queries")
        logger.info("   - ✅ Added: Clustering on product_id, language, condition")
        logger.info("   - ❌ Removed: file_processed_at (TIMESTAMP)")
        logger.info("   - ❌ Removed: source_directory field")
        
    except Exception as e:
        logger.error(f"Failed to verify table: {e}")
        sys.exit(1)
    
    # Step 6: Clean up tracking CSV (optional)
    tracking_csv = "uploaded_files_tracker.csv"
    if os.path.exists(tracking_csv):
        clean_csv = input(f"\nDo you want to delete the tracking CSV '{tracking_csv}'? (y/N): ")
        if clean_csv.lower() == 'y':
            os.remove(tracking_csv)
            logger.info(f"✓ Deleted {tracking_csv}")
        else:
            logger.info(f"Kept {tracking_csv} - consider deleting it manually for a fresh start")
    
    logger.info("\n✨ Table is ready for fresh data import with deduplication tracking!")
    logger.info("Run './run_tcg_processor.sh start append ./product_details yes' to process data")

if __name__ == "__main__":
    recreate_table()