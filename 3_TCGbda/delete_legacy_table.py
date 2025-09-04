#!/usr/bin/env python3
"""
Delete the legacy product_prices table
"""

import os
from google.cloud import bigquery
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def delete_legacy_table():
    """Delete the legacy product_prices table"""
    PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
    DATASET_ID = os.getenv("BIGQUERY_DATASET", "tcg_data")
    LEGACY_TABLE_ID = "product_prices"
    
    if not PROJECT_ID:
        print("Error: GOOGLE_CLOUD_PROJECT environment variable not set")
        return
    
    # Initialize BigQuery client
    client = bigquery.Client(project=PROJECT_ID)
    
    # Get table reference
    table_ref = client.dataset(DATASET_ID).table(LEGACY_TABLE_ID)
    
    try:
        # Check if table exists first
        table = client.get_table(table_ref)
        print(f"Found legacy table: {PROJECT_ID}.{DATASET_ID}.{LEGACY_TABLE_ID}")
        
        # Delete the table
        client.delete_table(table_ref)
        print(f"✅ Successfully deleted legacy table: {PROJECT_ID}.{DATASET_ID}.{LEGACY_TABLE_ID}")
        
    except Exception as e:
        if "Not found" in str(e):
            print(f"ℹ️  Legacy table {PROJECT_ID}.{DATASET_ID}.{LEGACY_TABLE_ID} does not exist - nothing to delete")
        else:
            print(f"❌ Error deleting legacy table: {e}")
            raise

def main():
    """Main function"""
    try:
        delete_legacy_table()
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    main()