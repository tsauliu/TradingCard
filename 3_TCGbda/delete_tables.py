#!/usr/bin/env python3
"""Delete existing BigQuery tables before full upload"""

import os
from google.cloud import bigquery
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def delete_bigquery_tables():
    """Delete both test and main tables"""
    PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
    DATASET_ID = os.getenv("BIGQUERY_DATASET", "tcg_data")
    
    if not PROJECT_ID:
        print("❌ Error: GOOGLE_CLOUD_PROJECT environment variable not set")
        return
    
    print(f"🗑️  Deleting BigQuery tables in {PROJECT_ID}.{DATASET_ID}")
    
    client = bigquery.Client(project=PROJECT_ID)
    
    tables_to_delete = ["tcg_prices_bda", "tcg_prices_bda_test"]
    
    for table_id in tables_to_delete:
        try:
            table_ref = client.dataset(DATASET_ID).table(table_id)
            client.delete_table(table_ref)
            print(f"✅ Deleted table: {PROJECT_ID}.{DATASET_ID}.{table_id}")
        except Exception as e:
            if "Not found" in str(e):
                print(f"ℹ️  Table not found (already deleted): {PROJECT_ID}.{DATASET_ID}.{table_id}")
            else:
                print(f"❌ Error deleting {table_id}: {e}")
    
    print("🧹 Table cleanup completed")

if __name__ == "__main__":
    delete_bigquery_tables()