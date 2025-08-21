#!/usr/bin/env python3
"""
BigQuery Deduplication Script
Removes duplicate records from PSA auction prices table
"""

from google.cloud import bigquery
import os
from datetime import datetime

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

def deduplicate_psa_data():
    """Remove duplicates from PSA auction prices table"""
    
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'rising-environs-456314-a3')
    client = bigquery.Client(project=project_id)
    
    print("🧹 PSA BigQuery Deduplication")
    print("=" * 40)
    
    # First, check current duplicates
    duplicate_check_query = """
    WITH duplicates AS (
      SELECT 
        item_id, grade, record_type, 
        COUNT(*) as duplicate_count
      FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
      WHERE record_type = 'summary'
      GROUP BY item_id, grade, record_type
      HAVING COUNT(*) > 1
    )
    SELECT 
      COUNT(*) as duplicate_summary_combinations,
      SUM(duplicate_count - 1) as records_to_remove
    FROM duplicates
    """
    
    result = client.query(duplicate_check_query).to_dataframe()
    duplicates = result['duplicate_summary_combinations'].iloc[0]
    to_remove = result['records_to_remove'].iloc[0]
    
    print(f"📊 Found {duplicates} duplicate summary combinations")
    print(f"🗑️ Will remove {to_remove} duplicate records")
    
    if duplicates == 0:
        print("✅ No duplicates found - data is clean!")
        return
    
    # Create backup table first
    backup_query = f"""
    CREATE OR REPLACE TABLE `rising-environs-456314-a3.tcg_data.psa_auction_prices_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}` AS
    SELECT * FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
    """
    
    print("💾 Creating backup table...")
    client.query(backup_query).result()
    print("✅ Backup created successfully")
    
    # Deduplication using a different approach
    dedupe_query = """
    DELETE FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
    WHERE CONCAT(item_id, '|', grade, '|', record_type, '|', CAST(scraped_at AS STRING)) NOT IN (
      SELECT CONCAT(item_id, '|', grade, '|', record_type, '|', CAST(MAX(scraped_at) AS STRING))
      FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
      GROUP BY item_id, grade, record_type
    )
    """
    
    print("🧹 Removing duplicates...")
    client.query(dedupe_query).result()
    print("✅ Deduplication completed successfully")
    
    # Verify results
    final_check_query = """
    SELECT 
      COUNT(*) as total_records,
      COUNT(DISTINCT CONCAT(item_id, '-', grade, '-', record_type)) as unique_combinations
    FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
    """
    
    final_result = client.query(final_check_query).to_dataframe()
    total = final_result['total_records'].iloc[0]
    unique = final_result['unique_combinations'].iloc[0]
    
    print(f"\n📊 Final Results:")
    print(f"  Total records: {total:,}")
    print(f"  Unique combinations: {unique:,}")
    print(f"  Records removed: {to_remove:,}")
    
    print(f"\n✅ Deduplication completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    try:
        deduplicate_psa_data()
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()