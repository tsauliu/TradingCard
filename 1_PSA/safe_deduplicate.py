#!/usr/bin/env python3
"""
Safe PSA BigQuery Deduplication 
Only removes duplicate summary records for same item/grade combinations
"""

from google.cloud import bigquery
import os
from datetime import datetime

# Load environment variables  
from dotenv import load_dotenv
load_dotenv()

def safe_deduplicate():
    """Safely remove only duplicate summary records"""
    
    client = bigquery.Client()
    
    print("🧹 Safe PSA Deduplication - Summary Records Only")
    print("=" * 50)
    
    # Step 1: Identify specific duplicate summary records
    find_duplicates_query = """
    WITH summary_duplicates AS (
      SELECT 
        item_id,
        grade,
        scraped_at,
        ROW_NUMBER() OVER (
          PARTITION BY item_id, grade 
          ORDER BY scraped_at DESC
        ) as rn
      FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
      WHERE record_type = 'summary'
    )
    SELECT COUNT(*) as duplicates_to_remove
    FROM summary_duplicates 
    WHERE rn > 1
    """
    
    result = client.query(find_duplicates_query).to_dataframe()
    duplicates = result['duplicates_to_remove'].iloc[0]
    
    print(f"📊 Found {duplicates} duplicate summary records to remove")
    
    if duplicates == 0:
        print("✅ No duplicates found - data is clean!")
        return
    
    # Step 2: Remove only the older duplicate summary records
    remove_duplicates_query = """
    DELETE FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
    WHERE scraped_at IN (
      SELECT scraped_at
      FROM (
        SELECT 
          scraped_at,
          ROW_NUMBER() OVER (
            PARTITION BY item_id, grade 
            ORDER BY scraped_at DESC
          ) as rn
        FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
        WHERE record_type = 'summary'
      ) ranked
      WHERE rn > 1
    )
    """
    
    print("🧹 Removing duplicate summary records...")
    result = client.query(remove_duplicates_query)
    
    # Get the number of affected rows
    if hasattr(result, 'num_dml_affected_rows') and result.num_dml_affected_rows is not None:
        removed = result.num_dml_affected_rows
        print(f"✅ Removed {removed} duplicate summary records")
    else:
        print("✅ Deduplication completed")
    
    # Step 3: Verify results
    final_query = """
    SELECT 
      COUNT(*) as total_records,
      SUM(CASE WHEN record_type = 'summary' THEN 1 ELSE 0 END) as summary_records,
      SUM(CASE WHEN record_type = 'sale' THEN 1 ELSE 0 END) as sale_records,
      COUNT(DISTINCT item_id) as unique_items
    FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
    """
    
    final_result = client.query(final_query).to_dataframe()
    
    print(f"\n📊 Final Results:")
    print(f"  Total records: {final_result['total_records'].iloc[0]:,}")
    print(f"  Summary records: {final_result['summary_records'].iloc[0]:,}")
    print(f"  Sale records: {final_result['sale_records'].iloc[0]:,}")
    print(f"  Unique items: {final_result['unique_items'].iloc[0]}")
    
    print(f"\n✅ Safe deduplication completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    try:
        safe_deduplicate()
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()