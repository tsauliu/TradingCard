#!/usr/bin/env python3
"""
BigQuery Backup Table Cleanup Script
Safely removes backup tables created during deduplication
"""

from google.cloud import bigquery
from dotenv import load_dotenv
load_dotenv()

def cleanup_backup_tables():
    """Remove PSA backup tables after confirming main table is intact"""
    
    client = bigquery.Client()
    
    print("🧹 PSA BigQuery Backup Cleanup")
    print("=" * 40)
    
    # First verify main table is intact
    main_check = """
    SELECT COUNT(*) as record_count
    FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
    """
    
    main_count = list(client.query(main_check))[0].record_count
    print(f"✅ Main table has {main_count:,} records")
    
    if main_count < 25000:
        print("⚠️ Main table seems to have too few records. Aborting cleanup for safety.")
        return
    
    # Find backup tables
    backup_query = """
    SELECT table_name
    FROM `rising-environs-456314-a3.tcg_data.INFORMATION_SCHEMA.TABLES`
    WHERE table_name LIKE 'psa_auction_prices_backup_%'
    ORDER BY creation_time ASC
    """
    
    backup_tables = [row.table_name for row in client.query(backup_query)]
    
    if not backup_tables:
        print("✅ No backup tables found to clean up")
        return
    
    print(f"\n🗂️ Found {len(backup_tables)} backup tables:")
    for table in backup_tables:
        print(f"  📦 {table}")
    
    # Confirm deletion
    response = input(f"\n❓ Delete these {len(backup_tables)} backup tables? (y/N): ").strip().lower()
    
    if response != 'y':
        print("❌ Cleanup cancelled")
        return
    
    # Delete backup tables
    deleted_count = 0
    for table in backup_tables:
        try:
            client.delete_table(f"rising-environs-456314-a3.tcg_data.{table}")
            print(f"✅ Deleted: {table}")
            deleted_count += 1
        except Exception as e:
            print(f"❌ Failed to delete {table}: {e}")
    
    print(f"\n🎯 Cleanup completed: {deleted_count}/{len(backup_tables)} tables deleted")
    
    # Final verification
    final_query = """
    SELECT table_name
    FROM `rising-environs-456314-a3.tcg_data.INFORMATION_SCHEMA.TABLES`
    WHERE table_name LIKE '%psa_auction_prices%'
    ORDER BY table_name
    """
    
    remaining_tables = [row.table_name for row in client.query(final_query)]
    
    print(f"\n📊 Remaining PSA tables:")
    for table in remaining_tables:
        table_type = "Main" if table == "psa_auction_prices" else "Backup"
        print(f"  {'✅' if table_type == 'Main' else '📦'} {table} ({table_type})")

if __name__ == "__main__":
    try:
        cleanup_backup_tables()
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()