#!/usr/bin/env python3
"""
Verify the data loaded correctly in BigQuery
"""

import os
from google.cloud import bigquery
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def verify_bigquery_data():
    """Query BigQuery to verify the data was loaded correctly"""
    PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
    DATASET_ID = os.getenv("BIGQUERY_DATASET", "tcg_data")
    TABLE_ID = "tcg_prices_bda"
    
    if not PROJECT_ID:
        print("Error: GOOGLE_CLOUD_PROJECT environment variable not set")
        return
    
    # Initialize BigQuery client
    client = bigquery.Client(project=PROJECT_ID)
    
    table_name = f"`{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"
    
    print(f"Verifying data in {table_name}")
    print("=" * 60)
    
    # Query 1: Count total records
    query1 = f"""
    SELECT COUNT(*) as total_records
    FROM {table_name}
    """
    
    result1 = client.query(query1).result()
    for row in result1:
        print(f"✓ Total records: {row.total_records}")
    
    # Query 2: Count records per product_id
    query2 = f"""
    SELECT 
        product_id,
        COUNT(*) as record_count,
        COUNT(DISTINCT sku_id) as unique_skus
    FROM {table_name}
    GROUP BY product_id
    ORDER BY product_id
    """
    
    print("\n📊 Records per product ID:")
    result2 = client.query(query2).result()
    for row in result2:
        print(f"  Product {row.product_id}: {row.record_count} records, {row.unique_skus} SKUs")
    
    # Query 3: Sample data from each product
    query3 = f"""
    SELECT 
        product_id,
        sku_id,
        variant,
        language,
        condition,
        market_price,
        bucket_start_date
    FROM {table_name}
    WHERE bucket_start_date = (
        SELECT MAX(bucket_start_date) 
        FROM {table_name} t2 
        WHERE t2.product_id = {table_name}.product_id
    )
    ORDER BY product_id
    """
    
    print("\n📋 Latest price data per product:")
    result3 = client.query(query3).result()
    for row in result3:
        print(f"  Product {row.product_id} (SKU: {row.sku_id})")
        print(f"    {row.variant} | {row.language} | {row.condition}")
        print(f"    Latest price: ${row.market_price} on {row.bucket_start_date}")
    
    # Query 4: Data quality checks
    query4 = f"""
    SELECT 
        'Missing product_id' as check_type,
        COUNT(*) as count
    FROM {table_name}
    WHERE product_id IS NULL
    
    UNION ALL
    
    SELECT 
        'Missing market_price' as check_type,
        COUNT(*) as count
    FROM {table_name}
    WHERE market_price IS NULL
    
    UNION ALL
    
    SELECT 
        'Invalid dates' as check_type,
        COUNT(*) as count
    FROM {table_name}
    WHERE bucket_start_date IS NULL
    """
    
    print("\n🔍 Data quality checks:")
    result4 = client.query(query4).result()
    for row in result4:
        status = "✓" if row.count == 0 else "⚠️"
        print(f"  {status} {row.check_type}: {row.count}")
    
    # Query 5: Schema verification
    query5 = f"""
    SELECT 
        column_name,
        data_type,
        is_nullable
    FROM {PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = '{TABLE_ID}'
    ORDER BY ordinal_position
    """
    
    print("\n📋 Table schema:")
    result5 = client.query(query5).result()
    for row in result5:
        nullable = "NULL" if row.is_nullable == "YES" else "NOT NULL"
        print(f"  {row.column_name}: {row.data_type} ({nullable})")

def main():
    """Main verification function"""
    try:
        verify_bigquery_data()
        print("\n✅ Data verification completed!")
    except Exception as e:
        print(f"❌ Error during verification: {e}")
        raise

if __name__ == "__main__":
    main()