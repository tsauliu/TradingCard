#!/usr/bin/env python3
"""Deduplicate BigQuery PSA auction data"""

from google.cloud import bigquery
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'service-account.json'
client = bigquery.Client(project='rising-environs-456314-a3')
dataset_id = "rising-environs-456314-a3.tcg_data"
table_id = f"{dataset_id}.psa_auction_prices"
dedupe_table_id = f"{dataset_id}.psa_auction_prices_deduped"

print("=" * 60)
print("DEDUPLICATING PSA AUCTION DATA")
print("=" * 60)

# 1. Check current duplicate situation
query_check = """
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT CONCAT(
        IFNULL(item_id, ''),
        '_', IFNULL(grade, ''),
        '_', IFNULL(record_type, ''),
        '_', IFNULL(CAST(sale_date AS STRING), ''),
        '_', IFNULL(CAST(sale_price AS STRING), ''),
        '_', IFNULL(CAST(total_sales_count AS STRING), '')
    )) as unique_records
FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
"""

result = client.query(query_check).result()
for row in result:
    print(f"\nCurrent status:")
    print(f"  Total records: {row.total_records:,}")
    print(f"  Unique records: {row.unique_records:,}")
    print(f"  Duplicates to remove: {row.total_records - row.unique_records:,}")
    duplicate_pct = ((row.total_records - row.unique_records) / row.total_records) * 100
    print(f"  Duplication rate: {duplicate_pct:.1f}%")

# 2. Create deduplicated table
print("\nCreating deduplicated table...")

# For summary records: keep the most recent scraped_at for each unique combination
# For sale records: keep the most recent scraped_at for each unique sale
dedupe_query = f"""
CREATE OR REPLACE TABLE `{dedupe_table_id}` 
PARTITION BY DATE(scraped_at)
AS
WITH ranked_records AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                item_id,
                grade,
                record_type,
                IFNULL(sale_date, 'NULL'),
                IFNULL(CAST(sale_price AS STRING), 'NULL'),
                IFNULL(CAST(total_sales_count AS STRING), 'NULL')
            ORDER BY scraped_at DESC
        ) as rn
    FROM `{table_id}`
)
SELECT 
    item_id,
    grade,
    grade_label,
    record_type,
    total_sales_count,
    average_price,
    median_price,
    min_price,
    max_price,
    std_deviation,
    date_range_start,
    date_range_end,
    sale_date,
    sale_price,
    scraped_at,
    data_source,
    card_name,
    card_set,
    card_year,
    card_variant,
    psa_url,
    lifecycle_sales_count
FROM ranked_records
WHERE rn = 1
"""

print("Executing deduplication query...")
job = client.query(dedupe_query)
job.result()  # Wait for query to complete
print("✓ Deduplicated table created")

# 3. Verify deduplication
verify_query = f"""
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT item_id) as unique_items,
    COUNT(DISTINCT CONCAT(item_id, '_', grade)) as unique_card_grades,
    SUM(CASE WHEN record_type = 'summary' THEN 1 ELSE 0 END) as summary_records,
    SUM(CASE WHEN record_type = 'sale' THEN 1 ELSE 0 END) as sale_records
FROM `{dedupe_table_id}`
"""

print("\nDeduplicated table statistics:")
print("-" * 40)
result = client.query(verify_query).result()
for row in result:
    print(f"Total records: {row.total_records:,}")
    print(f"Unique items: {row.unique_items}")
    print(f"Unique card-grade combinations: {row.unique_card_grades}")
    print(f"Summary records: {row.summary_records:,}")
    print(f"Sale records: {row.sale_records:,}")

# 4. Check dates preserved
dates_query = f"""
SELECT 
    DATE(scraped_at) as scrape_date,
    COUNT(*) as record_count
FROM `{dedupe_table_id}`
GROUP BY scrape_date
ORDER BY scrape_date DESC
"""

print("\nRecords by date after deduplication:")
print("-" * 40)
result = client.query(dates_query).result()
for row in result:
    print(f"{row.scrape_date}: {row.record_count:,} records")

# 5. Sample verification - Charizard Grade 10
sample_query = f"""
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT sale_date) as unique_sales
FROM `{dedupe_table_id}`
WHERE item_id = '544027' 
    AND grade = '10' 
    AND record_type = 'sale'
"""

print("\nSample verification (Charizard Grade 10 sales):")
print("-" * 40)
result = client.query(sample_query).result()
for row in result:
    print(f"Total sale records: {row.total_records}")
    print(f"Unique sale dates: {row.unique_sales}")

print("\n" + "=" * 60)
print("DEDUPLICATION COMPLETE!")
print(f"New table: {dedupe_table_id}")
print("\nTo replace the original table, run:")
print(f"  1. RENAME original: psa_auction_prices → psa_auction_prices_backup")
print(f"  2. RENAME deduped: psa_auction_prices_deduped → psa_auction_prices")
print("=" * 60)