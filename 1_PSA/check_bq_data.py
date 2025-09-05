#!/usr/bin/env python3
"""Check BigQuery data for duplicates and missing dates"""

from google.cloud import bigquery
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'service-account.json'
client = bigquery.Client(project='rising-environs-456314-a3')
table_id = "rising-environs-456314-a3.tcg_data.psa_auction_prices"

print("=" * 60)
print("CHECKING BIGQUERY DATA INTEGRITY")
print("=" * 60)

# 1. Check record counts by date
query = """
SELECT 
    DATE(scraped_at) as scrape_date,
    COUNT(*) as record_count,
    COUNT(DISTINCT CONCAT(item_id, '_', grade)) as unique_card_grades,
    MIN(scraped_at) as earliest_time,
    MAX(scraped_at) as latest_time
FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
GROUP BY scrape_date
ORDER BY scrape_date DESC
"""

print("\n1. RECORDS BY SCRAPE DATE:")
print("-" * 40)
results = client.query(query)
total_records = 0
for row in results:
    total_records += row.record_count
    print(f"Date: {row.scrape_date}")
    print(f"  Records: {row.record_count:,}")
    print(f"  Unique card-grades: {row.unique_card_grades}")
    print(f"  Time range: {row.earliest_time} to {row.latest_time}")
    print()

print(f"TOTAL RECORDS: {total_records:,}")

# 2. Check for exact duplicates (same sale appearing multiple times)
query2 = """
SELECT 
    item_id,
    grade,
    sale_date,
    sale_price,
    record_type,
    COUNT(*) as duplicate_count,
    ARRAY_AGG(DISTINCT DATE(scraped_at)) as scrape_dates
FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
WHERE record_type = 'sale'
GROUP BY item_id, grade, sale_date, sale_price, record_type
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 20
"""

print("\n2. DUPLICATE SALE RECORDS:")
print("-" * 40)
results2 = client.query(query2)
dup_count = 0
for row in results2:
    dup_count += 1
    print(f"Item {row.item_id}, Grade {row.grade}: {row.sale_date} @ ${row.sale_price}")
    print(f"  Appears {row.duplicate_count} times on dates: {row.scrape_dates}")

if dup_count == 0:
    print("No duplicate sales found")
else:
    print(f"\nFound {dup_count} duplicate sale records")

# 3. Check for August 2025 data specifically
query3 = """
SELECT 
    COUNT(*) as aug_records,
    COUNT(DISTINCT item_id) as unique_items,
    COUNT(DISTINCT CONCAT(item_id, '_', grade)) as unique_combos,
    MIN(scraped_at) as earliest,
    MAX(scraped_at) as latest
FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
WHERE DATE(scraped_at) BETWEEN '2025-08-01' AND '2025-08-31'
"""

print("\n3. AUGUST 2025 DATA CHECK:")
print("-" * 40)
results3 = client.query(query3)
for row in results3:
    if row.aug_records > 0:
        print(f"August 2025 records: {row.aug_records:,}")
        print(f"Unique items: {row.unique_items}")
        print(f"Unique card-grade combos: {row.unique_combos}")
        print(f"Time range: {row.earliest} to {row.latest}")
    else:
        print("⚠️  NO AUGUST 2025 DATA FOUND!")

# 4. Check table partitioning
query4 = """
SELECT 
    table_name,
    partition_id,
    total_rows,
    total_logical_bytes
FROM `rising-environs-456314-a3.tcg_data.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'psa_auction_prices'
ORDER BY partition_id DESC
LIMIT 10
"""

print("\n4. TABLE PARTITIONS:")
print("-" * 40)
try:
    results4 = client.query(query4)
    for row in results4:
        print(f"Partition: {row.partition_id}")
        print(f"  Rows: {row.total_rows:,}")
        print(f"  Size: {row.total_logical_bytes / (1024*1024):.2f} MB")
except Exception as e:
    print(f"Could not check partitions: {e}")

# 5. Check for potential overwrites
query5 = """
SELECT 
    item_id,
    grade,
    COUNT(DISTINCT DATE(scraped_at)) as scrape_days,
    COUNT(*) as total_records,
    ARRAY_AGG(DISTINCT DATE(scraped_at) ORDER BY DATE(scraped_at)) as dates
FROM `rising-environs-456314-a3.tcg_data.psa_auction_prices`
WHERE item_id = '544027' AND grade = '10'
GROUP BY item_id, grade
"""

print("\n5. SAMPLE CARD HISTORY (Charizard Grade 10):")
print("-" * 40)
results5 = client.query(query5)
for row in results5:
    print(f"Card {row.item_id} Grade {row.grade}:")
    print(f"  Scraped on {row.scrape_days} different days")
    print(f"  Total records: {row.total_records}")
    print(f"  Dates: {row.dates}")

print("\n" + "=" * 60)