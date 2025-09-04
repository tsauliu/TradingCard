#!/usr/bin/env python3
from datetime import datetime
from ebay_simple_batch import batch_search, jsons_to_excel

# Read updated keywords
with open('keywords_full_dedup.txt', 'r') as f:
    keywords = [line.strip() for line in f if line.strip()]

print(f"Starting at {datetime.now()}")
print(f"Processing {len(keywords)} keywords...")

# Batch search - will create YYMMDD_raw_jsons folder
batch_search(keywords, days=1095)

# Convert to Excel - will create YYYYMMDDHHMMSS_ebay_pivot.xlsx
jsons_to_excel()

print(f"Completed at {datetime.now()}")