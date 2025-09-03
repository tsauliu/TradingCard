#!/usr/bin/env python3
from ebay_simple_batch import batch_search, jsons_to_excel

# Read updated keywords
with open('keywords_full_dedup.txt', 'r') as f:
    keywords = [line.strip() for line in f if line.strip()]

print(f"Processing {len(keywords)} keywords...")
batch_search(keywords, days=1095)
jsons_to_excel()
print("Done!")