#!/usr/bin/env python3
import sys
import datetime
from ebay_simple_batch import batch_search, jsons_to_excel

def main():
    print(f"Starting batch search at {datetime.datetime.now()}")
    print("="*50)
    
    # Read keywords from file
    with open('keywords_full_dedup.txt', 'r') as f:
        keywords = [line.strip() for line in f if line.strip()]
    
    print(f"Found {len(keywords)} keywords to process")
    print("Keywords:", keywords)
    print("="*50)
    
    # Run batch search with 3 years of weekly data
    print("\nStarting batch search (60s delay between each)...")
    print(f"Estimated time: {len(keywords)} minutes\n")
    
    try:
        batch_search(keywords, days=1095)  # 3 years
        print(f"\nBatch search completed at {datetime.datetime.now()}")
        
        # Convert to Excel
        print("\nConverting to Excel...")
        jsons_to_excel()
        print(f"Excel conversion completed at {datetime.datetime.now()}")
        print("\nAll tasks completed successfully!")
        
    except Exception as e:
        print(f"\nError occurred: {e}")
        print(f"Failed at {datetime.datetime.now()}")
        sys.exit(1)

if __name__ == "__main__":
    main()