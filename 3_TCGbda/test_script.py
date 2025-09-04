#!/usr/bin/env python3
"""
Test script to validate the JSON processing logic before uploading to BigQuery
"""

import json
from pathlib import Path
from load_to_bigquery import extract_product_id, process_json_file

def test_product_id_extraction():
    """Test product ID extraction from filenames"""
    test_cases = [
        ("481225.0.json", "481225"),
        ("100495.0.json", "100495"),
        ("42349.0.json", "42349")
    ]
    
    print("Testing product ID extraction...")
    for filename, expected in test_cases:
        try:
            result = extract_product_id(filename)
            status = "✓" if result == expected else "✗"
            print(f"{status} {filename} -> {result} (expected: {expected})")
        except Exception as e:
            print(f"✗ {filename} -> Error: {e}")

def test_json_processing():
    """Test JSON file processing with sample files"""
    test_files = [
        "481225.0.json",
        "100495.0.json", 
        "42349.0.json"
    ]
    
    json_dir = Path("./product_details")
    
    print("\nTesting JSON file processing...")
    for filename in test_files:
        file_path = json_dir / filename
        if file_path.exists():
            try:
                records = process_json_file(file_path)
                print(f"✓ {filename}: Processed {len(records)} records")
                
                # Show sample record structure
                if records:
                    print(f"  Sample record keys: {list(records[0].keys())}")
                    print(f"  Product ID: {records[0]['product_id']}")
                    print(f"  SKU ID: {records[0]['sku_id']}")
                    print(f"  Sample market price: {records[0]['market_price']}")
                
            except Exception as e:
                print(f"✗ {filename}: Error - {e}")
        else:
            print(f"✗ {filename}: File not found")

def analyze_data_structure():
    """Analyze the structure of processed data"""
    json_dir = Path("./product_details")
    sample_file = json_dir / "481225.0.json"
    
    if sample_file.exists():
        print("\nData structure analysis:")
        records = process_json_file(sample_file)
        
        if records:
            sample_record = records[0]
            print(f"Total fields: {len(sample_record)}")
            print("Field types:")
            for key, value in sample_record.items():
                print(f"  {key}: {type(value).__name__} = {value}")

def main():
    """Run all tests"""
    print("=" * 50)
    print("BigQuery Loader Test Suite")
    print("=" * 50)
    
    test_product_id_extraction()
    test_json_processing()
    analyze_data_structure()
    
    print("\n" + "=" * 50)
    print("Test completed!")

if __name__ == "__main__":
    main()