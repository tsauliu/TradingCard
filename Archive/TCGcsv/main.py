#!/usr/bin/env python3
import os
import sys
from dotenv import load_dotenv
from product_downloader.downloader import TCGCSVDownloader
from product_downloader.bigquery_loader import BigQueryLoader

def load_products():
    """Load TCG product catalog data"""
    print("=== TCG CSV to BigQuery - Product Catalog ===\n")
    
    print("Step 1: Downloading test data from TCGcsv.com...")
    downloader = TCGCSVDownloader()
    download_stats = downloader.download_test_data()
    print(f"Download completed: {download_stats}\n")
    
    print("Step 2: Setting up BigQuery connection...")
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
    dataset_id = os.getenv('BIGQUERY_DATASET', 'tcg_data')
    
    if not project_id:
        print("Warning: GOOGLE_CLOUD_PROJECT not set in .env file")
        print("BigQuery will use default project from credentials")
    
    loader = BigQueryLoader(project_id=project_id, dataset_id=dataset_id)
    print(f"Connected to BigQuery project: {loader.project_id}")
    print(f"Using dataset: {dataset_id}\n")
    
    print("Step 3: Loading denormalized TCG products to BigQuery...")
    success = loader.load_tcg_products()
    
    if success:
        print("\nStep 4: Verifying data in BigQuery...")
        loader.query_table_info()
        loader.run_sample_queries()
        
        print(f"\n=== Success! ===")
        print(f"Data loaded to: {loader.project_id}.{dataset_id}.{loader.table_name}")
        print(f"\nFor bi-weekly updates, use this query to get latest data:")
        print(loader.get_latest_data_query())
        
        print(f"\nNext steps:")
        print("1. Set up automated bi-weekly data refresh")
        print("2. Configure data validation rules")
        print("3. Set up monitoring and alerting")
        print("4. Scale to full dataset (remove limits in downloader)")
        return True
    else:
        print("\n=== Failed to load data ===")
        print("Check the error messages above for details")
        return False

def load_prices():
    """Load TCG price data"""
    from price_downloader.main_price import download_test_prices
    return download_test_prices()

def main():
    """Main function with options for products or prices"""
    load_dotenv()
    
    if len(sys.argv) < 2:
        print("=== TCG Data Pipeline ===")
        print("\nUsage:")
        print("  python3 main.py products    # Load product catalog data")
        print("  python3 main.py prices      # Load price data (test)")
        print("\nFor more price options, use:")
        print("  python3 price_downloader/main_price.py test              # Test price pipeline")
        print("  python3 price_downloader/main_price.py daily [date]      # Daily price update")
        print("  python3 price_downloader/main_price.py backfill start end # Historical backfill")
        return
    
    command = sys.argv[1]
    
    if command == "products":
        load_products()
    elif command == "prices":
        load_prices()
    else:
        print(f"Unknown command: {command}")
        print("Available commands: products, prices")

if __name__ == "__main__":
    main()