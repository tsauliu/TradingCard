#!/usr/bin/env python3
import os
import sys
from datetime import date, timedelta
from dotenv import load_dotenv

try:
    from .price_downloader import TCGPriceDownloader
    from .bigquery_price_loader import BigQueryPriceLoader
except ImportError:
    from price_downloader import TCGPriceDownloader
    from bigquery_price_loader import BigQueryPriceLoader

def download_test_prices():
    """Download and load test price data for one day"""
    print("=== TCG Price Data Test Pipeline ===\n")
    
    load_dotenv()
    
    # Step 1: Download test price data
    print("Step 1: Downloading test price data...")
    downloader = TCGPriceDownloader()
    
    # Use a recent date for testing
    test_date = "2024-12-01"
    
    try:
        download_stats = downloader.download_test_data(test_date)
        print(f"Download completed: {download_stats}\n")
    except Exception as e:
        print(f"Failed to download price data: {e}")
        return False
    
    # Step 2: Setup BigQuery connection
    print("Step 2: Setting up BigQuery connection...")
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
    dataset_id = os.getenv('BIGQUERY_DATASET', 'tcg_data')
    
    if not project_id:
        print("Warning: GOOGLE_CLOUD_PROJECT not set in .env file")
        print("BigQuery will use default project from credentials")
    
    loader = BigQueryPriceLoader(project_id=project_id, dataset_id=dataset_id)
    print(f"Connected to BigQuery project: {loader.project_id}")
    print(f"Using dataset: {dataset_id}\n")
    
    # Step 3: Load price data to BigQuery
    print("Step 3: Loading price data to BigQuery...")
    csv_path = download_stats['csv_path']
    
    success = loader.load_price_data(csv_path, price_date=test_date, force_recreate=True)
    
    if success:
        print("\nStep 4: Verifying price data in BigQuery...")
        loader.query_table_info()
        loader.run_sample_queries()
        
        print(f"\n=== Success! ===")
        print(f"Price data loaded to: {loader.project_id}.{dataset_id}.tcg_prices")
        print(f"Test date: {test_date}")
        print(f"Records loaded: {download_stats['total_records']:,}")
        
        print(f"\nSample queries:")
        print(f"1. Daily prices: {loader.get_daily_price_query(test_date)}")
        print(f"2. Product trends: {loader.get_price_trends_query(281940, 7)}")  # Example product ID
        
        print(f"\nNext steps:")
        print("1. Run download_daily_prices() for daily updates")
        print("2. Run download_historical_prices() for backfill")
        print("3. Set up automated daily price updates")
        
        return True
    else:
        print("\n=== Failed to load price data ===")
        return False

def download_daily_prices(target_date: str = None):
    """Download and load price data for a specific date"""
    if target_date is None:
        # Default to yesterday
        target_date = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    print(f"=== TCG Daily Price Update: {target_date} ===\n")
    
    load_dotenv()
    
    # Download price data
    print(f"Downloading price data for {target_date}...")
    downloader = TCGPriceDownloader()
    
    try:
        df = downloader.download_and_process_date(target_date)
        csv_path = os.path.join("data", f"tcg_prices_{target_date}.csv")
        
        print(f"Downloaded {len(df):,} price records")
    except Exception as e:
        print(f"Failed to download price data: {e}")
        return False
    
    # Load to BigQuery
    print(f"Loading to BigQuery...")
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
    dataset_id = os.getenv('BIGQUERY_DATASET', 'tcg_data')
    
    loader = BigQueryPriceLoader(project_id=project_id, dataset_id=dataset_id)
    
    # Check if data already exists for this date
    existing_count = loader.check_existing_data(target_date)
    if existing_count > 0:
        print(f"Found {existing_count:,} existing records for {target_date}")
        print("Replacing existing data...")
        success = loader.replace_date_data(csv_path, target_date)
    else:
        success = loader.load_price_data(csv_path, target_date)
    
    if success:
        print(f"Successfully updated price data for {target_date}")
        return True
    else:
        print(f"Failed to load price data for {target_date}")
        return False

def download_historical_prices(start_date: str, end_date: str = None):
    """Download historical price data for a date range"""
    if end_date is None:
        end_date = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    print(f"=== TCG Historical Price Backfill: {start_date} to {end_date} ===\n")
    
    from datetime import datetime
    
    start = datetime.strptime(start_date, '%Y-%m-%d').date()
    end = datetime.strptime(end_date, '%Y-%m-%d').date()
    
    current_date = start
    success_count = 0
    total_days = (end - start).days + 1
    
    print(f"Will process {total_days} days of price data")
    
    while current_date <= end:
        date_str = current_date.strftime('%Y-%m-%d')
        print(f"\nProcessing {date_str}...")
        
        try:
            success = download_daily_prices(date_str)
            if success:
                success_count += 1
                print(f"  ✓ Success ({success_count}/{total_days})")
            else:
                print(f"  ✗ Failed")
        except Exception as e:
            print(f"  ✗ Error: {e}")
        
        current_date += timedelta(days=1)
    
    print(f"\n=== Historical Backfill Complete ===")
    print(f"Successful: {success_count}/{total_days} days")
    print(f"Failed: {total_days - success_count} days")

def main():
    """Main function with command options"""
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python3 main_price.py test              # Download and test with 1 day")
        print("  python3 main_price.py daily [YYYY-MM-DD] # Download daily prices")
        print("  python3 main_price.py backfill START_DATE [END_DATE] # Historical backfill")
        return
    
    command = sys.argv[1]
    
    if command == "test":
        download_test_prices()
    elif command == "daily":
        target_date = sys.argv[2] if len(sys.argv) > 2 else None
        download_daily_prices(target_date)
    elif command == "backfill":
        if len(sys.argv) < 3:
            print("Error: backfill requires start date")
            print("Usage: python3 main_price.py backfill 2024-02-08 [2024-12-01]")
            return
        
        start_date = sys.argv[2]
        end_date = sys.argv[3] if len(sys.argv) > 3 else None
        download_historical_prices(start_date, end_date)
    else:
        print(f"Unknown command: {command}")
        print("Available commands: test, daily, backfill")

if __name__ == "__main__":
    main()