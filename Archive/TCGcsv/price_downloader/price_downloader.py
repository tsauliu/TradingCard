#!/usr/bin/env python3
import requests
import pandas as pd
import json
import os
import subprocess
import shutil
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import glob

class TCGPriceDownloader:
    def __init__(self, max_workers=5):
        self.base_url = "https://tcgcsv.com/archive/tcgplayer"
        self.data_dir = "data"
        self.archive_dir = os.path.join(self.data_dir, "archive")
        self.prices_dir = os.path.join(self.archive_dir, "prices")
        self.max_workers = max_workers
        
        # Create directories
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.archive_dir, exist_ok=True)
        os.makedirs(self.prices_dir, exist_ok=True)
    
    def download_price_archive(self, target_date: str) -> bool:
        """Download price archive for a specific date (YYYY-MM-DD format)"""
        archive_filename = f"prices-{target_date}.ppmd.7z"
        archive_url = f"{self.base_url}/{archive_filename}"
        archive_path = os.path.join(self.archive_dir, archive_filename)
        
        print(f"Downloading price archive for {target_date}...")
        
        try:
            response = requests.get(archive_url, stream=True)
            response.raise_for_status()
            
            with open(archive_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            print(f"Downloaded {archive_filename} ({os.path.getsize(archive_path):,} bytes)")
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"Error downloading {archive_filename}: {e}")
            return False
    
    def extract_price_archive(self, target_date: str) -> bool:
        """Extract price archive using 7z"""
        archive_filename = f"prices-{target_date}.ppmd.7z"
        archive_path = os.path.join(self.archive_dir, archive_filename)
        extract_path = os.path.join(self.prices_dir, target_date)
        
        if not os.path.exists(archive_path):
            print(f"Archive file not found: {archive_path}")
            return False
        
        print(f"Extracting {archive_filename}...")
        
        try:
            # Remove existing extraction directory
            if os.path.exists(extract_path):
                shutil.rmtree(extract_path)
            
            # Extract using 7z
            result = subprocess.run(
                ['7z', 'x', archive_path, f'-o{self.prices_dir}', '-y'],
                capture_output=True,
                text=True,
                check=True
            )
            
            if os.path.exists(extract_path):
                print(f"Extracted to {extract_path}")
                # Count files for verification
                total_files = sum(len(files) for _, _, files in os.walk(extract_path))
                print(f"Extracted {total_files:,} price files")
                return True
            else:
                print(f"Extraction failed - directory not found: {extract_path}")
                return False
                
        except subprocess.CalledProcessError as e:
            print(f"Error extracting {archive_filename}: {e}")
            print(f"7z stderr: {e.stderr}")
            return False
    
    def parse_price_file(self, price_file_path: str, category_id: str, group_id: str, price_date: str) -> List[Dict]:
        """Parse a single price file and return structured data"""
        try:
            with open(price_file_path, 'r') as f:
                price_response = json.load(f)
            
            # Extract the results array from the response
            if isinstance(price_response, dict) and 'results' in price_response:
                price_data = price_response['results']
            elif isinstance(price_response, list):
                price_data = price_response
            else:
                print(f"Unexpected price file format in {price_file_path}")
                return []
            
            parsed_prices = []
            for price_entry in price_data:
                # Handle the price entry structure
                parsed_price = {
                    'price_date': price_date,
                    'product_id': price_entry.get('productId'),
                    'sub_type_name': price_entry.get('subTypeName'),
                    'low_price': price_entry.get('lowPrice'),
                    'mid_price': price_entry.get('midPrice'),
                    'high_price': price_entry.get('highPrice'),
                    'market_price': price_entry.get('marketPrice'),
                    'direct_low_price': price_entry.get('directLowPrice'),
                    'category_id': int(category_id),
                    'group_id': int(group_id),
                    'update_timestamp': datetime.now().isoformat()
                }
                parsed_prices.append(parsed_price)
            
            return parsed_prices
            
        except Exception as e:
            print(f"Error parsing price file {price_file_path}: {e}")
            return []
    
    def process_single_group(self, args) -> tuple:
        """Process prices for a single group - for parallel execution"""
        category_id, group_id, price_date, prices_base_path = args
        
        price_file_path = os.path.join(prices_base_path, price_date, category_id, group_id, 'prices')
        
        if not os.path.exists(price_file_path):
            return 0, f"Price file not found: {price_file_path}"
        
        try:
            parsed_prices = self.parse_price_file(price_file_path, category_id, group_id, price_date)
            return len(parsed_prices), parsed_prices
        except Exception as e:
            return 0, f"Error processing {category_id}/{group_id}: {e}"
    
    def create_price_dataframe(self, target_date: str, limit_categories: int = None, limit_groups_per_category: int = None) -> pd.DataFrame:
        """Create price dataframe from extracted archive"""
        extract_path = os.path.join(self.prices_dir, target_date)
        
        if not os.path.exists(extract_path):
            raise Exception(f"Extracted data not found for {target_date}. Run extract_price_archive first.")
        
        print(f"Processing price data for {target_date}...")
        
        # Get all category directories
        category_dirs = [d for d in os.listdir(extract_path) 
                        if os.path.isdir(os.path.join(extract_path, d)) and d.isdigit()]
        
        if limit_categories:
            category_dirs = category_dirs[:limit_categories]
        
        print(f"Found {len(category_dirs)} categories to process")
        
        # Collect all group processing tasks
        processing_tasks = []
        
        for category_id in category_dirs:
            category_path = os.path.join(extract_path, category_id)
            group_dirs = [d for d in os.listdir(category_path) 
                         if os.path.isdir(os.path.join(category_path, d)) and d.isdigit()]
            
            if limit_groups_per_category:
                group_dirs = group_dirs[:limit_groups_per_category]
            
            for group_id in group_dirs:
                processing_tasks.append((category_id, group_id, target_date, self.prices_dir))
        
        print(f"Processing {len(processing_tasks)} groups across {len(category_dirs)} categories...")
        
        all_prices = []
        total_records = 0
        
        # Process groups in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_task = {executor.submit(self.process_single_group, task): task for task in processing_tasks}
            
            for future in tqdm(as_completed(future_to_task), total=len(processing_tasks), desc="Processing groups"):
                try:
                    count, result = future.result()
                    if isinstance(result, list):  # Success case
                        all_prices.extend(result)
                        total_records += count
                    else:  # Error case
                        if count == 0:  # Only log actual errors, not missing files
                            print(f"  Warning: {result}")
                except Exception as e:
                    task = future_to_task[future]
                    print(f"  Error processing task {task}: {e}")
        
        if not all_prices:
            raise Exception("No price data was processed successfully")
        
        # Create DataFrame
        df = pd.DataFrame(all_prices)
        
        # Convert price_date to proper date format
        df['price_date'] = pd.to_datetime(df['price_date']).dt.date
        df['update_timestamp'] = pd.to_datetime(df['update_timestamp'])
        
        print(f"\nPrice data processing completed!")
        print(f"Total records: {len(df):,}")
        print(f"Categories processed: {len(category_dirs)}")
        print(f"Date: {target_date}")
        
        return df
    
    def download_and_process_date(self, target_date: str, limit_categories: int = None, limit_groups_per_category: int = None) -> pd.DataFrame:
        """Download, extract, and process price data for a specific date"""
        
        # Step 1: Download archive
        if not self.download_price_archive(target_date):
            raise Exception(f"Failed to download archive for {target_date}")
        
        # Step 2: Extract archive
        if not self.extract_price_archive(target_date):
            raise Exception(f"Failed to extract archive for {target_date}")
        
        # Step 3: Process data
        df = self.create_price_dataframe(target_date, limit_categories, limit_groups_per_category)
        
        # Step 4: Save to CSV
        csv_path = os.path.join(self.data_dir, f"tcg_prices_{target_date}.csv")
        df.to_csv(csv_path, index=False)
        print(f"Saved price data to: {csv_path}")
        
        # Step 5: Cleanup archive files to save space
        archive_filename = f"prices-{target_date}.ppmd.7z"
        archive_path = os.path.join(self.archive_dir, archive_filename)
        if os.path.exists(archive_path):
            os.remove(archive_path)
            print(f"Cleaned up archive file: {archive_filename}")
        
        return df
    
    def download_test_data(self, target_date: str = None) -> Dict[str, Any]:
        """Download test price data for a single day with limits"""
        if target_date is None:
            # Use yesterday as default
            target_date = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        print(f"Downloading test price data for {target_date}...")
        
        try:
            df = self.download_and_process_date(
                target_date=target_date,
                limit_categories=2,  # Just 2 categories for testing
                limit_groups_per_category=3  # 3 groups per category
            )
            
            return {
                'target_date': target_date,
                'total_records': len(df),
                'categories_processed': df['category_id'].nunique(),
                'groups_processed': df['group_id'].nunique(),
                'products_processed': df['product_id'].nunique(),
                'csv_path': os.path.join(self.data_dir, f"tcg_prices_{target_date}.csv")
            }
            
        except Exception as e:
            print(f"Error in test download: {e}")
            raise
    
    def download_full_date_data(self, target_date: str = None) -> Dict[str, Any]:
        """Download FULL price data for a single day with NO limits - for performance testing"""
        if target_date is None:
            # Use yesterday as default
            target_date = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        print(f"Downloading FULL price data for {target_date} (NO LIMITS)...")
        print("This will process ALL categories and groups - could take several minutes!")
        
        try:
            import time
            start_time = time.time()
            
            df = self.download_and_process_date(
                target_date=target_date,
                limit_categories=None,  # No category limit
                limit_groups_per_category=None  # No group limit
            )
            
            end_time = time.time()
            processing_time = end_time - start_time
            
            stats = {
                'target_date': target_date,
                'total_records': len(df),
                'categories_processed': df['category_id'].nunique(),
                'groups_processed': df['group_id'].nunique(),
                'products_processed': df['product_id'].nunique(),
                'processing_time_seconds': processing_time,
                'processing_time_minutes': processing_time / 60,
                'records_per_second': len(df) / processing_time if processing_time > 0 else 0,
                'csv_path': os.path.join(self.data_dir, f"tcg_prices_{target_date}.csv")
            }
            
            print(f"\n=== PERFORMANCE RESULTS ===")
            print(f"Total processing time: {processing_time:.2f} seconds ({processing_time/60:.2f} minutes)")
            print(f"Records processed: {len(df):,}")
            print(f"Processing rate: {len(df) / processing_time:.0f} records/second")
            print(f"Categories: {df['category_id'].nunique()}")
            print(f"Groups: {df['group_id'].nunique()}")
            print(f"Unique products: {df['product_id'].nunique()}")
            
            return stats
            
        except Exception as e:
            print(f"Error in full date download: {e}")
            raise
    
    def create_price_dataframe_from_path(self, extract_path: str, target_date: str, 
                                       limit_categories: int = None, limit_groups_per_category: int = None) -> pd.DataFrame:
        """Create price dataframe from a custom extracted path (for robust logger integration)"""
        
        if not os.path.exists(extract_path):
            raise Exception(f"Extracted data not found: {extract_path}")
        
        print(f"Processing price data for {target_date} from {extract_path}...")
        
        # Get all category directories
        category_dirs = [d for d in os.listdir(extract_path) 
                        if os.path.isdir(os.path.join(extract_path, d)) and d.isdigit()]
        
        if limit_categories:
            category_dirs = category_dirs[:limit_categories]
        
        print(f"Found {len(category_dirs)} categories to process")
        
        # Collect all group processing tasks
        processing_tasks = []
        
        for category_id in category_dirs:
            category_path = os.path.join(extract_path, category_id)
            group_dirs = [d for d in os.listdir(category_path) 
                         if os.path.isdir(os.path.join(category_path, d)) and d.isdigit()]
            
            if limit_groups_per_category:
                group_dirs = group_dirs[:limit_groups_per_category]
            
            for group_id in group_dirs:
                # Adjust the base path for the custom structure
                processing_tasks.append((category_id, group_id, target_date, os.path.dirname(extract_path)))
        
        print(f"Processing {len(processing_tasks)} groups across {len(category_dirs)} categories...")
        
        all_prices = []
        total_records = 0
        
        # Process groups in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_task = {executor.submit(self._process_single_group_custom, task): task for task in processing_tasks}
            
            for future in tqdm(as_completed(future_to_task), total=len(processing_tasks), desc="Processing groups"):
                try:
                    count, result = future.result()
                    if isinstance(result, list):  # Success case
                        all_prices.extend(result)
                        total_records += count
                    else:  # Error case
                        if count == 0:  # Only log actual errors, not missing files
                            print(f"  Warning: {result}")
                except Exception as e:
                    task = future_to_task[future]
                    print(f"  Error processing task {task}: {e}")
        
        if not all_prices:
            raise Exception("No price data was processed successfully")
        
        # Create DataFrame
        df = pd.DataFrame(all_prices)
        
        # Convert price_date to proper date format
        df['price_date'] = pd.to_datetime(df['price_date']).dt.date
        df['update_timestamp'] = pd.to_datetime(df['update_timestamp'])
        
        print(f"\nPrice data processing completed!")
        print(f"Total records: {len(df):,}")
        print(f"Categories processed: {len(category_dirs)}")
        print(f"Date: {target_date}")
        
        return df
    
    def _process_single_group_custom(self, args) -> tuple:
        """Process prices for a single group with custom path structure"""
        category_id, group_id, price_date, prices_base_path = args
        
        price_file_path = os.path.join(prices_base_path, price_date, category_id, group_id, 'prices')
        
        if not os.path.exists(price_file_path):
            return 0, f"Price file not found: {price_file_path}"
        
        try:
            parsed_prices = self.parse_price_file(price_file_path, category_id, group_id, price_date)
            return len(parsed_prices), parsed_prices
        except Exception as e:
            return 0, f"Error processing {category_id}/{group_id}: {e}"

if __name__ == "__main__":
    downloader = TCGPriceDownloader()
    
    # Test with a specific date
    test_date = "2024-12-01"  # Use a recent date
    
    try:
        stats = downloader.download_test_data(test_date)
        print(f"\nTest download completed successfully!")
        print(f"Stats: {stats}")
    except Exception as e:
        print(f"Test failed: {e}")