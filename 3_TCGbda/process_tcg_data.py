#!/usr/bin/env python3
"""
TCG Data Processor - Analyzes JSON files and uploads to BigQuery with deduplication tracking
"""

import json
import os
import re
import logging
import sys
import zipfile
import shutil
import psutil
import gc
import csv
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Generator, Set
from datetime import datetime, timezone, date
import time

import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, WriteDisposition
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f'/logs/tcg_upload_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class TCGDataProcessor:
    """Main class for processing TCG data and uploading to BigQuery with deduplication"""
    
    def __init__(self, project_id: str = None, dataset_id: str = "tcg_data", 
                 table_id: str = "tcg_prices_bda", json_directory: str = "./product_details",
                 upload_directory: str = None, batch_size: int = 1000, 
                 max_memory_mb: int = 1024, tracking_csv: str = "uploaded_files_tracker.csv"):
        """Initialize the processor with BigQuery configuration
        
        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
            json_directory: Directory containing JSON files to process
            upload_directory: Directory to scan for ZIP files (default: ~/fileuploader/uploads)
        """
        self.project_id = project_id or os.getenv("GOOGLE_CLOUD_PROJECT")
        self.dataset_id = dataset_id or os.getenv("BIGQUERY_DATASET", "tcg_data")
        self.table_id = table_id
        self.json_directory = json_directory
        self.upload_directory = Path(upload_directory or "~/fileuploader/uploads").expanduser()
        self.batch_size = batch_size  # Number of files to process per batch
        self.max_memory_mb = max_memory_mb  # Maximum memory threshold in MB
        self.tracking_csv = tracking_csv  # CSV file to track uploaded files
        self.uploaded_files = self._load_uploaded_files()  # Set of already uploaded files
        self.current_scrape_date = None  # Will be set when processing a ZIP
        
        # Validate configuration
        if not self.project_id:
            raise ValueError("GOOGLE_CLOUD_PROJECT environment variable not set")
        
        # Don't validate json_directory yet - it might be created from ZIP extraction
        
        logger.info(f"Configuration: Project={self.project_id}, Dataset={self.dataset_id}, Table={self.table_id}")
        logger.info(f"JSON Directory: {self.json_directory}")
        logger.info(f"Upload Directory: {self.upload_directory}")
        logger.info(f"Batch Size: {self.batch_size} files, Max Memory: {self.max_memory_mb} MB")
        logger.info(f"Tracking CSV: {self.tracking_csv}")
        logger.info(f"Previously uploaded files: {len(self.uploaded_files)}")
        
        # Initialize BigQuery client
        self.client = bigquery.Client(project=self.project_id)
        self.table_ref = self.client.dataset(self.dataset_id).table(self.table_id)
        
        # Initialize or verify table
        self._setup_table()
    
    def _setup_table(self):
        """Create BigQuery table if it doesn't exist"""
        try:
            table = self.client.get_table(self.table_ref)
            logger.info(f"Table {self.table_id} exists with {table.num_rows:,} rows")
        except Exception:
            schema = self._get_table_schema()
            table = bigquery.Table(self.table_ref, schema=schema)
            table = self.client.create_table(table)
            logger.info(f"Created table {self.table_id}")
    
    @staticmethod
    def _get_table_schema():
        """Define BigQuery table schema"""
        return [
            bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("sku_id", "STRING"),
            bigquery.SchemaField("variant", "STRING"),
            bigquery.SchemaField("language", "STRING"),
            bigquery.SchemaField("condition", "STRING"),
            bigquery.SchemaField("average_daily_quantity_sold", "STRING"),
            bigquery.SchemaField("average_daily_transaction_count", "STRING"),
            bigquery.SchemaField("total_quantity_sold", "STRING"),
            bigquery.SchemaField("total_transaction_count", "STRING"),
            bigquery.SchemaField("bucket_start_date", "DATE"),
            bigquery.SchemaField("market_price", "FLOAT"),
            bigquery.SchemaField("quantity_sold", "INTEGER"),
            bigquery.SchemaField("low_sale_price", "FLOAT"),
            bigquery.SchemaField("low_sale_price_with_shipping", "FLOAT"),
            bigquery.SchemaField("high_sale_price", "FLOAT"),
            bigquery.SchemaField("high_sale_price_with_shipping", "FLOAT"),
            bigquery.SchemaField("transaction_count", "INTEGER"),
            bigquery.SchemaField("scrape_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("source_file", "STRING", mode="NULLABLE")
        ]
    
    @staticmethod
    def _extract_product_id(filename: str) -> str:
        """Extract product ID from filename (e.g., '481225.0.json' -> '481225')"""
        match = re.match(r'^(\d+)(?:\.0)?\.json$', filename)
        if match:
            return match.group(1)
        raise ValueError(f"Invalid filename format: {filename}")
    
    def _load_uploaded_files(self) -> Set[str]:
        """Load set of already uploaded files from tracking CSV
        
        Returns:
            Set of file paths that have been uploaded
        """
        uploaded = set()
        if Path(self.tracking_csv).exists():
            try:
                with open(self.tracking_csv, 'r', newline='') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        # Store as tuple of (filepath, scrape_date) for unique identification
                        uploaded.add((row['filepath'], row['scrape_date']))
                logger.info(f"Loaded {len(uploaded)} previously uploaded files from tracker")
            except Exception as e:
                logger.warning(f"Error loading tracking CSV: {e}")
        else:
            logger.info("No existing tracking CSV found - starting fresh")
        return uploaded
    
    def _save_uploaded_file(self, filepath: str, scrape_date: str, record_count: int):
        """Save uploaded file info to tracking CSV
        
        Args:
            filepath: Path to the uploaded file
            scrape_date: Scrape date from ZIP filename
            record_count: Number of records uploaded from this file
        """
        file_exists = Path(self.tracking_csv).exists()
        
        try:
            with open(self.tracking_csv, 'a', newline='') as f:
                fieldnames = ['filepath', 'scrape_date', 'upload_timestamp', 'record_count']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                
                # Write header if file is new
                if not file_exists:
                    writer.writeheader()
                
                writer.writerow({
                    'filepath': filepath,
                    'scrape_date': scrape_date,
                    'upload_timestamp': datetime.now(timezone.utc).isoformat(),
                    'record_count': record_count
                })
                
            # Add to in-memory set
            self.uploaded_files.add((filepath, scrape_date))
            
        except Exception as e:
            logger.error(f"Failed to save to tracking CSV: {e}")
    
    def _is_file_uploaded(self, filepath: str, scrape_date: str) -> bool:
        """Check if a file has already been uploaded
        
        Args:
            filepath: Path to check
            scrape_date: Scrape date for this batch
            
        Returns:
            True if file was already uploaded, False otherwise
        """
        return (filepath, scrape_date) in self.uploaded_files
    
    @staticmethod
    def _extract_date_from_zip_name(zip_path: Path) -> Optional[str]:
        """Extract date from ZIP filename
        
        Expected formats:
        - tcg_data_2025-01-15.zip -> 2025-01-15
        - data_20250115.zip -> 2025-01-15
        - product_details_2025-01.zip -> 2025-01-01 (assumes first of month)
        
        Args:
            zip_path: Path to ZIP file
            
        Returns:
            Date string in YYYY-MM-DD format or None if no date found
        """
        filename = zip_path.stem  # Remove .zip extension
        
        # Try different date patterns
        patterns = [
            (r'(\d{4})-(\d{2})-(\d{2})', lambda m: f"{m.group(1)}-{m.group(2)}-{m.group(3)}"),  # YYYY-MM-DD
            (r'(\d{4})(\d{2})(\d{2})', lambda m: f"{m.group(1)}-{m.group(2)}-{m.group(3)}"),      # YYYYMMDD
            (r'(\d{4})-(\d{2})', lambda m: f"{m.group(1)}-{m.group(2)}-01"),                       # YYYY-MM
            (r'(\d{4})_(\d{2})_(\d{2})', lambda m: f"{m.group(1)}-{m.group(2)}-{m.group(3)}"),    # YYYY_MM_DD
        ]
        
        for pattern, formatter in patterns:
            match = re.search(pattern, filename)
            if match:
                try:
                    date_str = formatter(match)
                    # Validate it's a real date
                    datetime.strptime(date_str, "%Y-%m-%d")
                    logger.info(f"Extracted scrape date from ZIP name: {date_str}")
                    return date_str
                except ValueError:
                    continue
        
        # If no date found in filename, use file modification time
        logger.warning(f"No date pattern found in ZIP filename: {filename}")
        logger.info("Using ZIP file modification time as scrape date")
        mod_time = datetime.fromtimestamp(zip_path.stat().st_mtime)
        return mod_time.strftime("%Y-%m-%d")
    
    def find_latest_zip(self) -> Optional[Path]:
        """Find the most recent ZIP file in the upload directory
        
        Returns:
            Path to the most recent ZIP file, or None if no ZIP files found
        """
        if not self.upload_directory.exists():
            logger.warning(f"Upload directory does not exist: {self.upload_directory}")
            return None
        
        zip_files = list(self.upload_directory.glob("*.zip"))
        
        if not zip_files:
            logger.warning(f"No ZIP files found in {self.upload_directory}")
            return None
        
        # Sort by modification time (most recent first)
        zip_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        latest_zip = zip_files[0]
        
        logger.info(f"Found {len(zip_files)} ZIP file(s)")
        logger.info(f"Most recent: {latest_zip.name} (modified: {datetime.fromtimestamp(latest_zip.stat().st_mtime)})")
        
        return latest_zip
    
    def copy_and_extract_zip(self, zip_path: Path) -> Tuple[bool, str]:
        """Copy ZIP file from upload directory and extract with preserved structure
        
        Args:
            zip_path: Path to the ZIP file to copy and extract
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        if not zip_path.exists():
            return False, f"ZIP file does not exist: {zip_path}"
        
        if not zipfile.is_zipfile(zip_path):
            return False, f"File is not a valid ZIP: {zip_path}"
        
        # Extract scrape date from ZIP filename
        self.current_scrape_date = self._extract_date_from_zip_name(zip_path)
        logger.info(f"Processing ZIP with scrape date: {self.current_scrape_date}")
        
        # Create extraction directory based on ZIP name and scrape date
        extract_dir = Path(self.json_directory) / f"{zip_path.stem}_{self.current_scrape_date.replace('-', '')}"
        
        try:
            # Create target directory if it doesn't exist
            extract_dir.mkdir(parents=True, exist_ok=True)
            
            # Copy ZIP file to working directory for backup
            backup_dir = Path("./zip_backups")
            backup_dir.mkdir(exist_ok=True)
            backup_path = backup_dir / f"{zip_path.stem}_{self.current_scrape_date.replace('-', '')}.zip"
            
            if not backup_path.exists():
                logger.info(f"Copying ZIP file to backup: {backup_path}")
                shutil.copy2(zip_path, backup_path)
            else:
                logger.info(f"Backup already exists: {backup_path}")
            
            # Extract the ZIP file preserving structure
            logger.info(f"Extracting {zip_path.name} to {extract_dir}")
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Get info about ZIP contents
                file_list = zip_ref.namelist()
                json_files = [f for f in file_list if f.endswith('.json')]
                
                logger.info(f"ZIP contains {len(file_list)} files, {len(json_files)} JSON files")
                
                # Extract all files preserving structure
                zip_ref.extractall(extract_dir)
            
            # Count extracted files
            extracted_json_files = list(extract_dir.glob("**/*.json"))
            logger.info(f"Successfully extracted {len(extracted_json_files)} JSON files with preserved structure")
            logger.info(f"Extraction directory: {extract_dir}")
            
            # Update json_directory to point to the extracted location
            self.json_directory = str(extract_dir)
            
            return True, f"Extracted {len(extracted_json_files)} JSON files to {extract_dir}"
            
        except zipfile.BadZipFile as e:
            return False, f"Corrupt ZIP file: {e}"
        except Exception as e:
            return False, f"Extraction failed: {e}"
    
    def process_latest_zip(self) -> Tuple[bool, str]:
        """Find and process the latest ZIP file from upload directory
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        logger.info("="*60)
        logger.info("Looking for latest ZIP file to process")
        logger.info("="*60)
        
        # Find latest ZIP
        latest_zip = self.find_latest_zip()
        if not latest_zip:
            return False, "No ZIP files found to process"
        
        # Copy and extract ZIP with preserved structure
        success, message = self.copy_and_extract_zip(latest_zip)
        if not success:
            logger.error(f"Failed to extract ZIP: {message}")
            return False, message
        
        logger.info(message)
        return True, f"Ready to process data from {latest_zip.name} with scrape_date={self.current_scrape_date}"
    
    def _process_json_file(self, file_path: Path, add_metadata: bool = False) -> List[Dict[str, Any]]:
        """Process a single JSON file and return flattened records
        
        Args:
            file_path: Path to the JSON file
            add_metadata: Whether to add source file metadata
        """
        # Skip summary files by default
        if "_summary.json" in file_path.name:
            logger.debug(f"Skipping summary file: {file_path.name}")
            return []
        
        try:
            product_id = self._extract_product_id(file_path.name)
        except ValueError as e:
            logger.debug(f"Skipping non-product file {file_path.name}: {e}")
            return []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning(f"Invalid JSON file {file_path.name}: {e}")
            return []
        
        records = []
        results = data.get('result', [])
        
        for result in results:
            # Base product information
            base_info = {
                'product_id': product_id,
                'sku_id': result.get('skuId'),
                'variant': result.get('variant'),
                'language': result.get('language'),
                'condition': result.get('condition'),
                'average_daily_quantity_sold': result.get('averageDailyQuantitySold'),
                'average_daily_transaction_count': result.get('averageDailyTransactionCount'),
                'total_quantity_sold': result.get('totalQuantitySold'),
                'total_transaction_count': result.get('totalTransactionCount'),
                'scrape_date': self.current_scrape_date  # Use scrape_date from ZIP
            }
            
            # Add source metadata
            if add_metadata:
                base_info['source_file'] = str(file_path.relative_to(Path(self.json_directory)))
            else:
                base_info['source_file'] = None
            
            # Process price history buckets
            buckets = result.get('buckets', [])
            if not buckets:
                records.append(base_info)
            else:
                for bucket in buckets:
                    record = base_info.copy()
                    record.update({
                        'bucket_start_date': bucket.get('bucketStartDate'),
                        'market_price': bucket.get('marketPrice'),
                        'quantity_sold': bucket.get('quantitySold'),
                        'low_sale_price': bucket.get('lowSalePrice'),
                        'low_sale_price_with_shipping': bucket.get('lowSalePriceWithShipping'),
                        'high_sale_price': bucket.get('highSalePrice'),
                        'high_sale_price_with_shipping': bucket.get('highSalePriceWithShipping'),
                        'transaction_count': bucket.get('transactionCount')
                    })
                    records.append(record)
        
        return records
    
    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB"""
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024
    
    def _file_batch_generator(self, json_files: List[Path], batch_size: int) -> Generator[List[Path], None, None]:
        """Generate batches of files for processing
        
        Args:
            json_files: List of JSON file paths
            batch_size: Number of files per batch
        
        Yields:
            Batches of file paths
        """
        for i in range(0, len(json_files), batch_size):
            yield json_files[i:i + batch_size]
    
    def _process_batch(self, file_batch: List[Path], add_metadata: bool = False) -> Tuple[pd.DataFrame, List[Tuple[Path, int]]]:
        """Process a batch of JSON files and return a DataFrame
        
        Args:
            file_batch: List of file paths to process
            add_metadata: Whether to add source file metadata
        
        Returns:
            Tuple of (DataFrame with processed records, List of (filepath, record_count))
        """
        batch_records = []
        processed_files = []  # Track files and their record counts
        
        for json_file in file_batch:
            # Check if file was already uploaded for this scrape_date
            file_key = str(json_file.relative_to(Path(self.json_directory)))
            
            if self._is_file_uploaded(file_key, self.current_scrape_date):
                logger.debug(f"Skipping already uploaded file: {file_key}")
                continue
            
            try:
                records = self._process_json_file(json_file, add_metadata=add_metadata)
                if records:
                    batch_records.extend(records)
                    processed_files.append((file_key, len(records)))
            except Exception as e:
                logger.debug(f"Error processing {json_file.name}: {e}")
                continue
        
        if not batch_records:
            return pd.DataFrame(), processed_files
        
        # Convert to DataFrame and clean data
        df = pd.DataFrame(batch_records)
        
        # Data type conversions
        numeric_columns = ['market_price', 'low_sale_price', 'low_sale_price_with_shipping',
                          'high_sale_price', 'high_sale_price_with_shipping']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        integer_columns = ['quantity_sold', 'transaction_count']
        for col in integer_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        
        # Convert dates
        if 'bucket_start_date' in df.columns:
            df['bucket_start_date'] = pd.to_datetime(df['bucket_start_date'], errors='coerce')
        
        if 'scrape_date' in df.columns:
            df['scrape_date'] = pd.to_datetime(df['scrape_date'], errors='coerce')
        
        return df, processed_files
    
    def process_all_files_batched(self, recursive: bool = True, add_metadata: bool = False,
                                  mode: str = "replace") -> int:
        """Process JSON files in batches and upload incrementally to BigQuery
        
        Args:
            recursive: Whether to scan subdirectories recursively
            add_metadata: Whether to add source file metadata
            mode: Upload mode - 'replace' or 'append'
        
        Returns:
            Total number of records processed
        """
        json_path = Path(self.json_directory)
        
        # Check if directory exists
        if not json_path.exists():
            logger.error(f"JSON directory does not exist: {json_path}")
            return 0
        
        # Get JSON files (recursive or flat)
        if recursive:
            json_files = list(json_path.glob("**/*.json"))
            logger.info(f"Scanning recursively for JSON files in {json_path}")
        else:
            json_files = list(json_path.glob("*.json"))
            logger.info(f"Scanning flat directory for JSON files in {json_path}")
        
        # Filter out summary files
        json_files = [f for f in json_files if not f.name.startswith('_summary')]
        total_files = len(json_files)
        
        logger.info(f"Found {total_files:,} JSON files to process in batches of {self.batch_size}")
        logger.info(f"Initial memory usage: {self._get_memory_usage():.2f} MB")
        
        total_records = 0
        processed_files = 0
        error_count = 0
        batch_num = 0
        start_time = time.time()
        
        # Process first batch with TRUNCATE mode if replacing
        first_batch = True
        
        # Process files in batches
        for file_batch in self._file_batch_generator(json_files, self.batch_size):
            batch_num += 1
            batch_start = time.time()
            
            # Check memory usage
            current_memory = self._get_memory_usage()
            if current_memory > self.max_memory_mb:
                logger.warning(f"Memory usage ({current_memory:.2f} MB) exceeds threshold "
                             f"({self.max_memory_mb} MB), forcing garbage collection")
                gc.collect()
                current_memory = self._get_memory_usage()
                logger.info(f"Memory after GC: {current_memory:.2f} MB")
            
            logger.info(f"Processing batch {batch_num} ({len(file_batch)} files) - "
                       f"Memory: {current_memory:.2f} MB")
            
            # Process batch (returns DataFrame and list of processed files)
            batch_df, processed_file_info = self._process_batch(file_batch, add_metadata=add_metadata)
            
            if not batch_df.empty:
                # Upload batch to BigQuery
                try:
                    # Use TRUNCATE for first batch if replacing, APPEND for all others
                    batch_mode = mode if first_batch else "append"
                    if batch_mode == "replace" and first_batch:
                        logger.warning("⚠️  TRUNCATING TABLE - All existing data will be deleted!")
                    self._upload_batch_to_bigquery(batch_df, mode=batch_mode)
                    
                    batch_records = len(batch_df)
                    total_records += batch_records
                    first_batch = False
                    
                    # Save uploaded files to tracking CSV
                    for filepath, record_count in processed_file_info:
                        self._save_uploaded_file(filepath, self.current_scrape_date, record_count)
                    
                    logger.info(f"Batch {batch_num}: Uploaded {len(processed_file_info)} files with {batch_records} records")
                    
                    # Clear DataFrame from memory
                    del batch_df
                    gc.collect()
                    
                except Exception as e:
                    logger.error(f"Failed to upload batch {batch_num}: {e}")
                    error_count += len(file_batch)
            else:
                # Even if no new records, count skipped files
                skipped = len(file_batch) - len(processed_file_info)
                if skipped > 0:
                    logger.info(f"Batch {batch_num}: Skipped {skipped} already uploaded files")
            
            processed_files += len(file_batch)
            
            # Progress update
            elapsed = time.time() - start_time
            batch_elapsed = time.time() - batch_start
            rate = processed_files / elapsed if elapsed > 0 else 0
            remaining = (total_files - processed_files) / rate if rate > 0 else 0
            
            logger.info(f"Batch {batch_num} completed in {batch_elapsed:.1f}s | "
                       f"Progress: {processed_files:,}/{total_files:,} files "
                       f"({processed_files/total_files*100:.1f}%) | "
                       f"Rate: {rate:.1f} files/sec | "
                       f"ETA: {remaining/60:.1f} min | "
                       f"Total records: {total_records:,}")
        
        elapsed = time.time() - start_time
        logger.info(f"Batch processing completed: {processed_files:,} files, "
                   f"{total_records:,} records, {error_count:,} errors, "
                   f"Time: {elapsed/60:.1f} minutes")
        logger.info(f"Final memory usage: {self._get_memory_usage():.2f} MB")
        
        return total_records
    
    def process_all_files(self, recursive: bool = True, add_metadata: bool = False) -> pd.DataFrame:
        """Legacy method - process all JSON files and return a DataFrame (memory intensive)
        
        WARNING: This method loads all data into memory at once. Use process_all_files_batched
        for large datasets.
        
        Args:
            recursive: Whether to scan subdirectories recursively
            add_metadata: Whether to add source file metadata
        """
        logger.warning("Using legacy process_all_files method - this loads all data into memory!")
        logger.warning("Consider using process_all_files_batched for large datasets")
        
        json_path = Path(self.json_directory)
        
        # Check if directory exists
        if not json_path.exists():
            logger.error(f"JSON directory does not exist: {json_path}")
            return pd.DataFrame()
        
        # Get JSON files (recursive or flat)
        if recursive:
            json_files = list(json_path.glob("**/*.json"))
            logger.info(f"Scanning recursively for JSON files in {json_path}")
        else:
            json_files = list(json_path.glob("*.json"))
            logger.info(f"Scanning flat directory for JSON files in {json_path}")
        
        # Filter out summary files unless we're processing them
        json_files = [f for f in json_files if not f.name.startswith('_summary')]
        total_files = len(json_files)
        
        logger.info(f"Found {total_files:,} JSON files to process")
        
        all_records = []
        processed_count = 0
        error_count = 0
        start_time = time.time()
        
        for json_file in json_files:
            try:
                records = self._process_json_file(json_file, add_metadata=add_metadata)
                all_records.extend(records)
                processed_count += 1
                
                # Progress update every 1000 files
                if processed_count % 1000 == 0:
                    elapsed = time.time() - start_time
                    rate = processed_count / elapsed if elapsed > 0 else 0
                    remaining = (total_files - processed_count) / rate if rate > 0 else 0
                    logger.info(f"Progress: {processed_count:,}/{total_files:,} files "
                               f"({processed_count/total_files*100:.1f}%) | "
                               f"Rate: {rate:.1f} files/sec | "
                               f"ETA: {remaining/60:.1f} min | "
                               f"Records: {len(all_records):,}")
                    
            except Exception as e:
                error_count += 1
                if error_count <= 10:
                    logger.error(f"Error processing {json_file.name}: {e}")
                continue
        
        elapsed = time.time() - start_time
        logger.info(f"Processing completed: {processed_count:,} files, "
                   f"{len(all_records):,} records, {error_count:,} errors, "
                   f"Time: {elapsed/60:.1f} minutes")
        
        # Convert to DataFrame and clean data
        df = pd.DataFrame(all_records)
        
        if df.empty:
            logger.warning("No records to upload")
            return df
        
        # Data type conversions
        logger.info("Converting data types...")
        
        numeric_columns = ['market_price', 'low_sale_price', 'low_sale_price_with_shipping',
                          'high_sale_price', 'high_sale_price_with_shipping']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        integer_columns = ['quantity_sold', 'transaction_count']
        for col in integer_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        
        # Convert dates
        if 'bucket_start_date' in df.columns:
            df['bucket_start_date'] = pd.to_datetime(df['bucket_start_date'], errors='coerce')
        
        if 'file_processed_at' in df.columns:
            df['file_processed_at'] = pd.to_datetime(df['file_processed_at'], errors='coerce')
        
        logger.info(f"DataFrame ready: {len(df):,} rows × {len(df.columns)} columns")
        return df
    
    def _upload_batch_to_bigquery(self, df: pd.DataFrame, mode: str = "append"):
        """Upload a batch DataFrame to BigQuery
        
        Args:
            df: DataFrame to upload
            mode: Upload mode - 'replace' or 'append'
        """
        if df.empty:
            return
        
        # Configure load job
        write_disposition = WriteDisposition.WRITE_TRUNCATE if mode == "replace" else WriteDisposition.WRITE_APPEND
        
        job_config = LoadJobConfig(
            write_disposition=write_disposition,
            autodetect=False,
            schema=self._get_table_schema(),
            max_bad_records=1000,
            ignore_unknown_values=True
        )
        
        # Load data
        job = self.client.load_table_from_dataframe(df, self.table_ref, job_config=job_config)
        job.result()  # Wait for completion
    
    def upload_to_bigquery(self, df: pd.DataFrame, mode: str = "append"):
        """Upload DataFrame to BigQuery (legacy method)"""
        if df.empty:
            logger.warning("No data to upload")
            return
        
        logger.info(f"Uploading {len(df):,} records to BigQuery (mode={mode})")
        if mode == "replace":
            logger.warning("⚠️  REPLACE mode: This will DELETE all existing data in the table!")
        
        # Configure load job
        write_disposition = WriteDisposition.WRITE_TRUNCATE if mode == "replace" else WriteDisposition.WRITE_APPEND
        
        job_config = LoadJobConfig(
            write_disposition=write_disposition,
            autodetect=False,
            schema=self._get_table_schema(),
            max_bad_records=1000,
            ignore_unknown_values=True
        )
        
        start_time = time.time()
        
        # Load data
        job = self.client.load_table_from_dataframe(df, self.table_ref, job_config=job_config)
        
        logger.info("Waiting for BigQuery upload to complete...")
        job.result()  # Wait for completion
        
        elapsed = time.time() - start_time
        
        # Get final table info
        table = self.client.get_table(self.table_ref)
        
        logger.info(f"Upload completed: {table.num_rows:,} total rows in table, "
                   f"Time: {elapsed/60:.1f} minutes, "
                   f"Rate: {len(df)/elapsed:.0f} records/sec")
    
    def run(self, mode: str = "append", process_zip: bool = False, use_batching: bool = True):
        """Main execution method with deduplication tracking
        
        Args:
            mode: Upload mode - 'replace' or 'append'
            process_zip: Whether to process the latest ZIP file first
            use_batching: Whether to use batch processing (recommended for large datasets)
        """
        logger.info("="*60)
        logger.info("Starting TCG data processing pipeline with deduplication tracking")
        logger.info(f"Mode: {mode}, Batching: {use_batching}")
        logger.info(f"Tracking CSV: {self.tracking_csv}")
        
        if mode == "replace":
            logger.warning("⚠️  WARNING: REPLACE MODE WILL DELETE ALL EXISTING DATA IN THE TABLE!")
            logger.warning("⚠️  Your existing historical data will be permanently lost!")
            logger.warning("⚠️  Consider using 'append' mode instead to preserve existing data.")
        logger.info("="*60)
        
        try:
            # Process latest ZIP if requested (always preserves structure now)
            if process_zip:
                success, message = self.process_latest_zip()
                if not success:
                    logger.error(f"ZIP processing failed: {message}")
                    if not self.current_scrape_date:
                        logger.error("No scrape date set - cannot proceed")
                        return
                    if not Path(self.json_directory).exists() or not list(Path(self.json_directory).glob("**/*.json")):
                        logger.error("No JSON files available to process")
                        return
                    logger.warning("Continuing with existing JSON files...")
                else:
                    logger.info(f"ZIP processing completed: {message}")
            else:
                # If not processing ZIP, ensure we have a scrape date
                if not self.current_scrape_date:
                    logger.warning("No ZIP processed - using today's date as scrape_date")
                    self.current_scrape_date = datetime.now().strftime("%Y-%m-%d")
            
            # Process files using batch method or legacy method
            if use_batching:
                logger.info("Using batch processing method with deduplication tracking")
                total_records = self.process_all_files_batched(
                    recursive=True,  # Always recursive for preserved structure
                    add_metadata=True,  # Always add metadata for tracking
                    mode=mode
                )
                if total_records > 0:
                    logger.info(f"Pipeline completed successfully - processed {total_records:,} records")
                else:
                    logger.warning("No new data processed (all files may have been previously uploaded)")
            else:
                # Legacy method - loads all data into memory
                logger.warning("Legacy mode does not support deduplication tracking!")
                df = self.process_all_files(
                    recursive=True,
                    add_metadata=True
                )
                
                if not df.empty:
                    # Upload to BigQuery
                    self.upload_to_bigquery(df, mode=mode)
                    logger.info("Pipeline completed successfully")
                else:
                    logger.warning("No data processed")
                
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
        finally:
            # Final garbage collection
            gc.collect()
            logger.info(f"Final memory usage: {self._get_memory_usage():.2f} MB")
            logger.info("="*60)


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Process TCG data and upload to BigQuery with deduplication tracking')
    parser.add_argument('--project', help='GCP project ID', default=None)
    parser.add_argument('--dataset', help='BigQuery dataset ID', default='tcg_data')
    parser.add_argument('--table', help='BigQuery table ID', default='tcg_prices_bda')
    parser.add_argument('--directory', help='JSON files directory', default='./product_details')
    parser.add_argument('--mode', choices=['replace', 'append'], default='append',
                       help='Upload mode: append (default, safe) or replace (DANGER: deletes all existing data!)')
    parser.add_argument('--upload-dir', help='Directory containing ZIP files to process',
                       default='~/fileuploader/uploads')
    parser.add_argument('--tracking-csv', help='CSV file to track uploaded files',
                       default='uploaded_files_tracker.csv')
    parser.add_argument('--process-zip', action='store_true',
                       help='Process the latest ZIP file from upload directory before uploading')
    parser.add_argument('--zip-only', action='store_true',
                       help='Only extract ZIP file, do not upload to BigQuery')
    parser.add_argument('--batch-size', type=int, default=1000,
                       help='Number of files to process per batch (default: 1000)')
    parser.add_argument('--max-memory', type=int, default=1024,
                       help='Maximum memory usage in MB before forcing garbage collection (default: 1024)')
    parser.add_argument('--no-batching', action='store_true',
                       help='Disable batch processing (use legacy method - not recommended, no dedup tracking)')
    
    args = parser.parse_args()
    
    try:
        processor = TCGDataProcessor(
            project_id=args.project,
            dataset_id=args.dataset,
            table_id=args.table,
            json_directory=args.directory,
            upload_directory=args.upload_dir,
            batch_size=args.batch_size,
            max_memory_mb=args.max_memory,
            tracking_csv=args.tracking_csv
        )
        
        # If only extracting ZIP
        if args.zip_only:
            success, message = processor.process_latest_zip()
            if success:
                logger.info(f"ZIP extraction completed: {message}")
            else:
                logger.error(f"ZIP extraction failed: {message}")
                sys.exit(1)
        else:
            # Run full pipeline
            processor.run(
                mode=args.mode, 
                process_zip=args.process_zip,
                use_batching=not args.no_batching
            )
        
    except Exception as e:
        logger.error(f"Failed to run processor: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()