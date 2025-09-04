#!/usr/bin/env python3
"""
TCG Data Processor - Analyzes JSON files and uploads to BigQuery
"""

import json
import os
import re
import logging
import sys
import zipfile
import shutil
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone
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
    """Main class for processing TCG data and uploading to BigQuery"""
    
    def __init__(self, project_id: str = None, dataset_id: str = "tcg_data", 
                 table_id: str = "tcg_prices_bda", json_directory: str = "./product_details",
                 upload_directory: str = None):
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
        
        # Validate configuration
        if not self.project_id:
            raise ValueError("GOOGLE_CLOUD_PROJECT environment variable not set")
        
        # Don't validate json_directory yet - it might be created from ZIP extraction
        
        logger.info(f"Configuration: Project={self.project_id}, Dataset={self.dataset_id}, Table={self.table_id}")
        logger.info(f"JSON Directory: {self.json_directory}")
        logger.info(f"Upload Directory: {self.upload_directory}")
        
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
            bigquery.SchemaField("file_processed_at", "TIMESTAMP"),
            bigquery.SchemaField("source_file", "STRING"),
            bigquery.SchemaField("source_directory", "STRING")
        ]
    
    @staticmethod
    def _extract_product_id(filename: str) -> str:
        """Extract product ID from filename (e.g., '481225.0.json' -> '481225')"""
        match = re.match(r'^(\d+)(?:\.0)?\.json$', filename)
        if match:
            return match.group(1)
        raise ValueError(f"Invalid filename format: {filename}")
    
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
    
    def extract_zip(self, zip_path: Path, extract_to: str = None, clean_existing: bool = True, 
                    preserve_structure: bool = False, handle_duplicates: str = "rename") -> Tuple[bool, str]:
        """Extract ZIP file to specified directory
        
        Args:
            zip_path: Path to the ZIP file to extract
            extract_to: Directory to extract to (default: self.json_directory)
            clean_existing: Whether to clean existing files in the target directory
            preserve_structure: Whether to preserve ZIP's directory structure
            handle_duplicates: How to handle duplicate filenames ('rename', 'skip', 'overwrite')
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        if not zip_path.exists():
            return False, f"ZIP file does not exist: {zip_path}"
        
        if not zipfile.is_zipfile(zip_path):
            return False, f"File is not a valid ZIP: {zip_path}"
        
        target_dir = Path(extract_to or self.json_directory)
        
        try:
            # Create target directory if it doesn't exist
            target_dir.mkdir(parents=True, exist_ok=True)
            
            # Clean existing JSON files if requested
            if clean_existing and target_dir.exists():
                existing_files = list(target_dir.glob("*.json"))
                if existing_files:
                    logger.info(f"Cleaning {len(existing_files)} existing JSON files from {target_dir}")
                    for file in existing_files:
                        file.unlink()
            
            # Extract the ZIP file
            logger.info(f"Extracting {zip_path.name} to {target_dir}")
            logger.info(f"Options: preserve_structure={preserve_structure}, handle_duplicates={handle_duplicates}")
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Get info about ZIP contents
                file_list = zip_ref.namelist()
                json_files = [f for f in file_list if f.endswith('.json')]
                
                logger.info(f"ZIP contains {len(file_list)} files, {len(json_files)} JSON files")
                
                # If preserving structure, extract and return
                if preserve_structure:
                    # Create subdirectory based on ZIP filename and date
                    zip_subdir = target_dir / f"{zip_path.stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    zip_subdir.mkdir(parents=True, exist_ok=True)
                    zip_ref.extractall(zip_subdir)
                    
                    # Count extracted files
                    extracted_json_files = list(zip_subdir.glob("**/*.json"))
                    logger.info(f"Preserved structure: {len(extracted_json_files)} JSON files in {zip_subdir}")
                    return True, f"Extracted {len(extracted_json_files)} JSON files to {zip_subdir} with preserved structure"
                
                # Extract all files for flattening
                zip_ref.extractall(target_dir)
            
            # If files were extracted to a subdirectory, flatten them with duplicate handling
            extracted_subdirs = [d for d in target_dir.iterdir() if d.is_dir()]
            if extracted_subdirs and not list(target_dir.glob("*.json")):
                # Files might be in subdirectories
                logger.info("Flattening directory structure with duplicate handling...")
                
                # Track files we've seen to detect duplicates
                seen_files = {}
                duplicate_count = 0
                skipped_count = 0
                renamed_count = 0
                
                for subdir in extracted_subdirs:
                    json_files_in_subdir = list(subdir.glob("**/*.json"))
                    if json_files_in_subdir:
                        logger.info(f"Processing {len(json_files_in_subdir)} JSON files from {subdir.name}")
                        
                        for json_file in json_files_in_subdir:
                            base_name = json_file.name
                            
                            # Check for duplicates
                            if base_name in seen_files:
                                duplicate_count += 1
                                
                                if handle_duplicates == "skip":
                                    logger.debug(f"Skipping duplicate: {base_name} from {subdir.name}")
                                    skipped_count += 1
                                    continue
                                elif handle_duplicates == "rename":
                                    # Create unique name with directory prefix or counter
                                    if base_name == "_summary.json":
                                        # Special handling for summary files
                                        dir_prefix = subdir.name.replace("product_details_", "").replace("2025-09/", "")
                                        new_name = f"{dir_prefix}_{base_name}"
                                    else:
                                        # Add counter for other duplicates
                                        counter = 1
                                        new_name = f"{json_file.stem}_{counter}{json_file.suffix}"
                                        while (target_dir / new_name).exists():
                                            counter += 1
                                            new_name = f"{json_file.stem}_{counter}{json_file.suffix}"
                                    
                                    target_path = target_dir / new_name
                                    logger.debug(f"Renaming duplicate: {base_name} -> {new_name}")
                                    renamed_count += 1
                                else:  # overwrite
                                    target_path = target_dir / base_name
                                    logger.debug(f"Overwriting: {base_name}")
                            else:
                                target_path = target_dir / base_name
                                seen_files[base_name] = str(subdir.name)
                            
                            shutil.move(str(json_file), str(target_path))
                        
                        # Clean up empty subdirectory
                        if subdir.exists() and not any(subdir.iterdir()):
                            shutil.rmtree(subdir, ignore_errors=True)
                
                if duplicate_count > 0:
                    logger.warning(f"Found {duplicate_count} duplicate filenames: "
                                 f"renamed={renamed_count}, skipped={skipped_count}, "
                                 f"overwritten={duplicate_count - renamed_count - skipped_count}")
            
            # Verify extraction
            extracted_json_files = list(target_dir.glob("*.json"))
            
            if not extracted_json_files:
                return False, f"No JSON files found after extraction"
            
            logger.info(f"Successfully extracted {len(extracted_json_files)} JSON files")
            return True, f"Extracted {len(extracted_json_files)} JSON files from {zip_path.name}"
            
        except zipfile.BadZipFile as e:
            return False, f"Corrupt ZIP file: {e}"
        except Exception as e:
            return False, f"Extraction failed: {e}"
    
    def process_latest_zip(self, preserve_structure: bool = False, 
                          handle_duplicates: str = "rename") -> Tuple[bool, str]:
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
        
        # Extract ZIP with options
        success, message = self.extract_zip(
            latest_zip, 
            preserve_structure=preserve_structure,
            handle_duplicates=handle_duplicates
        )
        if not success:
            logger.error(f"Failed to extract ZIP: {message}")
            return False, message
        
        logger.info(message)
        return True, f"Ready to process data from {latest_zip.name}"
    
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
                'file_processed_at': datetime.now(timezone.utc)
            }
            
            # Add source metadata if requested
            if add_metadata:
                base_info['source_file'] = str(file_path.relative_to(Path(self.json_directory)))
                base_info['source_directory'] = file_path.parent.name
            
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
    
    def process_all_files(self, recursive: bool = True, add_metadata: bool = False) -> pd.DataFrame:
        """Process all JSON files and return a DataFrame
        
        Args:
            recursive: Whether to scan subdirectories recursively
            add_metadata: Whether to add source file metadata
        """
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
    
    def upload_to_bigquery(self, df: pd.DataFrame, mode: str = "replace"):
        """Upload DataFrame to BigQuery"""
        if df.empty:
            logger.warning("No data to upload")
            return
        
        logger.info(f"Uploading {len(df):,} records to BigQuery (mode={mode})")
        
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
    
    def run(self, mode: str = "replace", process_zip: bool = False,
            preserve_structure: bool = False, handle_duplicates: str = "rename"):
        """Main execution method
        
        Args:
            mode: Upload mode - 'replace' or 'append'
            process_zip: Whether to process the latest ZIP file first
            preserve_structure: Whether to preserve ZIP directory structure
            handle_duplicates: How to handle duplicate files ('rename', 'skip', 'overwrite')
        """
        logger.info("="*60)
        logger.info("Starting TCG data processing pipeline")
        logger.info("="*60)
        
        try:
            # Process latest ZIP if requested
            if process_zip:
                success, message = self.process_latest_zip(
                    preserve_structure=preserve_structure,
                    handle_duplicates=handle_duplicates
                )
                if not success:
                    logger.error(f"ZIP processing failed: {message}")
                    if not Path(self.json_directory).exists() or not list(Path(self.json_directory).glob("**/*.json")):
                        logger.error("No JSON files available to process")
                        return
                    logger.warning("Continuing with existing JSON files...")
                else:
                    logger.info(f"ZIP processing completed: {message}")
            
            # Process all files (use recursive scanning if structure was preserved)
            df = self.process_all_files(
                recursive=preserve_structure or process_zip,
                add_metadata=preserve_structure
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
            logger.info("="*60)


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Process TCG data and upload to BigQuery')
    parser.add_argument('--project', help='GCP project ID', default=None)
    parser.add_argument('--dataset', help='BigQuery dataset ID', default='tcg_data')
    parser.add_argument('--table', help='BigQuery table ID', default='tcg_prices_bda')
    parser.add_argument('--directory', help='JSON files directory', default='./product_details')
    parser.add_argument('--mode', choices=['replace', 'append'], default='replace',
                       help='Upload mode: replace all data or append')
    parser.add_argument('--upload-dir', help='Directory containing ZIP files to process',
                       default='~/fileuploader/uploads')
    parser.add_argument('--process-zip', action='store_true',
                       help='Process the latest ZIP file from upload directory before uploading')
    parser.add_argument('--zip-only', action='store_true',
                       help='Only extract ZIP file, do not upload to BigQuery')
    parser.add_argument('--preserve-structure', action='store_true',
                       help='Preserve directory structure when extracting ZIP files')
    parser.add_argument('--handle-duplicates', choices=['rename', 'skip', 'overwrite'], 
                       default='rename', help='How to handle duplicate filenames when flattening')
    
    args = parser.parse_args()
    
    try:
        processor = TCGDataProcessor(
            project_id=args.project,
            dataset_id=args.dataset,
            table_id=args.table,
            json_directory=args.directory,
            upload_directory=args.upload_dir
        )
        
        # If only extracting ZIP
        if args.zip_only:
            success, message = processor.process_latest_zip(
                preserve_structure=args.preserve_structure,
                handle_duplicates=args.handle_duplicates
            )
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
                preserve_structure=args.preserve_structure,
                handle_duplicates=args.handle_duplicates
            )
        
    except Exception as e:
        logger.error(f"Failed to run processor: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()