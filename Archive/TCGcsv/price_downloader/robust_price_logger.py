#!/usr/bin/env python3
"""
Robust Price Logger - Main orchestrator for historical price data backfill
Comprehensive error handling, checkpointing, raw data preservation, and recovery
"""
import os
import json
import time
import shutil
import pandas as pd
import logging
import random
import functools
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from pathlib import Path

try:
    from .price_logger_config import PriceLoggerConfig, DEFAULT_CONFIG
    from .failure_analyzer import FailureAnalyzer, FailureRecord
    from .price_downloader import TCGPriceDownloader
    from .bigquery_price_loader import BigQueryPriceLoader
except ImportError:
    from price_logger_config import PriceLoggerConfig, DEFAULT_CONFIG
    from failure_analyzer import FailureAnalyzer, FailureRecord
    from price_downloader import TCGPriceDownloader
    from bigquery_price_loader import BigQueryPriceLoader

def retry_with_backoff(config=None):
    """Decorator for exponential backoff retry with jitter"""
    if config is None:
        config = DEFAULT_CONFIG
    
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            for attempt in range(config.retry.max_retries + 1):
                try:
                    return func(self, *args, **kwargs)
                except Exception as e:
                    if attempt == config.retry.max_retries:
                        self.logger.error(f"Max retries ({config.retry.max_retries}) exceeded for {func.__name__}: {e}")
                        # Record failure
                        if hasattr(self, 'failure_analyzer'):
                            func_name = func.__name__.replace('_', ' ')
                            self.failure_analyzer.record_failure(
                                date=kwargs.get('date', 'unknown'),
                                failure_type=func_name,
                                error=e,
                                retry_count=attempt,
                                context={'function': func.__name__, 'args': str(args)}
                            )
                        raise e
                    
                    # Calculate delay with exponential backoff and jitter
                    delay = min(
                        config.retry.base_delay * (config.retry.backoff_factor ** attempt),
                        config.retry.max_delay
                    )
                    jitter_min, jitter_max = config.retry.jitter_range
                    jitter = random.uniform(jitter_min, jitter_max) * delay
                    total_delay = delay + jitter
                    
                    self.logger.warning(f"Attempt {attempt + 1}/{config.retry.max_retries + 1} failed for {func.__name__}: {e}")
                    self.logger.info(f"Retrying in {total_delay:.2f} seconds...")
                    time.sleep(total_delay)
                    
            return None  # Should never reach here
        return wrapper
    return decorator

class CheckpointManager:
    """Manages checkpoint data for resumable operations"""
    
    def __init__(self, config: PriceLoggerConfig, run_id: str):
        self.config = config
        self.run_id = run_id
        self.checkpoint_file = config.get_checkpoint_filename(run_id)
        self.checkpoint_data = self._load_checkpoint()
    
    def _load_checkpoint(self) -> Dict[str, Any]:
        """Load existing checkpoint data"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    return json.load(f)
            except Exception:
                pass
        
        return {
            'run_id': self.run_id,
            'started_at': datetime.now().isoformat(),
            'completed_dates': [],
            'failed_dates': [],
            'current_date': None,
            'total_records_processed': 0,
            'phase': 'initialization',
            'statistics': {
                'total_dates': 0,
                'completed_dates': 0,
                'failed_dates': 0,
                'total_processing_time': 0.0,
                'average_time_per_date': 0.0
            }
        }
    
    def save_checkpoint(self):
        """Save current checkpoint data"""
        self.checkpoint_data['last_updated'] = datetime.now().isoformat()
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump(self.checkpoint_data, f, indent=2)
        except Exception as e:
            logging.error(f"Failed to save checkpoint: {e}")
    
    def mark_date_completed(self, date_str: str, record_count: int, processing_time: float):
        """Mark a date as completed"""
        if date_str not in self.checkpoint_data['completed_dates']:
            self.checkpoint_data['completed_dates'].append(date_str)
        
        if date_str in self.checkpoint_data['failed_dates']:
            self.checkpoint_data['failed_dates'].remove(date_str)
        
        self.checkpoint_data['total_records_processed'] += record_count
        self.checkpoint_data['statistics']['completed_dates'] = len(self.checkpoint_data['completed_dates'])
        self.checkpoint_data['statistics']['total_processing_time'] += processing_time
        
        # Calculate average time per date
        completed_count = self.checkpoint_data['statistics']['completed_dates']
        if completed_count > 0:
            total_time = self.checkpoint_data['statistics']['total_processing_time']
            self.checkpoint_data['statistics']['average_time_per_date'] = total_time / completed_count
        
        self.save_checkpoint()
    
    def mark_date_failed(self, date_str: str):
        """Mark a date as failed"""
        if date_str not in self.checkpoint_data['failed_dates']:
            self.checkpoint_data['failed_dates'].append(date_str)
        
        if date_str in self.checkpoint_data['completed_dates']:
            self.checkpoint_data['completed_dates'].remove(date_str)
        
        self.checkpoint_data['statistics']['failed_dates'] = len(self.checkpoint_data['failed_dates'])
        self.save_checkpoint()
    
    def set_phase(self, phase: str):
        """Set current processing phase"""
        self.checkpoint_data['phase'] = phase
        self.save_checkpoint()
    
    def get_remaining_dates(self, all_dates: List[str]) -> List[str]:
        """Get list of dates that still need processing"""
        completed = set(self.checkpoint_data['completed_dates'])
        return [date for date in all_dates if date not in completed]

class RobustPriceLogger:
    """Main orchestrator for robust historical price data collection"""
    
    def __init__(self, config: PriceLoggerConfig = None, run_id: str = None):
        self.config = config or DEFAULT_CONFIG
        self.run_id = run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Setup logging
        self._setup_logging()
        
        # Initialize components
        self.checkpoint_manager = CheckpointManager(self.config, self.run_id)
        self.failure_analyzer = FailureAnalyzer(self.config)
        self.downloader = TCGPriceDownloader(max_workers=self.config.performance.max_workers)
        self.bigquery_loader = None  # Initialized on demand
        
        self.logger.info(f"Initialized RobustPriceLogger with run_id: {self.run_id}")
    
    def _setup_logging(self):
        """Setup comprehensive logging"""
        self.logger = logging.getLogger(f"RobustPriceLogger_{self.run_id}")
        self.logger.setLevel(getattr(logging, self.config.logging.log_level))
        
        # Clear existing handlers
        self.logger.handlers = []
        
        formatter = logging.Formatter(self.config.logging.log_format)
        
        # Console handler
        if self.config.logging.console_logging:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
        
        # File handler
        if self.config.logging.file_logging:
            log_file = self.config.get_log_filename(f"robust_logger_{self.run_id}")
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
    
    def generate_date_range(self, start_date: str = None, end_date: str = None) -> List[str]:
        """Generate list of dates for processing"""
        start = start_date or self.config.date_range.start_date
        end = end_date or self.config.date_range.end_date
        
        start_dt = datetime.strptime(start, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end, '%Y-%m-%d').date()
        
        dates = []
        current = start_dt
        
        while current <= end_dt:
            # Skip weekends if configured
            if self.config.date_range.skip_weekends and current.weekday() >= 5:
                current += timedelta(days=1)
                continue
            
            dates.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)
        
        return dates
    
    @retry_with_backoff()
    def download_raw_archive(self, date_str: str) -> bool:
        """Download and preserve raw archive with retry logic"""
        self.logger.info(f"Downloading raw archive for {date_str}")
        
        archive_url = f"{self.downloader.base_url}/prices-{date_str}.ppmd.7z"
        raw_archive_path = self.config.get_raw_archive_path(date_str)
        
        # Check if already downloaded
        if os.path.exists(raw_archive_path):
            file_size = os.path.getsize(raw_archive_path)
            if file_size > 1000:  # Basic size check
                self.logger.info(f"Raw archive already exists: {raw_archive_path}")
                return True
        
        try:
            import requests
            response = requests.get(archive_url, stream=True, timeout=300)
            response.raise_for_status()
            
            with open(raw_archive_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            file_size = os.path.getsize(raw_archive_path)
            self.logger.info(f"Downloaded raw archive: {raw_archive_path} ({file_size:,} bytes)")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to download raw archive for {date_str}: {e}")
            # Remove partial file
            if os.path.exists(raw_archive_path):
                os.remove(raw_archive_path)
            raise
    
    @retry_with_backoff()
    def extract_archive(self, date_str: str) -> bool:
        """Extract archive to structured directory with retry logic"""
        self.logger.info(f"Extracting archive for {date_str}")
        
        raw_archive_path = self.config.get_raw_archive_path(date_str)
        extract_path = self.config.get_extracted_path(date_str)
        
        if not os.path.exists(raw_archive_path):
            raise FileNotFoundError(f"Raw archive not found: {raw_archive_path}")
        
        # Check if already extracted
        if os.path.exists(extract_path) and os.listdir(extract_path):
            self.logger.info(f"Archive already extracted: {extract_path}")
            return True
        
        try:
            # Remove existing extraction directory
            if os.path.exists(extract_path):
                shutil.rmtree(extract_path)
            
            # Extract using 7z
            import subprocess
            result = subprocess.run(
                ['7z', 'x', raw_archive_path, f'-o{self.config.directories.extracted_path}', '-y'],
                capture_output=True,
                text=True,
                check=True,
                timeout=600  # 10 minute timeout
            )
            
            if os.path.exists(extract_path):
                file_count = sum(len(files) for _, _, files in os.walk(extract_path))
                self.logger.info(f"Extracted {file_count:,} files to: {extract_path}")
                return True
            else:
                raise RuntimeError(f"Extraction failed - directory not found: {extract_path}")
                
        except Exception as e:
            self.logger.error(f"Failed to extract archive for {date_str}: {e}")
            if os.path.exists(extract_path):
                shutil.rmtree(extract_path)
            raise
    
    @retry_with_backoff()
    def process_date_data(self, date_str: str) -> Tuple[int, str]:
        """Process extracted data into CSV with retry logic"""
        self.logger.info(f"Processing data for {date_str}")
        
        extract_path = self.config.get_extracted_path(date_str)
        csv_path = self.config.get_processed_csv_path(date_str)
        
        if not os.path.exists(extract_path):
            raise FileNotFoundError(f"Extracted data not found: {extract_path}")
        
        # Check if already processed
        if os.path.exists(csv_path):
            try:
                # Quick validation
                df = pd.read_csv(csv_path, nrows=10)
                if len(df) > 0:
                    record_count = sum(1 for _ in open(csv_path)) - 1  # Count lines minus header
                    self.logger.info(f"Data already processed: {csv_path} ({record_count:,} records)")
                    return record_count, csv_path
            except Exception:
                pass  # File might be corrupted, reprocess
        
        try:
            # Use existing downloader logic but with our directory structure
            df = self.downloader.create_price_dataframe_from_path(extract_path, date_str)
            
            # Validate data
            if len(df) < self.config.validation.min_records_per_date:
                raise ValueError(f"Insufficient records: {len(df)} < {self.config.validation.min_records_per_date}")
            
            # Save to CSV
            df.to_csv(csv_path, index=False)
            record_count = len(df)
            
            self.logger.info(f"Processed {record_count:,} records to: {csv_path}")
            return record_count, csv_path
            
        except Exception as e:
            self.logger.error(f"Failed to process data for {date_str}: {e}")
            if os.path.exists(csv_path):
                os.remove(csv_path)
            raise
    
    @retry_with_backoff()
    def upload_to_bigquery(self, date_str: str, csv_path: str) -> bool:
        """Upload processed data to BigQuery with retry logic"""
        self.logger.info(f"Uploading {date_str} to BigQuery")
        
        if not self.bigquery_loader:
            from dotenv import load_dotenv
            load_dotenv()
            
            project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
            dataset_id = os.getenv('BIGQUERY_DATASET', 'tcg_data')
            self.bigquery_loader = BigQueryPriceLoader(project_id=project_id, dataset_id=dataset_id)
        
        try:
            # Check if data already exists
            existing_count = self.bigquery_loader.check_existing_data(date_str)
            
            if existing_count > 0:
                self.logger.info(f"Replacing existing {existing_count:,} records for {date_str}")
                success = self.bigquery_loader.replace_date_data(csv_path, date_str)
            else:
                success = self.bigquery_loader.load_price_data(csv_path, date_str)
            
            if success:
                self.logger.info(f"Successfully uploaded {date_str} to BigQuery")
                return True
            else:
                raise RuntimeError("BigQuery upload returned False")
                
        except Exception as e:
            self.logger.error(f"Failed to upload {date_str} to BigQuery: {e}")
            raise
    
    def process_single_date(self, date_str: str) -> Dict[str, Any]:
        """Process a single date completely with comprehensive error handling"""
        start_time = time.time()
        
        self.logger.info(f"Starting processing for {date_str}")
        self.checkpoint_manager.checkpoint_data['current_date'] = date_str
        
        result = {
            'date': date_str,
            'success': False,
            'record_count': 0,
            'processing_time': 0.0,
            'steps_completed': [],
            'error': None
        }
        
        try:
            # Step 1: Download raw archive
            self.download_raw_archive(date_str)
            result['steps_completed'].append('download')
            
            # Step 2: Extract archive
            self.extract_archive(date_str)
            result['steps_completed'].append('extract')
            
            # Step 3: Process data to CSV
            record_count, csv_path = self.process_date_data(date_str)
            result['record_count'] = record_count
            result['steps_completed'].append('process')
            
            # Step 4: Upload to BigQuery
            self.upload_to_bigquery(date_str, csv_path)
            result['steps_completed'].append('upload')
            
            # Success!
            processing_time = time.time() - start_time
            result['processing_time'] = processing_time
            result['success'] = True
            
            self.checkpoint_manager.mark_date_completed(date_str, record_count, processing_time)
            
            # Cleanup if configured
            if self.config.performance.cleanup_temp_files:
                self._cleanup_temporary_files(date_str)
            
            self.logger.info(f"Successfully processed {date_str} in {processing_time:.2f}s ({record_count:,} records)")
            
        except Exception as e:
            processing_time = time.time() - start_time
            result['processing_time'] = processing_time
            result['error'] = str(e)
            
            self.checkpoint_manager.mark_date_failed(date_str)
            self.logger.error(f"Failed to process {date_str} after {processing_time:.2f}s: {e}")
        
        return result
    
    def _cleanup_temporary_files(self, date_str: str):
        """Clean up temporary files for a date"""
        try:
            # Remove extracted directory
            extract_path = self.config.get_extracted_path(date_str)
            if os.path.exists(extract_path):
                shutil.rmtree(extract_path)
                self.logger.debug(f"Cleaned up extracted files: {extract_path}")
            
            # Remove processed CSV file (to save disk space - data is in BigQuery)
            csv_path = self.config.get_processed_csv_path(date_str)
            if os.path.exists(csv_path):
                os.remove(csv_path)
                self.logger.debug(f"Cleaned up processed CSV: {csv_path}")
                
        except Exception as e:
            self.logger.warning(f"Failed to cleanup temporary files for {date_str}: {e}")
    
    def run_historical_backfill(self, start_date: str = None, end_date: str = None) -> Dict[str, Any]:
        """Run the complete historical backfill process"""
        self.logger.info("Starting historical price data backfill")
        
        # Generate date range
        all_dates = self.generate_date_range(start_date, end_date)
        self.checkpoint_manager.checkpoint_data['statistics']['total_dates'] = len(all_dates)
        
        # Get remaining dates (for resumability)
        remaining_dates = self.checkpoint_manager.get_remaining_dates(all_dates)
        
        self.logger.info(f"Total dates to process: {len(all_dates)}")
        self.logger.info(f"Remaining dates: {len(remaining_dates)}")
        self.logger.info(f"Already completed: {len(all_dates) - len(remaining_dates)}")
        
        if not remaining_dates:
            self.logger.info("All dates already processed!")
            return self._generate_final_report(all_dates)
        
        # Update checkpoint
        self.checkpoint_manager.set_phase('processing')
        
        # Process dates
        results = []
        start_time = time.time()
        
        with tqdm(remaining_dates, desc="Processing dates") as pbar:
            for date_str in pbar:
                pbar.set_description(f"Processing {date_str}")
                
                result = self.process_single_date(date_str)
                results.append(result)
                
                # Update progress bar
                status = "✓" if result['success'] else "✗"
                pbar.set_postfix(
                    status=status,
                    records=f"{result['record_count']:,}",
                    time=f"{result['processing_time']:.1f}s"
                )
                
                # Periodic checkpoint save
                if len(results) % self.config.performance.checkpoint_interval == 0:
                    self.checkpoint_manager.save_checkpoint()
        
        # Final processing
        total_time = time.time() - start_time
        self.checkpoint_manager.set_phase('completed')
        
        return self._generate_final_report(all_dates, results, total_time)
    
    def _generate_final_report(self, all_dates: List[str], results: List[Dict] = None, 
                              total_time: float = 0.0) -> Dict[str, Any]:
        """Generate final processing report"""
        successful_results = [r for r in (results or []) if r['success']]
        failed_results = [r for r in (results or []) if not r['success']]
        
        completed_dates = self.checkpoint_manager.checkpoint_data['completed_dates']
        failed_dates = self.checkpoint_manager.checkpoint_data['failed_dates']
        
        report = {
            'run_id': self.run_id,
            'completed_at': datetime.now().isoformat(),
            'summary': {
                'total_dates': len(all_dates),
                'completed_dates': len(completed_dates),
                'failed_dates': len(failed_dates),
                'success_rate': len(completed_dates) / len(all_dates) * 100 if all_dates else 0,
                'total_records': self.checkpoint_manager.checkpoint_data['total_records_processed'],
                'total_processing_time': total_time,
                'average_time_per_date': total_time / len(successful_results) if successful_results else 0,
                'average_records_per_date': (
                    self.checkpoint_manager.checkpoint_data['total_records_processed'] / len(completed_dates)
                    if completed_dates else 0
                )
            },
            'performance': {
                'records_per_second': (
                    self.checkpoint_manager.checkpoint_data['total_records_processed'] / total_time
                    if total_time > 0 else 0
                ),
                'dates_per_hour': len(successful_results) / (total_time / 3600) if total_time > 0 else 0
            },
            'failed_dates': failed_dates,
            'next_actions': []
        }
        
        # Add recommendations
        if failed_dates:
            report['next_actions'].extend([
                f"Investigate {len(failed_dates)} failed dates",
                "Review failure patterns in logs",
                "Consider manual retry for failed dates",
                "Generate recovery report for detailed analysis"
            ])
        
        if len(completed_dates) == len(all_dates):
            report['next_actions'].append("✅ Historical backfill completed successfully!")
        
        # Save report
        report_file = os.path.join(
            self.config.directories.logs_path,
            f"final_report_{self.run_id}.json"
        )
        
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        self.logger.info(f"Final report saved: {report_file}")
        return report
    
    def generate_recovery_report(self) -> str:
        """Generate comprehensive recovery report"""
        return self.failure_analyzer.save_recovery_report(f"recovery_{self.run_id}.json")
    
    def retry_failed_dates(self, max_retries: int = 3) -> Dict[str, Any]:
        """Retry processing failed dates"""
        failed_dates = self.checkpoint_manager.checkpoint_data.get('failed_dates', [])
        
        if not failed_dates:
            self.logger.info("No failed dates to retry")
            return {'retried_dates': 0, 'recovered_dates': 0}
        
        self.logger.info(f"Retrying {len(failed_dates)} failed dates (max {max_retries} attempts each)")
        
        recovered_dates = []
        
        for date_str in failed_dates[:]:  # Copy list to avoid modification during iteration
            self.logger.info(f"Retrying {date_str}")
            
            result = self.process_single_date(date_str)
            
            if result['success']:
                recovered_dates.append(date_str)
                self.logger.info(f"✅ Recovered {date_str}")
            else:
                self.logger.warning(f"❌ Still failing {date_str}: {result['error']}")
        
        return {
            'retried_dates': len(failed_dates),
            'recovered_dates': len(recovered_dates),
            'still_failing': len(failed_dates) - len(recovered_dates)
        }