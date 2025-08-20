#!/usr/bin/env python3
"""
Enhanced TCG Metadata Downloader with Resume Support
Comprehensive downloader with checkpoints, logging, error handling, and resume functionality
"""
import requests
import pandas as pd
import time
import json
import os
import logging
import signal
import sys
from datetime import datetime, date
from typing import Dict, List, Any, Optional, Set
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from dataclasses import dataclass, asdict
import yaml

# Import proxy manager for automatic switching
try:
    from proxy_manager import MihomoProxyManager
    PROXY_MANAGER_AVAILABLE = True
except ImportError:
    PROXY_MANAGER_AVAILABLE = False

@dataclass
class DownloadCheckpoint:
    """Checkpoint data structure"""
    version: str = "1.0"
    started_at: str = ""
    last_updated: str = ""
    total_categories: int = 0
    total_groups: int = 0
    total_products: int = 0
    completed_categories: List[int] = None
    completed_groups: Set[str] = None  # Set of "category_id:group_id" strings
    failed_groups: Dict[str, str] = None  # group_key: error_message
    current_category: Optional[int] = None
    current_group: Optional[int] = None
    
    def __post_init__(self):
        if self.completed_categories is None:
            self.completed_categories = []
        if self.completed_groups is None:
            self.completed_groups = set()
        if self.failed_groups is None:
            self.failed_groups = {}

class EnhancedTCGMetadataDownloader:
    def __init__(self, 
                 project_id: Optional[str] = None, 
                 dataset_id: str = "tcg_data",
                 table_id: str = "tcg_metadata", 
                 min_request_interval: float = 1.2, 
                 batch_size: int = 500,
                 checkpoint_file: str = "download_checkpoint.json",
                 log_file: str = "tcg_enhanced_download.log",
                 config_file: str = "download_config.yaml",
                 proxy_config: Optional[Dict] = None,
                 use_proxy_manager: bool = True,
                 mihomo_api_url: str = "http://127.0.0.1:9090",
                 mihomo_secret: str = ""):
        """
        Initialize Enhanced TCG Metadata downloader with full resume support
        
        Args:
            project_id: GCP project ID (None for default)
            dataset_id: BigQuery dataset name
            table_id: BigQuery table name
            min_request_interval: Minimum seconds between API requests
            batch_size: Number of rows to batch before streaming to BigQuery
            checkpoint_file: Path to checkpoint JSON file
            log_file: Path to log file
            config_file: Path to YAML config file
            proxy_config: Dict with proxy settings (http, https keys) - legacy mode
            use_proxy_manager: Use Mihomo proxy manager for automatic switching
            mihomo_api_url: Mihomo API URL
            mihomo_secret: Mihomo API secret
        """
        self.base_url = "https://tcgcsv.com/tcgplayer"
        self.min_request_interval = min_request_interval
        self.last_request_time = 0
        self.last_operation_start = 0  # Track start of entire operation cycle
        self.data_dir = "data"
        self.checkpoint_file = checkpoint_file
        self.config_file = config_file
        
        # Create data directory
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Load configuration
        self.config = self._load_config()
        
        # Setup logging first
        self._setup_logging(log_file)
        
        # Initialize proxy manager for automatic switching
        self.proxy_manager = None
        self.use_proxy_manager = use_proxy_manager and PROXY_MANAGER_AVAILABLE
        
        if self.use_proxy_manager:
            try:
                self.proxy_manager = MihomoProxyManager(
                    api_url=mihomo_api_url,
                    secret=mihomo_secret,
                    rate_limit_codes=[403, 429, 503, 502, 504]
                )
                self.logger.info("✅ Proxy Manager initialized - automatic proxy switching enabled")
                # Get current proxy info
                current_proxy = self.proxy_manager.get_current_proxy()
                if current_proxy:
                    self.logger.info(f"Current proxy: {current_proxy}")
            except Exception as e:
                self.logger.warning(f"Failed to initialize proxy manager: {e}")
                self.use_proxy_manager = False
                self.proxy_manager = None
        
        # Setup legacy proxy configuration if proxy manager is not available
        if not self.use_proxy_manager:
            self.proxy_config = proxy_config or self.config.get('proxy', {})
            self.session = requests.Session()
            if self.proxy_config:
                self.session.proxies.update(self.proxy_config)
                self.logger.info(f"Using legacy proxy: {self.proxy_config}")
            else:
                self.logger.info("No proxy configuration - using direct connection")
        else:
            # Use requests.Session() directly for proxy manager mode
            self.session = requests.Session()
        
        # BigQuery setup
        self.bq_client = bigquery.Client(project=project_id)
        self.project_id = project_id or self.bq_client.project
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        
        # Batching for efficient streaming
        self.batch_size = batch_size
        self.current_batch = []
        self.total_rows_inserted = 0
        
        # Checkpoint system
        self.checkpoint = self._load_checkpoint()
        
        # Graceful shutdown handler
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.logger.info(f"Enhanced TCG Metadata Downloader initialized")
        self.logger.info(f"Target: {self.table_ref}")
        req_per_sec = 1/min_request_interval if min_request_interval > 0 else float('inf')
        rate_desc = f"{req_per_sec:.2f} req/s" if req_per_sec != float('inf') else "UNLIMITED (BigQuery buffering only)"
        self.logger.info(f"Rate limit: {rate_desc}")
        self.logger.info(f"Batch size: {batch_size} rows")
        
    def _load_config(self) -> Dict:
        """Load configuration from YAML file"""
        default_config = {
            'rate_limiting': {
                'min_interval': 1.2,
                'max_retries': 3,
                'backoff_factor': 2.0
            },
            'bigquery': {
                'batch_size': 500,
                'create_backup': True,
                'deduplication': True
            },
            'categories': {
                'skip_empty': True,
                'retry_failed': True
            },
            'logging': {
                'level': 'INFO',
                'max_file_size': '10MB',
                'backup_count': 5
            },
            'proxy': {
                'http': 'http://127.0.0.1:20172',
                'https': 'http://127.0.0.1:20172'
            }
        }
        
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    config = yaml.safe_load(f)
                    # Merge with defaults
                    for key, value in default_config.items():
                        if key not in config:
                            config[key] = value
                    return config
            except Exception as e:
                print(f"Config load error, using defaults: {e}")
        
        return default_config
    
    def _setup_logging(self, log_file: str):
        """Setup comprehensive logging with rotation"""
        from logging.handlers import RotatingFileHandler
        
        # Create logger
        self.logger = logging.getLogger('tcg_enhanced_downloader')
        self.logger.setLevel(getattr(logging, self.config['logging']['level']))
        
        # Clear any existing handlers
        self.logger.handlers.clear()
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(console_format)
        self.logger.addHandler(console_handler)
        
        # File handler with rotation
        max_bytes = 10 * 1024 * 1024  # 10MB
        file_handler = RotatingFileHandler(
            log_file, 
            maxBytes=max_bytes, 
            backupCount=self.config['logging']['backup_count']
        )
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        file_handler.setFormatter(file_format)
        self.logger.addHandler(file_handler)
        
    def _load_checkpoint(self) -> DownloadCheckpoint:
        """Load checkpoint from file or create new one"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                    # Convert completed_groups back to set
                    if 'completed_groups' in data and isinstance(data['completed_groups'], list):
                        data['completed_groups'] = set(data['completed_groups'])
                    checkpoint = DownloadCheckpoint(**data)
                    self.logger.info(f"Loaded checkpoint: {len(checkpoint.completed_groups)} groups completed")
                    return checkpoint
            except Exception as e:
                self.logger.warning(f"Failed to load checkpoint, starting fresh: {e}")
        
        checkpoint = DownloadCheckpoint()
        checkpoint.started_at = datetime.now().isoformat()
        return checkpoint
    
    def _save_checkpoint(self):
        """Save current checkpoint to file"""
        try:
            self.checkpoint.last_updated = datetime.now().isoformat()
            # Convert set to list for JSON serialization
            data = asdict(self.checkpoint)
            data['completed_groups'] = list(self.checkpoint.completed_groups)
            
            with open(self.checkpoint_file, 'w') as f:
                json.dump(data, f, indent=2)
            self.logger.debug("Checkpoint saved")
        except Exception as e:
            self.logger.error(f"Failed to save checkpoint: {e}")
    
    def _signal_handler(self, signum, frame):
        """Handle graceful shutdown on signals"""
        self.logger.info(f"Received signal {signum}, shutting down gracefully...")
        self._save_checkpoint()
        # Flush remaining batch
        if self.current_batch:
            self.logger.info("Flushing remaining batch...")
            self.stream_to_bigquery([], force=True)
        self.logger.info("Graceful shutdown complete")
        sys.exit(0)
    
    def ensure_table_exists(self):
        """Create BigQuery dataset and table if they don't exist"""
        try:
            # Ensure dataset exists
            try:
                self.bq_client.get_dataset(self.dataset_id)
                self.logger.debug(f"Dataset {self.dataset_id} exists")
            except NotFound:
                dataset = bigquery.Dataset(f"{self.project_id}.{self.dataset_id}")
                dataset.location = "US"
                dataset = self.bq_client.create_dataset(dataset, timeout=30)
                self.logger.info(f"Created dataset {self.dataset_id}")
            
            # Check if table exists
            try:
                table = self.bq_client.get_table(self.table_ref)
                self.logger.info(f"Table {self.table_id} exists ({table.num_rows:,} rows)")
                return table
            except NotFound:
                self.logger.info(f"Table {self.table_id} will be created on first insert")
                return None
                
        except Exception as e:
            self.logger.error(f"BigQuery setup error: {e}")
            return None
    
    def _smart_rate_limit(self):
        """Pure BigQuery-based rate limiting - no artificial delays"""
        # No artificial delays - rely entirely on BigQuery operations as natural buffer
        # This provides typically 0.15-0.35 seconds between requests
        current_time = time.time()
        total_elapsed = current_time - self.last_operation_start if self.last_operation_start > 0 else 0
        
        self.logger.debug(f"Pure BigQuery buffer: {total_elapsed:.2f}s (no artificial delay)")
    
    def _make_request_with_retry(self, url: str, description: str = "", max_retries: int = 3) -> List[Dict]:
        """Make rate-limited API request with retry logic and performance tracking"""
        for attempt in range(max_retries):
            try:
                # Mark start of new operation cycle for intelligent rate limiting
                self.last_operation_start = time.time()
                self._smart_rate_limit()
                
                api_start_time = time.time()
                
                # Use proxy manager if available, otherwise use regular session
                if self.use_proxy_manager and self.proxy_manager:
                    try:
                        # Use proxy manager with automatic switching
                        response = self.proxy_manager.make_request_with_auto_switch(
                            url, method='GET', timeout=30, max_switches=3
                        )
                    except Exception as e:
                        self.logger.warning(f"Proxy manager request failed, falling back to direct: {e}")
                        response = self.session.get(url, timeout=30)
                        response.raise_for_status()
                else:
                    # Use regular session (legacy or direct)
                    response = self.session.get(url, timeout=30)
                    response.raise_for_status()
                
                self.last_request_time = time.time()
                api_elapsed = self.last_request_time - api_start_time
                
                # Handle rate limiting responses
                if response.status_code in [403, 429, 503]:
                    self.logger.warning(f"Rate limit response (HTTP {response.status_code}) for {description}")
                    if attempt < max_retries - 1:
                        backoff = self.config['rate_limiting']['backoff_factor'] ** attempt * 2
                        self.logger.info(f"Backing off for {backoff}s before retry...")
                        time.sleep(backoff)
                        continue
                    else:
                        # Return the response anyway, let caller handle it
                        pass
                
                data = response.json()['results']
                
                self.logger.info(f"API {description}: {len(data)} items in {api_elapsed:.3f}s")
                return data
                
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    self.logger.error(f"API request failed after {max_retries} attempts: {e}")
                    raise
                else:
                    backoff = self.config['rate_limiting']['backoff_factor'] ** attempt
                    self.logger.warning(f"API request failed (attempt {attempt + 1}), retrying in {backoff}s: {e}")
                    time.sleep(backoff)
    
    def stream_to_bigquery(self, rows: List[Dict], force: bool = False):
        """Stream insert rows to BigQuery with batching and error handling"""
        if not rows and not force:
            return 0
            
        self.current_batch.extend(rows)
        
        # Only stream when batch is full or forced
        if len(self.current_batch) >= self.batch_size or (force and self.current_batch):
            start_time = time.time()
            
            try:
                # Create table on first insert if it doesn't exist
                try:
                    table = self.bq_client.get_table(self.table_ref)
                except NotFound:
                    if self.current_batch:
                        table = self._create_table_from_data(self.current_batch[0])
                    else:
                        self.logger.warning("No data to create table schema")
                        return 0
                
                # Stream insert
                errors = self.bq_client.insert_rows_json(table, self.current_batch)
                
                if errors:
                    self.logger.error(f"BigQuery insert errors: {errors}")
                    if self.config['bigquery']['create_backup']:
                        self._save_to_csv_backup(self.current_batch)
                    rows_inserted = 0
                else:
                    rows_inserted = len(self.current_batch)
                    self.total_rows_inserted += rows_inserted
                
                elapsed = time.time() - start_time
                self.logger.info(f"BigQuery: {rows_inserted} rows in {elapsed:.3f}s (providing natural rate buffer)")
                
                # Update performance stats for intelligent rate limiting
                total_cycle_time = time.time() - self.last_operation_start if self.last_operation_start > 0 else elapsed
                self.logger.debug(f"Total operation cycle time: {total_cycle_time:.3f}s (API + BigQuery)")
                
                # Clear batch
                self.current_batch = []
                return rows_inserted
                
            except Exception as e:
                self.logger.error(f"BigQuery error: {e}")
                if self.config['bigquery']['create_backup']:
                    self._save_to_csv_backup(self.current_batch)
                self.current_batch = []
                return 0
        
        return 0
    
    def _create_table_from_data(self, sample_row: Dict) -> bigquery.Table:
        """Create BigQuery table from sample data with partitioning and clustering"""
        self.logger.info(f"Creating table {self.table_id}...")
        
        # Define schema based on sample row
        schema = []
        for key, value in sample_row.items():
            if key == 'update_date':
                field_type = bigquery.enums.SqlTypeNames.DATE
            elif 'id' in key.lower() and isinstance(value, int):
                field_type = bigquery.enums.SqlTypeNames.INTEGER
            elif isinstance(value, float):
                field_type = bigquery.enums.SqlTypeNames.FLOAT
            else:
                field_type = bigquery.enums.SqlTypeNames.STRING
            
            schema.append(bigquery.SchemaField(key, field_type))
        
        # Create table
        table = bigquery.Table(self.table_ref, schema=schema)
        
        # Add partitioning and clustering for better performance
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="update_date"
        )
        
        # Find product ID field for clustering
        product_id_field = None
        for field in schema:
            if 'product' in field.name.lower() and 'id' in field.name.lower():
                product_id_field = field.name
                break
        
        if product_id_field:
            table.clustering_fields = [product_id_field]
        
        table = self.bq_client.create_table(table)
        self.logger.info(f"Created table with {len(schema)} columns")
        
        return table
    
    def _clean_value_for_bigquery(self, value):
        """Clean and convert values for BigQuery compatibility"""
        if value is None:
            return None
        elif isinstance(value, (list, dict)):
            return json.dumps(value)
        elif isinstance(value, bool):
            return str(value).lower()
        elif isinstance(value, (int, float)):
            return value
        elif isinstance(value, str):
            return value
        else:
            return str(value)
    
    def _save_to_csv_backup(self, rows: List[Dict]):
        """Save rows to CSV as backup when BigQuery fails"""
        if not rows:
            return
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = os.path.join(self.data_dir, f"backup_enhanced_{timestamp}.csv")
        
        try:
            df = pd.DataFrame(rows)
            df.to_csv(backup_file, index=False)
            self.logger.info(f"Backup saved: {backup_file}")
        except Exception as e:
            self.logger.error(f"Backup save error: {e}")
    
    def download_categories(self) -> List[Dict]:
        """Download all categories from TCG API"""
        self.logger.info("Downloading categories...")
        categories = self._make_request_with_retry(f"{self.base_url}/categories", "categories")
        self.checkpoint.total_categories = len(categories)
        self._save_checkpoint()
        return categories
    
    def download_groups(self, category_id: str) -> List[Dict]:
        """Download groups for a specific category"""
        groups = self._make_request_with_retry(
            f"{self.base_url}/{category_id}/groups", 
            f"groups for category {category_id}"
        )
        return groups
    
    def download_products(self, category_id: str, group_id: str) -> List[Dict]:
        """Download products for a specific group"""
        products = self._make_request_with_retry(
            f"{self.base_url}/{category_id}/{group_id}/products",
            f"products for group {group_id}"
        )
        return products
    
    def is_group_completed(self, category_id: str, group_id: str) -> bool:
        """Check if a group has already been completed"""
        group_key = f"{category_id}:{group_id}"
        return group_key in self.checkpoint.completed_groups
    
    def mark_group_completed(self, category_id: str, group_id: str):
        """Mark a group as completed"""
        group_key = f"{category_id}:{group_id}"
        self.checkpoint.completed_groups.add(group_key)
        # Remove from failed groups if it was there
        if group_key in self.checkpoint.failed_groups:
            del self.checkpoint.failed_groups[group_key]
        self._save_checkpoint()
    
    def mark_group_failed(self, category_id: str, group_id: str, error: str):
        """Mark a group as failed with error message"""
        group_key = f"{category_id}:{group_id}"
        self.checkpoint.failed_groups[group_key] = error
        self.logger.error(f"Group {group_key} failed: {error}")
        self._save_checkpoint()
    
    def download_and_save_group(self, category: Dict, group: Dict) -> int:
        """Download products for a group and prepare for BigQuery streaming with error handling"""
        category_id = str(category['categoryId'])
        group_id = str(group['groupId'])
        group_name = group.get('name', f'Group {group_id}')
        
        # Check if already completed
        if self.is_group_completed(category_id, group_id):
            self.logger.debug(f"Skipping completed group {category_id}:{group_id}")
            return 0
        
        self.logger.info(f"Processing: {category['name']} / {group_name}")
        
        try:
            # Update checkpoint with current progress
            self.checkpoint.current_category = int(category_id)
            self.checkpoint.current_group = int(group_id)
            
            # Download products for this group
            products = self.download_products(category_id, group_id)
            
            if not products:
                self.logger.debug(f"No products found in group {group_id}")
                self.mark_group_completed(category_id, group_id)
                return 0
            
            # Build denormalized records
            rows_to_insert = []
            for product in products:
                record = {}
                
                # Add category fields with prefix
                for key, value in category.items():
                    record[f'category_{key}'] = self._clean_value_for_bigquery(value)
                
                # Add group fields with prefix
                for key, value in group.items():
                    record[f'group_{key}'] = self._clean_value_for_bigquery(value)
                
                # Add product fields with prefix
                for key, value in product.items():
                    record[f'product_{key}'] = self._clean_value_for_bigquery(value)
                
                # Add metadata
                record['update_date'] = date.today().isoformat()
                
                rows_to_insert.append(record)
            
            self.logger.info(f"Found {len(products)} products in group {group_id}")
            self.checkpoint.total_products += len(products)
            
            # Add to batch for BigQuery streaming
            self.stream_to_bigquery(rows_to_insert)
            
            # Mark group as completed
            self.mark_group_completed(category_id, group_id)
            
            return len(products)
            
        except Exception as e:
            error_msg = f"Error processing group {group_id}: {str(e)}"
            self.logger.error(error_msg)
            self.mark_group_failed(category_id, group_id, error_msg)
            return 0
    
    def download_category(self, category: Dict, skip_completed: bool = True) -> Dict[str, Any]:
        """Download all products for a specific category with resume support"""
        category_id = str(category['categoryId'])
        category_name = category.get('name', f'Category {category_id}')
        
        # Check if category is already completed
        if skip_completed and int(category_id) in self.checkpoint.completed_categories:
            self.logger.info(f"Skipping completed category: {category_name}")
            return {
                'category_id': category_id,
                'category_name': category_name,
                'status': 'skipped',
                'products': 0,
                'groups_processed': 0,
                'groups_failed': 0
            }
        
        self.logger.info(f"Starting category: {category_name} (ID: {category_id})")
        
        try:
            # Download groups for this category
            groups = self.download_groups(category_id)
            self.logger.info(f"Found {len(groups)} groups in category {category_name}")
            
            category_stats = {
                'category_id': category_id,
                'category_name': category_name,
                'total_groups': len(groups),
                'groups_processed': 0,
                'groups_skipped': 0,
                'groups_failed': 0,
                'products': 0,
                'start_time': time.time()
            }
            
            # Process each group
            for grp_idx, group in enumerate(groups, 1):
                group_id = str(group['groupId'])
                group_key = f"{category_id}:{group_id}"
                
                if self.is_group_completed(category_id, group_id):
                    category_stats['groups_skipped'] += 1
                    self.logger.debug(f"[{grp_idx}/{len(groups)}] Skipped completed group {group_id}")
                    continue
                
                try:
                    products_count = self.download_and_save_group(category, group)
                    category_stats['products'] += products_count
                    category_stats['groups_processed'] += 1
                    
                    # Enhanced progress update with performance metrics
                    elapsed = time.time() - category_stats['start_time']
                    rate = category_stats['groups_processed'] / elapsed if elapsed > 0 else 0
                    products_per_second = category_stats['products'] / elapsed if elapsed > 0 else 0
                    eta_minutes = (len(groups) - grp_idx) / rate / 60 if rate > 0 else 0
                    
                    self.logger.info(
                        f"[{grp_idx}/{len(groups)}] +{products_count} products "
                        f"(Total: {category_stats['products']:,}, "
                        f"Rate: {rate:.2f} groups/min, {products_per_second:.1f} products/sec, "
                        f"ETA: {eta_minutes:.1f}min)"
                    )
                    
                except Exception as e:
                    category_stats['groups_failed'] += 1
                    self.mark_group_failed(category_id, group_id, str(e))
                    self.logger.error(f"Failed to process group {group_id}: {e}")
                    continue
            
            # Mark category as completed if all groups processed
            if category_stats['groups_failed'] == 0:
                self.checkpoint.completed_categories.append(int(category_id))
                category_stats['status'] = 'completed'
            else:
                category_stats['status'] = 'partial'
            
            self._save_checkpoint()
            
            elapsed = time.time() - category_stats['start_time']
            self.logger.info(
                f"Category {category_name} completed: "
                f"{category_stats['products']:,} products, "
                f"{category_stats['groups_processed']}/{category_stats['total_groups']} groups, "
                f"{elapsed/60:.1f} minutes"
            )
            
            return category_stats
            
        except Exception as e:
            self.logger.error(f"Category {category_name} failed: {e}")
            return {
                'category_id': category_id,
                'category_name': category_name,
                'status': 'failed',
                'error': str(e),
                'products': 0,
                'groups_processed': 0,
                'groups_failed': 1
            }
    
    def download_all_categories(self, 
                               specific_categories: Optional[List[int]] = None,
                               skip_completed: bool = True) -> Dict[str, Any]:
        """Download all categories with comprehensive resume support"""
        
        start_time = time.time()
        self.logger.info("=== STARTING ENHANCED TCG METADATA DOWNLOAD ===")
        
        # Ensure BigQuery table exists
        self.ensure_table_exists()
        
        # Download categories
        all_categories = self.download_categories()
        
        # Filter to specific categories if requested
        if specific_categories:
            categories = [cat for cat in all_categories if cat['categoryId'] in specific_categories]
            self.logger.info(f"Filtered to {len(categories)} specific categories: {specific_categories}")
        else:
            categories = all_categories
            self.logger.info(f"Processing all {len(categories)} categories")
        
        # Track overall statistics
        overall_stats = {
            'start_time': start_time,
            'categories_processed': 0,
            'categories_skipped': 0,
            'categories_failed': 0,
            'total_products': 0,
            'total_groups': 0,
            'category_results': []
        }
        
        # Process each category
        for cat_idx, category in enumerate(categories, 1):
            category_id = category['categoryId']
            category_name = category.get('name', f'Category {category_id}')
            
            self.logger.info(f"\n=== [{cat_idx}/{len(categories)}] CATEGORY: {category_name} ===")
            
            # Download category
            result = self.download_category(category, skip_completed)
            overall_stats['category_results'].append(result)
            
            # Update overall statistics
            if result['status'] == 'completed':
                overall_stats['categories_processed'] += 1
            elif result['status'] == 'skipped':
                overall_stats['categories_skipped'] += 1
            else:
                overall_stats['categories_failed'] += 1
            
            overall_stats['total_products'] += result.get('products', 0)
            overall_stats['total_groups'] += result.get('groups_processed', 0)
            
            # Print progress summary
            elapsed_hours = (time.time() - start_time) / 3600
            self.logger.info(
                f"Progress: {cat_idx}/{len(categories)} categories, "
                f"{overall_stats['total_products']:,} products, "
                f"{elapsed_hours:.1f}h elapsed"
            )
        
        # Flush remaining batch
        self.logger.info("Flushing final batch...")
        self.stream_to_bigquery([], force=True)
        
        # Final statistics
        self._print_final_stats(overall_stats)
        
        return overall_stats
    
    def _print_final_stats(self, stats: Dict):
        """Print comprehensive final statistics"""
        elapsed = time.time() - stats['start_time']
        
        self.logger.info("=" * 80)
        self.logger.info("🎉 ENHANCED DOWNLOAD COMPLETE")
        self.logger.info("=" * 80)
        self.logger.info(f"📊 Summary:")
        self.logger.info(f"   Categories processed: {stats['categories_processed']:,}")
        self.logger.info(f"   Categories skipped: {stats['categories_skipped']:,}")
        self.logger.info(f"   Categories failed: {stats['categories_failed']:,}")
        self.logger.info(f"   Total products: {stats['total_products']:,}")
        self.logger.info(f"   Total groups: {stats['total_groups']:,}")
        self.logger.info(f"   BigQuery rows inserted: {self.total_rows_inserted:,}")
        self.logger.info(f"")
        self.logger.info(f"⏱️  Performance:")
        self.logger.info(f"   Total time: {elapsed/3600:.2f} hours")
        self.logger.info(f"   Products/hour: {stats['total_products']/(elapsed/3600):.0f}")
        self.logger.info(f"   Groups/minute: {stats['total_groups']/(elapsed/60):.1f}")
        
        # Failed groups summary
        if self.checkpoint.failed_groups:
            self.logger.warning(f"❌ Failed groups: {len(self.checkpoint.failed_groups)}")
            for group_key, error in self.checkpoint.failed_groups.items():
                self.logger.warning(f"   {group_key}: {error}")
        
        self.logger.info("=" * 80)
    
    def retry_failed_groups(self) -> int:
        """Retry all previously failed groups"""
        if not self.checkpoint.failed_groups:
            self.logger.info("No failed groups to retry")
            return 0
        
        self.logger.info(f"Retrying {len(self.checkpoint.failed_groups)} failed groups...")
        
        # Get all categories to have context
        categories = self.download_categories()
        cat_lookup = {str(cat['categoryId']): cat for cat in categories}
        
        retry_count = 0
        failed_groups_copy = dict(self.checkpoint.failed_groups)
        
        for group_key in failed_groups_copy:
            try:
                category_id, group_id = group_key.split(':')
                
                if category_id not in cat_lookup:
                    self.logger.warning(f"Category {category_id} not found, skipping group {group_id}")
                    continue
                
                category = cat_lookup[category_id]
                
                # Get group info
                groups = self.download_groups(category_id)
                group = None
                for g in groups:
                    if str(g['groupId']) == group_id:
                        group = g
                        break
                
                if not group:
                    self.logger.warning(f"Group {group_id} not found in category {category_id}")
                    continue
                
                # Retry the group
                self.logger.info(f"Retrying group: {group_key}")
                products_count = self.download_and_save_group(category, group)
                
                if products_count >= 0:  # Success (even if 0 products)
                    retry_count += 1
                    self.logger.info(f"Retry successful: {group_key} ({products_count} products)")
                
            except Exception as e:
                self.logger.error(f"Retry failed for {group_key}: {e}")
        
        # Flush batch
        self.stream_to_bigquery([], force=True)
        
        self.logger.info(f"Retry complete: {retry_count}/{len(failed_groups_copy)} groups recovered")
        return retry_count
    
    def get_resume_status(self) -> Dict[str, Any]:
        """Get current resume status and progress"""
        total_completed = len(self.checkpoint.completed_groups)
        total_failed = len(self.checkpoint.failed_groups)
        
        return {
            'checkpoint_file': self.checkpoint_file,
            'started_at': self.checkpoint.started_at,
            'last_updated': self.checkpoint.last_updated,
            'completed_categories': len(self.checkpoint.completed_categories),
            'completed_groups': total_completed,
            'failed_groups': total_failed,
            'total_products': self.checkpoint.total_products,
            'current_category': self.checkpoint.current_category,
            'current_group': self.checkpoint.current_group,
            'can_resume': total_completed > 0 or total_failed > 0
        }
    
    def print_resume_status(self):
        """Print detailed resume status"""
        status = self.get_resume_status()
        
        print("=" * 60)
        print("📊 DOWNLOAD RESUME STATUS")
        print("=" * 60)
        print(f"Checkpoint file: {status['checkpoint_file']}")
        print(f"Started at: {status['started_at']}")
        print(f"Last updated: {status['last_updated']}")
        print(f"")
        print(f"Progress:")
        print(f"  ✅ Completed categories: {status['completed_categories']}")
        print(f"  ✅ Completed groups: {status['completed_groups']:,}")
        print(f"  ❌ Failed groups: {status['failed_groups']}")
        print(f"  📦 Total products: {status['total_products']:,}")
        print(f"")
        
        if status['current_category']:
            print(f"Last position:")
            print(f"  Category: {status['current_category']}")
            print(f"  Group: {status['current_group']}")
        
        if status['can_resume']:
            print(f"✅ Resume available: Yes")
        else:
            print(f"ℹ️  Resume available: No (fresh start)")
        
        # Show proxy status if proxy manager is enabled
        if self.use_proxy_manager and self.proxy_manager:
            print(f"")
            print(f"🔄 Proxy Status:")
            try:
                current_proxy = self.proxy_manager.get_current_proxy()
                stats = self.proxy_manager.get_proxy_statistics()
                print(f"  Current proxy: {current_proxy}")
                print(f"  Available proxies: {stats['total_proxies']}")
                print(f"  Healthy proxies: {stats['healthy_proxies']}")
                print(f"  Total requests: {stats['summary']['total_requests']}")
                if stats['summary']['total_requests'] > 0:
                    success_rate = (stats['summary']['total_successes'] / stats['summary']['total_requests']) * 100
                    print(f"  Success rate: {success_rate:.1f}%")
                    print(f"  Rate limits: {stats['summary']['total_rate_limits']}")
            except Exception as e:
                print(f"  Status unavailable: {e}")
        
        print("=" * 60)
    
    def get_proxy_statistics(self) -> Dict[str, Any]:
        """Get proxy manager statistics if available"""
        if self.use_proxy_manager and self.proxy_manager:
            try:
                return self.proxy_manager.get_proxy_statistics()
            except Exception as e:
                return {'error': str(e)}
        else:
            return {'proxy_manager': 'disabled'}