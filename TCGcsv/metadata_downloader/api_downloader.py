#!/usr/bin/env python3
"""
TCG API Downloader - Direct API approach with BigQuery streaming
Uses tcgcsv.com API endpoints with rate limiting and real-time BigQuery saves
"""
import requests
import pandas as pd
import time
import json
import os
from datetime import datetime, date
from typing import Dict, List, Any, Optional
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

class TCGAPIDownloader:
    def __init__(self, project_id: Optional[str] = None, dataset_id: str = "tcg_data", 
                 min_request_interval: float = 1.2, batch_size: int = 500):
        """
        Initialize TCG API downloader with BigQuery streaming
        
        Args:
            project_id: GCP project ID (None for default)
            dataset_id: BigQuery dataset name
            min_request_interval: Minimum seconds between API requests (1.2 = 0.83 req/s)
            batch_size: Number of rows to batch before streaming to BigQuery
        """
        self.base_url = "https://tcgcsv.com/tcgplayer"
        self.min_request_interval = min_request_interval
        self.last_request_time = 0
        self.data_dir = "data"
        
        # Create data directory
        os.makedirs(self.data_dir, exist_ok=True)
        
        # BigQuery setup
        self.bq_client = bigquery.Client(project=project_id)
        self.project_id = project_id or self.bq_client.project
        self.dataset_id = dataset_id
        self.table_id = "tcg_metadata"
        self.table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        
        # Batching for efficient streaming
        self.batch_size = batch_size
        self.current_batch = []
        self.total_rows_inserted = 0
        
        # Statistics
        self.stats = {
            'total_requests': 0,
            'total_products': 0,
            'total_groups': 0,
            'total_categories': 0,
            'start_time': None
        }
        
        print(f"Initializing TCG API Downloader")
        print(f"  Target: {self.table_ref}")
        print(f"  Rate limit: {1/min_request_interval:.2f} req/s")
        print(f"  Batch size: {batch_size} rows")
        
    def ensure_table_exists(self):
        """Create BigQuery dataset and table if they don't exist"""
        try:
            # Ensure dataset exists
            try:
                self.bq_client.get_dataset(self.dataset_id)
                print(f"  Dataset {self.dataset_id} exists")
            except NotFound:
                dataset = bigquery.Dataset(f"{self.project_id}.{self.dataset_id}")
                dataset.location = "US"
                dataset = self.bq_client.create_dataset(dataset, timeout=30)
                print(f"  Created dataset {self.dataset_id}")
            
            # Check if table exists
            try:
                table = self.bq_client.get_table(self.table_ref)
                print(f"  Table {self.table_id} exists ({table.num_rows:,} rows)")
                return table
            except NotFound:
                print(f"  Table {self.table_id} will be created on first insert")
                return None
                
        except Exception as e:
            print(f"  Warning: BigQuery setup error: {e}")
            return None
    
    def _smart_rate_limit(self):
        """Ensure minimum interval between requests"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            remaining = self.min_request_interval - elapsed
            print(f"    Rate limit: waiting {remaining:.2f}s")
            time.sleep(remaining)
    
    def _make_request(self, url: str, description: str = "") -> List[Dict]:
        """Make rate-limited API request"""
        self._smart_rate_limit()
        
        start_time = time.time()
        try:
            response = requests.get(url)
            response.raise_for_status()
            
            self.last_request_time = time.time()
            elapsed = self.last_request_time - start_time
            
            data = response.json()['results']
            self.stats['total_requests'] += 1
            
            print(f"    API {description}: {len(data)} items in {elapsed:.2f}s")
            return data
            
        except requests.exceptions.RequestException as e:
            print(f"    API Error {description}: {e}")
            raise
    
    def stream_to_bigquery(self, rows: List[Dict], force: bool = False):
        """
        Stream insert rows to BigQuery with batching
        
        Args:
            rows: List of row dictionaries to insert
            force: Force insert even if batch is not full
        """
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
                    # Create table from first batch schema
                    if self.current_batch:
                        table = self._create_table_from_data(self.current_batch[0])
                    else:
                        print("    No data to create table schema")
                        return 0
                
                # Stream insert
                errors = self.bq_client.insert_rows_json(table, self.current_batch)
                
                if errors:
                    print(f"    BigQuery insert errors: {errors}")
                    # Save to CSV as backup
                    self._save_to_csv_backup(self.current_batch)
                    rows_inserted = 0
                else:
                    rows_inserted = len(self.current_batch)
                    self.total_rows_inserted += rows_inserted
                
                elapsed = time.time() - start_time
                print(f"    BigQuery: {rows_inserted} rows in {elapsed:.2f}s")
                
                # Clear batch
                self.current_batch = []
                return rows_inserted
                
            except Exception as e:
                print(f"    BigQuery error: {e}")
                self._save_to_csv_backup(self.current_batch)
                self.current_batch = []
                return 0
        
        return 0
    
    def _create_table_from_data(self, sample_row: Dict) -> bigquery.Table:
        """Create BigQuery table from sample data"""
        print(f"    Creating table {self.table_id}...")
        
        # Define schema based on sample row
        schema = []
        for key, value in sample_row.items():
            if key == 'update_date':
                field_type = bigquery.enums.SqlTypeNames.DATE
            elif 'id' in key.lower() and isinstance(value, int):
                # Keep ID fields as integers
                field_type = bigquery.enums.SqlTypeNames.INTEGER
            elif isinstance(value, float):
                field_type = bigquery.enums.SqlTypeNames.FLOAT
            else:
                # Convert everything else to string for simplicity
                # This includes booleans, arrays, objects converted to JSON strings
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
        print(f"    Created table with {len(schema)} columns")
        
        return table
    
    def _clean_value_for_bigquery(self, value):
        """Clean and convert values for BigQuery compatibility"""
        if value is None:
            return None
        elif isinstance(value, (list, dict)):
            # Convert complex types to JSON strings
            return json.dumps(value)
        elif isinstance(value, bool):
            # Convert booleans to strings to avoid type conflicts
            return str(value).lower()
        elif isinstance(value, (int, float)):
            return value
        elif isinstance(value, str):
            return value
        else:
            # Convert any other type to string
            return str(value)
    
    def _save_to_csv_backup(self, rows: List[Dict]):
        """Save rows to CSV as backup when BigQuery fails"""
        if not rows:
            return
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = os.path.join(self.data_dir, f"backup_{timestamp}.csv")
        
        try:
            df = pd.DataFrame(rows)
            df.to_csv(backup_file, index=False)
            print(f"    Backup saved: {backup_file}")
        except Exception as e:
            print(f"    Backup save error: {e}")
    
    def download_categories(self) -> List[Dict]:
        """Download all categories from TCG API"""
        print("\n📁 Downloading categories...")
        categories = self._make_request(f"{self.base_url}/categories", "categories")
        self.stats['total_categories'] = len(categories)
        return categories
    
    def download_groups(self, category_id: str) -> List[Dict]:
        """Download groups for a specific category"""
        groups = self._make_request(f"{self.base_url}/{category_id}/groups", 
                                   f"groups for category {category_id}")
        return groups
    
    def download_products(self, category_id: str, group_id: str) -> List[Dict]:
        """Download products for a specific group"""
        products = self._make_request(f"{self.base_url}/{category_id}/{group_id}/products",
                                     f"products for group {group_id}")
        return products
    
    def download_and_save_group(self, category: Dict, group: Dict) -> int:
        """
        Download products for a group and prepare for BigQuery streaming
        
        Returns:
            Number of products processed
        """
        category_id = str(category['categoryId'])
        group_id = str(group['groupId'])
        group_name = group.get('name', f'Group {group_id}')
        
        print(f"\n🔍 Processing: {category['name']} / {group_name}")
        
        # Download products for this group
        products = self.download_products(category_id, group_id)
        
        if not products:
            print(f"    No products found")
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
        
        print(f"    Found {len(products)} products")
        self.stats['total_products'] += len(products)
        
        # Add to batch for BigQuery streaming
        self.stream_to_bigquery(rows_to_insert)
        
        return len(products)
    
    def download_all(self, limit_categories: Optional[int] = None, 
                     limit_groups_per_category: Optional[int] = None) -> int:
        """
        Download all TCG data with optional limits for testing
        
        Args:
            limit_categories: Limit number of categories (None for all)
            limit_groups_per_category: Limit groups per category (None for all)
            
        Returns:
            Total number of products downloaded
        """
        self.stats['start_time'] = time.time()
        
        print("🚀 Starting TCG API download...")
        print(f"   Rate limit: {1/self.min_request_interval:.2f} req/s")
        
        # Ensure BigQuery table exists
        self.ensure_table_exists()
        
        # Download categories
        categories = self.download_categories()
        
        if limit_categories:
            categories = categories[:limit_categories]
            print(f"   Limited to first {limit_categories} categories")
        
        print(f"   Processing {len(categories)} categories")
        
        # Process each category
        for cat_idx, category in enumerate(categories, 1):
            category_id = str(category['categoryId'])
            category_name = category.get('name', f'Category {category_id}')
            
            print(f"\n📂 [{cat_idx}/{len(categories)}] Category: {category_name}")
            
            # Download groups for this category
            groups = self.download_groups(category_id)
            
            if limit_groups_per_category:
                groups = groups[:limit_groups_per_category]
                print(f"     Limited to first {limit_groups_per_category} groups")
            
            print(f"     Found {len(groups)} groups")
            self.stats['total_groups'] += len(groups)
            
            # Process each group
            for grp_idx, group in enumerate(groups, 1):
                products_count = self.download_and_save_group(category, group)
                
                # Progress update
                elapsed = time.time() - self.stats['start_time']
                total_requests = self.stats['total_requests']
                req_rate = total_requests / elapsed if elapsed > 0 else 0
                
                print(f"     [{grp_idx}/{len(groups)}] +{products_count} products "
                      f"(Total: {self.stats['total_products']:,}, Rate: {req_rate:.2f} req/s)")
        
        # Flush remaining batch
        print(f"\n💾 Flushing final batch...")
        self.stream_to_bigquery([], force=True)
        
        # Final statistics
        self._print_final_stats()
        
        return self.stats['total_products']
    
    def _print_final_stats(self):
        """Print final download statistics"""
        elapsed = time.time() - self.stats['start_time']
        
        print(f"\n{'='*60}")
        print(f"🎉 DOWNLOAD COMPLETE")
        print(f"{'='*60}")
        print(f"📊 Statistics:")
        print(f"   Categories: {self.stats['total_categories']:,}")
        print(f"   Groups: {self.stats['total_groups']:,}")
        print(f"   Products: {self.stats['total_products']:,}")
        print(f"   API Requests: {self.stats['total_requests']:,}")
        print(f"   BigQuery Rows: {self.total_rows_inserted:,}")
        print(f"")
        print(f"⏱️  Performance:")
        print(f"   Total time: {elapsed/60:.1f} minutes")
        print(f"   Request rate: {self.stats['total_requests']/elapsed:.2f} req/s")
        print(f"   Products/sec: {self.stats['total_products']/elapsed:.1f}")
        print(f"{'='*60}")

if __name__ == "__main__":
    # Simple test
    downloader = TCGAPIDownloader()
    downloader.download_all(limit_categories=1, limit_groups_per_category=2)