#!/usr/bin/env python3
import os
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import glob
from typing import Dict, Any, Optional
from datetime import datetime

class BigQueryPriceLoader:
    def __init__(self, project_id: str = None, dataset_id: str = "tcg_data"):
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id or self.client.project
        self.dataset_id = dataset_id
        self.dataset_ref = self.client.dataset(dataset_id)
        self.table_name = "tcg_prices"
        
        self.ensure_dataset_exists()
    
    def ensure_dataset_exists(self):
        """Create dataset if it doesn't exist"""
        try:
            self.client.get_dataset(self.dataset_ref)
            print(f"Dataset {self.dataset_id} already exists")
        except NotFound:
            dataset = bigquery.Dataset(self.dataset_ref)
            dataset.location = "US"
            dataset = self.client.create_dataset(dataset, timeout=30)
            print(f"Created dataset {self.dataset_id}")
    
    def get_price_table_schema(self) -> list:
        """Create BigQuery schema for price table with partitioning optimization"""
        schema = [
            bigquery.SchemaField("price_date", bigquery.enums.SqlTypeNames.DATE),
            bigquery.SchemaField("product_id", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("sub_type_name", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("low_price", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField("mid_price", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField("high_price", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField("market_price", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField("direct_low_price", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField("category_id", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("group_id", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("update_timestamp", bigquery.enums.SqlTypeNames.TIMESTAMP),
        ]
        
        return schema
    
    def table_exists(self) -> bool:
        """Check if the price table already exists"""
        try:
            table_ref = self.dataset_ref.table(self.table_name)
            self.client.get_table(table_ref)
            return True
        except NotFound:
            return False
    
    def create_price_table(self):
        """Create the price table with partitioning and clustering"""
        table_ref = self.dataset_ref.table(self.table_name)
        
        table = bigquery.Table(table_ref, schema=self.get_price_table_schema())
        
        # Partition by price_date for efficient querying
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="price_date"
        )
        
        # Cluster by product_id for efficient lookups
        table.clustering_fields = ["product_id"]
        
        # Create the table
        table = self.client.create_table(table)
        print(f"Created partitioned table {self.project_id}.{self.dataset_id}.{self.table_name}")
        print(f"  - Partitioned by: price_date (daily)")
        print(f"  - Clustered by: product_id")
    
    def drop_table_if_exists(self):
        """Drop the price table if it exists"""
        try:
            table_ref = self.dataset_ref.table(self.table_name)
            self.client.delete_table(table_ref)
            print(f"Dropped existing table {self.table_name}")
            return True
        except NotFound:
            print(f"Table {self.table_name} doesn't exist, nothing to drop")
            return False
    
    def load_price_data(self, csv_path: str = None, price_date: str = None, force_recreate: bool = False) -> bool:
        """Load price data to BigQuery table with deduplication"""
        
        # Find CSV file if not specified
        if csv_path is None:
            csv_files = glob.glob(os.path.join("data", "tcg_prices_*.csv"))
            if not csv_files:
                print("No price CSV files found in data directory")
                return False
            csv_path = csv_files[-1]  # Use most recent
            print(f"Using CSV file: {csv_path}")
        
        if not os.path.exists(csv_path):
            print(f"CSV file not found: {csv_path}")
            return False
        
        # Load and validate data
        df = pd.read_csv(csv_path)
        if df.empty:
            print(f"CSV file is empty: {csv_path}")
            return False
        
        print(f"Loading {len(df):,} price records from {csv_path}...")
        
        # Convert data types
        df['price_date'] = pd.to_datetime(df['price_date']).dt.date
        df['update_timestamp'] = pd.to_datetime(df['update_timestamp'])
        
        # Handle force recreate
        if force_recreate:
            self.drop_table_if_exists()
        
        table_ref = self.dataset_ref.table(self.table_name)
        
        # Create table if it doesn't exist
        if not self.table_exists():
            self.create_price_table()
        
        # Load data using WRITE_APPEND with deduplication
        job_config = bigquery.LoadJobConfig(
            schema=self.get_price_table_schema(),
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        
        print(f"Loading data to {self.table_name}...")
        
        # Check if data for this date already exists
        if price_date:
            existing_data = self.check_existing_data(price_date)
            if existing_data > 0:
                print(f"Warning: Found {existing_data:,} existing records for {price_date}")
                print("This will create duplicates. Consider using replace_date_data() instead.")
        
        job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for job to complete
        
        table = self.client.get_table(table_ref)
        print(f"Successfully loaded to {self.project_id}.{self.dataset_id}.{self.table_name}")
        print(f"Table now has {table.num_rows:,} total rows")
        
        return True
    
    def replace_date_data(self, csv_path: str, price_date: str) -> bool:
        """Replace data for a specific date (delete + insert)"""
        
        if not os.path.exists(csv_path):
            print(f"CSV file not found: {csv_path}")
            return False
        
        # Load new data
        df = pd.read_csv(csv_path)
        if df.empty:
            print(f"CSV file is empty: {csv_path}")
            return False
        
        df['price_date'] = pd.to_datetime(df['price_date']).dt.date
        df['update_timestamp'] = pd.to_datetime(df['update_timestamp'])
        
        print(f"Replacing data for {price_date} with {len(df):,} records...")
        
        # Create table if it doesn't exist
        if not self.table_exists():
            self.create_price_table()
            return self.load_price_data(csv_path, price_date)
        
        table_ref = self.dataset_ref.table(self.table_name)
        
        # Delete existing data for this date
        delete_query = f"""
        DELETE FROM `{self.project_id}.{self.dataset_id}.{self.table_name}`
        WHERE price_date = '{price_date}'
        """
        
        print(f"Deleting existing data for {price_date}...")
        delete_job = self.client.query(delete_query)
        delete_job.result()
        
        deleted_rows = delete_job.num_dml_affected_rows
        print(f"Deleted {deleted_rows:,} existing records for {price_date}")
        
        # Insert new data
        job_config = bigquery.LoadJobConfig(
            schema=self.get_price_table_schema(),
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        
        job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        
        table = self.client.get_table(table_ref)
        print(f"Successfully replaced data for {price_date}")
        print(f"Table now has {table.num_rows:,} total rows")
        
        return True
    
    def check_existing_data(self, price_date: str) -> int:
        """Check if data exists for a specific date"""
        if not self.table_exists():
            return 0
        
        query = f"""
        SELECT COUNT(*) as record_count
        FROM `{self.project_id}.{self.dataset_id}.{self.table_name}`
        WHERE price_date = '{price_date}'
        """
        
        try:
            result = self.client.query(query).result()
            for row in result:
                return row.record_count
        except Exception as e:
            print(f"Error checking existing data: {e}")
            return 0
        
        return 0
    
    def query_table_info(self):
        """Display information about the price table"""
        try:
            table_ref = self.dataset_ref.table(self.table_name)
            table = self.client.get_table(table_ref)
            
            print(f"\nTable: {self.project_id}.{self.dataset_id}.{self.table_name}")
            print(f"  Rows: {table.num_rows:,}")
            print(f"  Columns: {len(table.schema):,}")
            print(f"  Size: {table.num_bytes / (1024*1024):.2f} MB")
            print(f"  Partitioned: {table.time_partitioning.field if table.time_partitioning else 'No'}")
            print(f"  Clustered: {', '.join(table.clustering_fields) if table.clustering_fields else 'No'}")
            
        except NotFound:
            print(f"Table {self.table_name} not found in dataset {self.dataset_id}")
    
    def run_sample_queries(self):
        """Run sample queries to verify price data"""
        if not self.table_exists():
            print("Price table doesn't exist yet")
            return
        
        queries = [
            ("Total records", f"SELECT COUNT(*) as total_records FROM `{self.table_name}`"),
            ("Date range", f"SELECT MIN(price_date) as earliest_date, MAX(price_date) as latest_date FROM `{self.table_name}`"),
            ("Unique products", f"SELECT COUNT(DISTINCT product_id) as unique_products FROM `{self.table_name}`"),
            ("Categories and groups", f"SELECT COUNT(DISTINCT category_id) as categories, COUNT(DISTINCT group_id) as group_count FROM `{self.table_name}`"),
            ("Sample records", f"SELECT price_date, product_id, sub_type_name, market_price, category_id, group_id FROM `{self.table_name}` WHERE market_price IS NOT NULL ORDER BY market_price DESC LIMIT 3"),
        ]
        
        for query_name, query in queries:
            try:
                full_query = query.replace(f"`{self.table_name}`", f"`{self.project_id}.{self.dataset_id}.{self.table_name}`")
                result = self.client.query(full_query).result()
                
                print(f"\n{query_name}:")
                for row in result:
                    print(f"  {dict(row)}")
            except Exception as e:
                print(f"Error running {query_name}: {e}")
    
    def get_daily_price_query(self, target_date: str) -> str:
        """Return query to get all prices for a specific date"""
        return f"""
        SELECT *
        FROM `{self.project_id}.{self.dataset_id}.{self.table_name}`
        WHERE price_date = '{target_date}'
        ORDER BY category_id, group_id, product_id, sub_type_name
        """
    
    def get_price_trends_query(self, product_id: int, days: int = 7) -> str:
        """Return query to get price trends for a product over time"""
        return f"""
        SELECT 
            price_date,
            sub_type_name,
            low_price,
            market_price,
            high_price
        FROM `{self.project_id}.{self.dataset_id}.{self.table_name}`
        WHERE product_id = {product_id}
            AND price_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
        ORDER BY price_date DESC, sub_type_name
        """

if __name__ == "__main__":
    loader = BigQueryPriceLoader()
    
    # Test loading most recent price file
    csv_files = glob.glob(os.path.join("data", "tcg_prices_*.csv"))
    if csv_files:
        latest_csv = max(csv_files, key=os.path.getctime)
        print(f"Testing with: {latest_csv}")
        
        success = loader.load_price_data(latest_csv, force_recreate=True)
        if success:
            loader.query_table_info()
            loader.run_sample_queries()
        else:
            print("Failed to load price data")
    else:
        print("No price CSV files found. Run price_downloader.py first.")