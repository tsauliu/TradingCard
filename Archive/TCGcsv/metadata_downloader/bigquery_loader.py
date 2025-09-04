#!/usr/bin/env python3
"""
BigQuery Loader for TCG Metadata
Handles table creation, schema management, and data verification
"""
import os
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from typing import Dict, Any, List, Optional

class BigQueryMetadataLoader:
    def __init__(self, project_id: Optional[str] = None, dataset_id: str = "tcg_data"):
        """
        Initialize BigQuery loader for metadata
        
        Args:
            project_id: GCP project ID (None for default)
            dataset_id: BigQuery dataset name
        """
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id or self.client.project
        self.dataset_id = dataset_id
        self.dataset_ref = self.client.dataset(dataset_id)
        self.table_name = "tcg_metadata"
        self.table_ref = f"{self.project_id}.{dataset_id}.{self.table_name}"
        
        print(f"BigQuery Loader initialized")
        print(f"  Project: {self.project_id}")
        print(f"  Dataset: {dataset_id}")
        print(f"  Table: {self.table_name}")
        
        self.ensure_dataset_exists()
    
    def ensure_dataset_exists(self):
        """Create dataset if it doesn't exist"""
        try:
            self.client.get_dataset(self.dataset_ref)
            print(f"  Dataset {self.dataset_id} exists")
        except NotFound:
            dataset = bigquery.Dataset(self.dataset_ref)
            dataset.location = "US"
            dataset.description = "TCG product and price data from tcgcsv.com API"
            dataset = self.client.create_dataset(dataset, timeout=30)
            print(f"  Created dataset {self.dataset_id}")
    
    def get_table_info(self) -> Dict[str, Any]:
        """Get information about the API table"""
        try:
            table = self.client.get_table(self.table_ref)
            return {
                'exists': True,
                'num_rows': table.num_rows,
                'num_bytes': table.num_bytes,
                'created': table.created,
                'modified': table.modified,
                'schema_fields': len(table.schema),
                'partitioned': table.time_partitioning is not None,
                'clustered': table.clustering_fields is not None
            }
        except NotFound:
            return {'exists': False}
    
    def verify_data(self, limit: int = 10) -> Dict[str, Any]:
        """
        Verify data exists and show sample rows
        
        Args:
            limit: Number of sample rows to return
            
        Returns:
            Dictionary with verification results
        """
        try:
            # Check if table exists
            table_info = self.get_table_info()
            if not table_info['exists']:
                return {
                    'success': False,
                    'error': 'Table does not exist',
                    'sample_rows': []
                }
            
            # Query for sample data
            query = f"""
            SELECT 
                category_name,
                category_categoryId,
                group_name,
                group_groupId,
                product_name,
                product_productId,
                update_date,
                COUNT(*) OVER() as total_rows
            FROM `{self.table_ref}`
            ORDER BY category_categoryId, group_groupId, product_productId
            LIMIT {limit}
            """
            
            results = self.client.query(query).result()
            rows = list(results)
            
            if not rows:
                return {
                    'success': False,
                    'error': 'Table exists but contains no data',
                    'table_info': table_info,
                    'sample_rows': []
                }
            
            # Convert rows to dictionaries
            sample_rows = []
            total_rows = 0
            for row in rows:
                total_rows = row['total_rows']
                sample_rows.append({
                    'category_name': row['category_name'],
                    'category_id': row['category_categoryId'],
                    'group_name': row['group_name'],
                    'group_id': row['group_groupId'],
                    'product_name': row['product_name'],
                    'product_id': row['product_productId'],
                    'update_date': str(row['update_date'])
                })
            
            return {
                'success': True,
                'total_rows': total_rows,
                'table_info': table_info,
                'sample_rows': sample_rows
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'sample_rows': []
            }
    
    def get_category_summary(self) -> List[Dict[str, Any]]:
        """Get summary statistics by category"""
        try:
            query = f"""
            SELECT 
                category_name,
                category_categoryId,
                COUNT(DISTINCT group_groupId) as groups_count,
                COUNT(*) as products_count,
                MIN(update_date) as first_update,
                MAX(update_date) as last_update
            FROM `{self.table_ref}`
            GROUP BY category_categoryId, category_name
            ORDER BY products_count DESC
            """
            
            results = self.client.query(query).result()
            
            summary = []
            for row in results:
                summary.append({
                    'category_name': row['category_name'],
                    'category_id': row['category_categoryId'],
                    'groups_count': row['groups_count'],
                    'products_count': row['products_count'],
                    'first_update': str(row['first_update']),
                    'last_update': str(row['last_update'])
                })
            
            return summary
            
        except Exception as e:
            print(f"Error getting category summary: {e}")
            return []
    
    def get_recent_activity(self, days: int = 7) -> Dict[str, Any]:
        """Get recent download activity"""
        try:
            query = f"""
            SELECT 
                update_date,
                COUNT(*) as products_added,
                COUNT(DISTINCT category_categoryId) as categories_updated,
                COUNT(DISTINCT group_groupId) as groups_updated
            FROM `{self.table_ref}`
            WHERE update_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
            GROUP BY update_date
            ORDER BY update_date DESC
            """
            
            results = self.client.query(query).result()
            
            activity = []
            total_products = 0
            for row in results:
                total_products += row['products_added']
                activity.append({
                    'date': str(row['update_date']),
                    'products_added': row['products_added'],
                    'categories_updated': row['categories_updated'],
                    'groups_updated': row['groups_updated']
                })
            
            return {
                'success': True,
                'days': days,
                'total_products': total_products,
                'daily_activity': activity
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def delete_data_by_date(self, target_date: str):
        """Delete data for a specific date (for re-running downloads)"""
        try:
            query = f"""
            DELETE FROM `{self.table_ref}`
            WHERE update_date = '{target_date}'
            """
            
            job = self.client.query(query)
            job.result()  # Wait for completion
            
            print(f"Deleted data for date: {target_date}")
            return True
            
        except Exception as e:
            print(f"Error deleting data for {target_date}: {e}")
            return False
    
    def print_table_summary(self):
        """Print a comprehensive table summary"""
        print("\n" + "="*60)
        print("📊 BIGQUERY TABLE SUMMARY")
        print("="*60)
        
        # Table info
        table_info = self.get_table_info()
        if not table_info['exists']:
            print("❌ Table does not exist")
            return
        
        print(f"✅ Table: {self.table_ref}")
        print(f"   Rows: {table_info['num_rows']:,}")
        print(f"   Size: {table_info['num_bytes'] / (1024*1024):.2f} MB")
        print(f"   Schema: {table_info['schema_fields']} columns")
        print(f"   Created: {table_info['created']}")
        print(f"   Modified: {table_info['modified']}")
        print(f"   Partitioned: {'Yes' if table_info['partitioned'] else 'No'}")
        print(f"   Clustered: {'Yes' if table_info['clustered'] else 'No'}")
        
        # Category summary
        print(f"\n📁 Categories Summary:")
        categories = self.get_category_summary()
        if categories:
            for cat in categories[:10]:  # Top 10 categories
                print(f"   {cat['category_name']}: {cat['products_count']:,} products, "
                      f"{cat['groups_count']} groups")
            
            if len(categories) > 10:
                print(f"   ... and {len(categories) - 10} more categories")
        
        # Recent activity
        print(f"\n📅 Recent Activity (last 7 days):")
        activity = self.get_recent_activity(7)
        if activity['success'] and activity['daily_activity']:
            for day in activity['daily_activity']:
                print(f"   {day['date']}: {day['products_added']:,} products, "
                      f"{day['categories_updated']} categories")
        else:
            print("   No recent activity")
        
        print("="*60)
    
    def run_sample_queries(self):
        """Run some sample queries to demonstrate the data"""
        print("\n" + "="*60)
        print("🔍 SAMPLE QUERIES")
        print("="*60)
        
        queries = [
            {
                'name': 'Top 5 categories by product count',
                'query': f"""
                SELECT 
                    category_name,
                    COUNT(*) as product_count
                FROM `{self.table_ref}`
                GROUP BY category_name
                ORDER BY product_count DESC
                LIMIT 5
                """
            },
            {
                'name': 'Most recent groups added',
                'query': f"""
                SELECT 
                    category_name,
                    group_name,
                    COUNT(*) as products,
                    MAX(update_date) as last_update
                FROM `{self.table_ref}`
                GROUP BY category_name, group_name
                ORDER BY last_update DESC, products DESC
                LIMIT 5
                """
            },
            {
                'name': 'Largest product groups',
                'query': f"""
                SELECT 
                    category_name,
                    group_name,
                    COUNT(*) as product_count
                FROM `{self.table_ref}`
                GROUP BY category_name, group_name
                ORDER BY product_count DESC
                LIMIT 5
                """
            }
        ]
        
        for query_info in queries:
            print(f"\n📈 {query_info['name']}:")
            try:
                results = self.client.query(query_info['query']).result()
                for i, row in enumerate(results):
                    row_data = [str(value) for value in row.values()]
                    print(f"   {i+1}. {' | '.join(row_data)}")
            except Exception as e:
                print(f"   Error: {e}")

if __name__ == "__main__":
    # Simple test
    loader = BigQueryMetadataLoader()
    loader.print_table_summary()
    loader.run_sample_queries()