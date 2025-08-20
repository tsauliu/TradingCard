#!/usr/bin/env python3
"""
Main entry point for TCG Metadata Downloader
Provides command-line interface for downloading TCG metadata
"""
import sys
import os
import argparse
from datetime import datetime
from dotenv import load_dotenv

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from api_downloader import TCGAPIDownloader
from bigquery_loader import BigQueryMetadataLoader

def test_download():
    """Run a test download with limited data"""
    print("🧪 TEST MODE: Downloading Pokemon category (first 3 groups)")
    print("="*60)
    
    downloader = TCGAPIDownloader(
        min_request_interval=1.2,
        batch_size=100
    )
    
    try:
        # Download test data
        total_products = downloader.download_all(
            limit_categories=1,
            limit_groups_per_category=3
        )
        
        print(f"\n✅ Test download completed")
        print(f"📦 Products downloaded: {total_products}")
        
        if total_products > 0:
            # Verify in BigQuery
            print(f"\n🔍 Verifying data in BigQuery...")
            loader = BigQueryMetadataLoader()
            verification = loader.verify_data(limit=5)
            
            if verification['success']:
                print(f"✅ Data verified in BigQuery: {verification['total_rows']} rows")
                return True
            else:
                print(f"❌ BigQuery verification failed: {verification.get('error', 'Unknown error')}")
                return False
        else:
            print("❌ No products downloaded")
            return False
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

def full_download():
    """Run a full download of all TCG data"""
    print("🚀 FULL DOWNLOAD MODE: All categories and groups")
    print("="*60)
    print("⚠️  This will take approximately 1-2 hours")
    print("⚠️  Rate limited to <1 request per second")
    
    # Confirm with user
    response = input("\nProceed with full download? (y/N): ")
    if response.lower() != 'y':
        print("Download cancelled")
        return False
    
    downloader = TCGAPIDownloader(
        min_request_interval=1.2,
        batch_size=1000  # Larger batches for efficiency
    )
    
    try:
        print(f"\n🎬 Starting full download at {datetime.now()}")
        total_products = downloader.download_all()
        
        print(f"\n🎉 Full download completed!")
        print(f"📦 Total products: {total_products:,}")
        
        # Show final BigQuery summary
        print(f"\n📊 BigQuery Summary:")
        loader = BigQueryMetadataLoader()
        loader.print_table_summary()
        
        return True
        
    except KeyboardInterrupt:
        print(f"\n⚠️  Download interrupted by user")
        print(f"📊 Partial data may be available in BigQuery")
        return False
    except Exception as e:
        print(f"❌ Full download failed: {e}")
        return False

def category_download(category_ids: list, limit_groups: int = None):
    """Download specific categories"""
    print(f"🎯 CATEGORY DOWNLOAD: {', '.join(map(str, category_ids))}")
    print("="*60)
    
    downloader = TCGAPIDownloader(
        min_request_interval=1.2,
        batch_size=500
    )
    
    try:
        # Get all categories first
        all_categories = downloader.download_categories()
        
        # Filter to requested categories
        target_categories = [cat for cat in all_categories if cat['categoryId'] in category_ids]
        
        if not target_categories:
            print(f"❌ No categories found with IDs: {category_ids}")
            return False
        
        print(f"📁 Found categories:")
        for cat in target_categories:
            print(f"   {cat['categoryId']}: {cat['name']}")
        
        total_products = 0
        
        for category in target_categories:
            category_id = str(category['categoryId'])
            print(f"\n📂 Processing category: {category['name']}")
            
            # Get groups for this category
            groups = downloader.download_groups(category_id)
            
            if limit_groups:
                groups = groups[:limit_groups]
                print(f"   Limited to first {limit_groups} groups")
            
            print(f"   Found {len(groups)} groups")
            
            # Process each group
            for group in groups:
                products_count = downloader.download_and_save_group(category, group)
                total_products += products_count
        
        # Flush final batch
        downloader.stream_to_bigquery([], force=True)
        
        print(f"\n✅ Category download completed")
        print(f"📦 Total products: {total_products:,}")
        
        return True
        
    except Exception as e:
        print(f"❌ Category download failed: {e}")
        return False

def show_status():
    """Show current BigQuery table status"""
    print("📊 BIGQUERY STATUS")
    print("="*40)
    
    try:
        loader = BigQueryMetadataLoader()
        loader.print_table_summary()
        return True
    except Exception as e:
        print(f"❌ Error accessing BigQuery: {e}")
        return False

def main():
    """Main command-line interface"""
    parser = argparse.ArgumentParser(
        description="TCG API Downloader - Download trading card data from tcgcsv.com API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 main.py test                    # Test download (Pokemon, 3 groups)
  python3 main.py full                    # Full download (all data)
  python3 main.py category 3              # Download Pokemon category
  python3 main.py category 3 5 --limit 10 # Download categories 3,5 (first 10 groups each)
  python3 main.py status                  # Show BigQuery table status
        """
    )
    
    parser.add_argument('mode', choices=['test', 'full', 'category', 'status'],
                       help='Download mode')
    parser.add_argument('category_ids', nargs='*', type=int,
                       help='Category IDs for category mode')
    parser.add_argument('--limit', type=int,
                       help='Limit groups per category')
    parser.add_argument('--project', type=str,
                       help='Google Cloud project ID')
    parser.add_argument('--dataset', type=str, default='tcg_data',
                       help='BigQuery dataset name')
    
    args = parser.parse_args()
    
    # Load environment variables
    load_dotenv()
    
    print("🃏 TCG METADATA DOWNLOADER")
    print("="*50)
    print(f"Mode: {args.mode}")
    print(f"Target: {args.project or 'default'}.{args.dataset}.tcg_metadata")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*50)
    
    # Set global project if specified
    if args.project:
        os.environ['GOOGLE_CLOUD_PROJECT'] = args.project
    
    # Route to appropriate function
    success = False
    
    if args.mode == 'test':
        success = test_download()
        
    elif args.mode == 'full':
        success = full_download()
        
    elif args.mode == 'category':
        if not args.category_ids:
            print("❌ Category mode requires category IDs")
            print("Example: python3 main.py category 3 5")
            return 1
        success = category_download(args.category_ids, args.limit)
        
    elif args.mode == 'status':
        success = show_status()
    
    # Exit with appropriate code
    if success:
        print(f"\n✅ {args.mode.title()} completed successfully")
        return 0
    else:
        print(f"\n❌ {args.mode.title()} failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())