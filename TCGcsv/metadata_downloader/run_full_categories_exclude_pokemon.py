#!/usr/bin/env python3
"""
Full categories download excluding Pokemon categories
Run all 87 non-Pokemon categories with optimized performance and comprehensive logging
"""
import os
import sys
import time
from datetime import datetime
from dotenv import load_dotenv

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from enhanced_api_downloader import EnhancedTCGMetadataDownloader

def main():
    print("🃏 FULL TCG METADATA DOWNLOAD (EXCLUDING POKEMON)")
    print("=" * 70)
    
    # Load environment variables
    load_dotenv()
    
    # Set credentials
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/caoliu/TradingCard/TCGcsv/service-account.json'
    
    # Categories to exclude: Pokemon (3) and Pokemon Japan (85)
    excluded_categories = [3, 85]
    
    # Get all categories and filter out Pokemon ones
    try:
        import requests
        response = requests.get('https://tcgcsv.com/tcgplayer/categories')
        all_categories = response.json()['results']
        
        # Filter categories to download (exclude Pokemon categories)
        categories_to_download = [
            cat['categoryId'] for cat in all_categories 
            if cat['categoryId'] not in excluded_categories
        ]
        
        print(f"📋 Download Plan:")
        print(f"   Total categories available: {len(all_categories)}")
        print(f"   Excluded (Pokemon): {excluded_categories}")
        print(f"   Categories to download: {len(categories_to_download)}")
        print(f"   Category IDs: {categories_to_download}")
        print()
        
        # Initialize enhanced downloader with optimized settings
        print("🚀 Initializing Enhanced TCG Metadata Downloader...")
        downloader = EnhancedTCGMetadataDownloader(
            min_request_interval=0.5,  # Slower rate to avoid rate limiting
            batch_size=1000,           # Larger batches for efficiency
            checkpoint_file="full_categories_checkpoint.json",
            log_file="full_categories_download.log",
            use_proxy_manager=False    # DISABLE proxy manager to avoid switching issues
        )
        
        print("✅ Downloader initialized")
        print(f"⚡ Rate limiting: Pure BigQuery buffering (no artificial delays)")
        print(f"📦 Batch size: 1000 rows")
        print(f"💾 Checkpoint: full_categories_checkpoint.json")
        print(f"📄 Log file: full_categories_download.log")
        print()
        
        # Start the download
        start_time = datetime.now()
        print(f"🎬 Starting full categories download at {start_time}")
        print(f"🎯 Target: {len(categories_to_download)} categories")
        print("=" * 70)
        
        # Run the download
        stats = downloader.download_all_categories(
            specific_categories=categories_to_download,
            skip_completed=True  # Resume capability
        )
        
        # Final summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        print("=" * 70)
        print("🎉 FULL CATEGORIES DOWNLOAD COMPLETE!")
        print("=" * 70)
        print(f"⏱️  Total duration: {duration}")
        print(f"📊 Final stats:")
        print(f"   Categories processed: {stats['categories_processed']}")
        print(f"   Categories skipped: {stats['categories_skipped']}")
        print(f"   Categories failed: {stats['categories_failed']}")
        print(f"   Total products: {stats['total_products']:,}")
        print(f"   Total groups: {stats['total_groups']:,}")
        duration_hours = duration.total_seconds()/3600
        products_per_hour = stats['total_products']/duration_hours if duration_hours > 0 else 0
        print(f"   Products/hour: {products_per_hour:.0f}")
        print("=" * 70)
        
        return True
        
    except KeyboardInterrupt:
        print("\n⚠️  Download interrupted by user")
        print("💾 Progress saved in checkpoint file for resume")
        return False
    except Exception as e:
        print(f"❌ Download failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)