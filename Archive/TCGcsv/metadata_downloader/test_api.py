#!/usr/bin/env python3
"""
Test script for TCG API Downloader
Downloads Pokemon data and verifies it appears in BigQuery
"""
import os
import sys
import time
from datetime import datetime
from dotenv import load_dotenv

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from api_downloader import TCGAPIDownloader
from bigquery_loader import BigQueryAPILoader

def test_pokemon_download():
    """Test download with Pokemon category and verify BigQuery data"""
    
    print("🧪 TCG API DOWNLOADER TEST")
    print("="*50)
    print(f"Test started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Testing with Pokemon category (ID: 3)")
    print("Downloading first 3 groups only...")
    print("="*50)
    
    # Initialize downloader
    downloader = TCGAPIDownloader(
        min_request_interval=1.2,  # 0.83 req/s (safely below 1 req/s)
        batch_size=100  # Smaller batches for testing
    )
    
    try:
        # Test specific Pokemon category with limited groups
        print("\n🔍 Starting Pokemon download test...")
        start_time = time.time()
        
        # Download with limits for testing
        total_products = downloader.download_all(
            limit_categories=1,  # Only first category (should be Pokemon if ordered correctly)
            limit_groups_per_category=3  # Only first 3 groups
        )
        
        elapsed = time.time() - start_time
        
        print(f"\n⏱️  Test completed in {elapsed:.1f} seconds")
        print(f"📦 Products downloaded: {total_products}")
        
        if total_products == 0:
            print("❌ No products downloaded - test failed")
            return False
        
        # Verify BigQuery data
        print(f"\n🔍 Verifying data in BigQuery...")
        return verify_bigquery_data()
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        return False

def test_specific_pokemon_groups():
    """Test with specific Pokemon groups we know exist"""
    
    print("\n🎯 SPECIFIC POKEMON GROUPS TEST")
    print("="*50)
    
    # Initialize downloader
    downloader = TCGAPIDownloader(
        min_request_interval=1.2,
        batch_size=50
    )
    
    try:
        # Get categories and find Pokemon (ID: 3)
        print("🔍 Finding Pokemon category...")
        categories = downloader.download_categories()
        
        pokemon_category = None
        for category in categories:
            if category['categoryId'] == 3:
                pokemon_category = category
                break
        
        if not pokemon_category:
            print("❌ Pokemon category (ID: 3) not found")
            return False
        
        print(f"✅ Found Pokemon category: {pokemon_category['name']}")
        
        # Get groups for Pokemon
        print("🔍 Getting Pokemon groups...")
        groups = downloader.download_groups("3")
        
        if not groups:
            print("❌ No Pokemon groups found")
            return False
        
        print(f"✅ Found {len(groups)} Pokemon groups")
        
        # Download first 3 groups
        test_groups = groups[:3]
        total_products = 0
        
        for i, group in enumerate(test_groups, 1):
            print(f"\n📦 [{i}/3] Processing group: {group.get('name', f'Group {group['groupId']}')}")
            products_count = downloader.download_and_save_group(pokemon_category, group)
            total_products += products_count
            print(f"    ✅ Downloaded {products_count} products")
        
        # Flush final batch
        print(f"\n💾 Flushing final batch...")
        downloader.stream_to_bigquery([], force=True)
        
        print(f"\n✅ Test completed successfully!")
        print(f"📊 Total products: {total_products}")
        
        if total_products > 0:
            return verify_bigquery_data()
        else:
            print("❌ No products downloaded")
            return False
        
    except Exception as e:
        print(f"\n❌ Specific test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def verify_bigquery_data():
    """Verify data was successfully loaded to BigQuery"""
    
    print("\n" + "="*60)
    print("🔍 VERIFYING DATA IN BIGQUERY")
    print("="*60)
    
    try:
        # Initialize BigQuery loader
        loader = BigQueryAPILoader()
        
        # Verify data exists
        verification = loader.verify_data(limit=10)
        
        if not verification['success']:
            print(f"❌ Verification failed: {verification.get('error', 'Unknown error')}")
            
            # Print table info if available
            if 'table_info' in verification:
                table_info = verification['table_info']
                if table_info['exists']:
                    print(f"📊 Table exists but has {table_info['num_rows']} rows")
                else:
                    print("📊 Table does not exist")
            
            return False
        
        # Success - print results
        total_rows = verification['total_rows']
        sample_rows = verification['sample_rows']
        table_info = verification['table_info']
        
        print(f"✅ SUCCESS: Found {total_rows:,} rows in BigQuery")
        print(f"📊 Table size: {table_info['num_bytes'] / (1024*1024):.2f} MB")
        print(f"📅 Last modified: {table_info['modified']}")
        
        # Show sample data
        if sample_rows:
            print(f"\n📋 Sample data (first {len(sample_rows)} rows):")
            print("-" * 60)
            
            for i, row in enumerate(sample_rows, 1):
                print(f"{i}. Category: {row['category_name']} (ID: {row['category_id']})")
                print(f"   Group: {row['group_name']} (ID: {row['group_id']})")
                print(f"   Product: {row['product_name']} (ID: {row['product_id']})")
                print(f"   Updated: {row['update_date']}")
                print("-" * 40)
        
        # Print comprehensive summary
        print(f"\n📊 Full table summary:")
        loader.print_table_summary()
        
        # Run sample queries
        loader.run_sample_queries()
        
        return True
        
    except Exception as e:
        print(f"❌ BigQuery verification error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_api_endpoints():
    """Test individual API endpoints before full download"""
    
    print("\n🔌 API ENDPOINTS TEST")
    print("="*40)
    
    downloader = TCGAPIDownloader(min_request_interval=1.2)
    
    try:
        # Test categories endpoint
        print("1. Testing categories endpoint...")
        categories = downloader.download_categories()
        print(f"   ✅ Found {len(categories)} categories")
        
        # Find Pokemon category
        pokemon_cat = None
        for cat in categories:
            if cat['categoryId'] == 3:
                pokemon_cat = cat
                break
        
        if not pokemon_cat:
            print("   ❌ Pokemon category not found")
            return False
        
        print(f"   ✅ Pokemon category: {pokemon_cat['name']}")
        
        # Test groups endpoint
        print("2. Testing groups endpoint...")
        groups = downloader.download_groups("3")
        print(f"   ✅ Found {len(groups)} Pokemon groups")
        
        if not groups:
            return False
        
        # Test products endpoint
        print("3. Testing products endpoint...")
        first_group = groups[0]
        products = downloader.download_products("3", str(first_group['groupId']))
        print(f"   ✅ Found {len(products)} products in group '{first_group.get('name', 'Unknown')}'")
        
        if products:
            print(f"   📦 Sample product: {products[0].get('name', 'Unknown')}")
        
        print("\n✅ All API endpoints working correctly!")
        return True
        
    except Exception as e:
        print(f"❌ API test failed: {e}")
        return False

def main():
    """Run the complete test suite"""
    
    # Load environment variables
    load_dotenv()
    
    print("🚀 STARTING TCG API TESTS")
    print("="*50)
    print(f"Working directory: {os.getcwd()}")
    
    # Check for BigQuery credentials
    if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
        print(f"✅ BigQuery credentials: {os.environ['GOOGLE_APPLICATION_CREDENTIALS']}")
    else:
        print("⚠️  No GOOGLE_APPLICATION_CREDENTIALS environment variable")
        print("   Using default credentials...")
    
    # Run tests in sequence
    tests = [
        ("API Endpoints", test_api_endpoints),
        ("Specific Pokemon Groups", test_specific_pokemon_groups),
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        print(f"\n{'='*60}")
        print(f"🧪 Running: {test_name}")
        print(f"{'='*60}")
        
        try:
            success = test_func()
            results[test_name] = success
            
            if success:
                print(f"✅ {test_name}: PASSED")
            else:
                print(f"❌ {test_name}: FAILED")
                
        except Exception as e:
            print(f"❌ {test_name}: ERROR - {e}")
            results[test_name] = False
    
    # Final results
    print(f"\n{'='*60}")
    print("🏁 FINAL TEST RESULTS")
    print(f"{'='*60}")
    
    passed = sum(1 for success in results.values() if success)
    total = len(results)
    
    for test_name, success in results.items():
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"{test_name}: {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Data should be visible in BigQuery.")
    else:
        print("⚠️  Some tests failed. Check the logs above.")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)