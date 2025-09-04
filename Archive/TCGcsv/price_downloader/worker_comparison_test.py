#!/usr/bin/env python3
import time
import sys
try:
    from .price_downloader import TCGPriceDownloader
except ImportError:
    from price_downloader import TCGPriceDownloader

def compare_worker_counts(target_date: str = "2024-12-01", worker_counts: list = [5, 50]):
    """Compare performance with different worker counts"""
    
    print("=" * 70)
    print(f"🚀 WORKER COUNT COMPARISON TEST")
    print(f"📅 Target Date: {target_date}")
    print(f"🔧 Testing worker counts: {worker_counts}")
    print("=" * 70)
    
    results = {}
    
    for worker_count in worker_counts:
        print(f"\n⚡ Testing with {worker_count} workers...")
        print("-" * 50)
        
        start_time = time.time()
        
        try:
            downloader = TCGPriceDownloader(max_workers=worker_count)
            download_stats = downloader.download_full_date_data(target_date)
            
            processing_time = download_stats['processing_time_seconds']
            total_records = download_stats['total_records']
            records_per_second = download_stats['records_per_second']
            
            results[worker_count] = {
                'processing_time': processing_time,
                'total_records': total_records,
                'records_per_second': records_per_second,
                'success': True
            }
            
            print(f"✅ Completed in {processing_time:.2f}s")
            print(f"📦 Records: {total_records:,}")
            print(f"🚀 Rate: {records_per_second:.0f} records/second")
            
        except Exception as e:
            print(f"❌ Failed with {worker_count} workers: {e}")
            results[worker_count] = {
                'success': False,
                'error': str(e)
            }
    
    # Compare results
    print("\n" + "=" * 70)
    print("📊 COMPARISON RESULTS")
    print("=" * 70)
    
    successful_tests = {k: v for k, v in results.items() if v.get('success', False)}
    
    if len(successful_tests) >= 2:
        worker_counts_sorted = sorted(successful_tests.keys())
        low_workers = worker_counts_sorted[0]
        high_workers = worker_counts_sorted[-1]
        
        low_time = successful_tests[low_workers]['processing_time']
        high_time = successful_tests[high_workers]['processing_time']
        
        low_rate = successful_tests[low_workers]['records_per_second']
        high_rate = successful_tests[high_workers]['records_per_second']
        
        print(f"🐌 {low_workers} workers:")
        print(f"   Time: {low_time:.2f}s")
        print(f"   Rate: {low_rate:.0f} records/second")
        
        print(f"\n🚀 {high_workers} workers:")
        print(f"   Time: {high_time:.2f}s") 
        print(f"   Rate: {high_rate:.0f} records/second")
        
        if low_time > 0 and high_time > 0:
            time_improvement = ((low_time - high_time) / low_time) * 100
            rate_improvement = ((high_rate - low_rate) / low_rate) * 100
            
            print(f"\n📈 PERFORMANCE DIFFERENCE:")
            print(f"   Time improvement: {time_improvement:+.1f}%")
            print(f"   Rate improvement: {rate_improvement:+.1f}%")
            
            if time_improvement > 5:
                print(f"   ✅ {high_workers} workers is significantly faster")
            elif time_improvement < -5:
                print(f"   ⚠️  {low_workers} workers is actually faster")
            else:
                print(f"   ≈ Similar performance, no significant difference")
    
    print("\n" + "=" * 70)
    
    return results

if __name__ == "__main__":
    # Use command line arguments
    test_date = sys.argv[1] if len(sys.argv) > 1 else "2024-12-01"
    
    # Default comparison: 5 vs 50 workers
    worker_counts = [5, 50]
    
    # Allow custom worker counts from command line
    if len(sys.argv) > 2:
        worker_counts = [int(x) for x in sys.argv[2:]]
    
    print(f"Starting worker comparison test for {test_date}...")
    results = compare_worker_counts(test_date, worker_counts)
    
    print("🎉 Worker comparison test completed!")