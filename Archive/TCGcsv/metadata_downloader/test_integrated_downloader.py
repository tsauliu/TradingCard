#!/usr/bin/env python3
"""
Test the integrated TCG downloader with proxy manager
"""

from enhanced_api_downloader import EnhancedTCGMetadataDownloader
import logging
import time

def main():
    """Test the integrated downloader"""
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger('test_integrated')
    
    logger.info("=== Testing Integrated TCG Downloader with Proxy Manager ===")
    
    try:
        # Initialize downloader with proxy manager
        downloader = EnhancedTCGMetadataDownloader(
            min_request_interval=0.5,
            batch_size=10,
            use_proxy_manager=True,
            mihomo_api_url="http://127.0.0.1:9090",
            mihomo_secret=""
        )
        
        logger.info("✅ Downloader initialized successfully")
        
        # Show resume status with proxy info
        downloader.print_resume_status()
        
        # Test categories download
        logger.info("\n=== Testing Categories Download ===")
        categories = downloader.download_categories()
        logger.info(f"✅ Successfully downloaded {len(categories)} categories")
        
        # Test a single group download to verify proxy switching works
        logger.info("\n=== Testing Single Group Download ===")
        test_category = categories[0]  # Use first category
        groups = downloader.download_groups(str(test_category['categoryId']))
        logger.info(f"✅ Found {len(groups)} groups in category: {test_category['name']}")
        
        if groups:
            # Download products from first group
            test_group = groups[0]
            products_count = downloader.download_and_save_group(test_category, test_group)
            logger.info(f"✅ Downloaded {products_count} products from test group")
        
        # Show final proxy statistics
        proxy_stats = downloader.get_proxy_statistics()
        if 'proxy_manager' not in proxy_stats:
            logger.info("\n=== Final Proxy Statistics ===")
            logger.info(f"Total requests: {proxy_stats['summary']['total_requests']}")
            logger.info(f"Successful requests: {proxy_stats['summary']['total_successes']}")
            logger.info(f"Failed requests: {proxy_stats['summary']['total_failures']}")
            logger.info(f"Rate limited requests: {proxy_stats['summary']['total_rate_limits']}")
            logger.info(f"Current proxy: {proxy_stats['current_proxy']}")
        
        logger.info("\n✅ All tests completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        raise

if __name__ == "__main__":
    main()