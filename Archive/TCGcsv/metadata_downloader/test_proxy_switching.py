#!/usr/bin/env python3
"""
Test proxy automatic switching functionality
"""

from proxy_manager import MihomoProxyManager
import requests
import time
import logging

def main():
    """Test proxy switching functionality"""
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger('proxy_switch_test')
    
    logger.info("=== Testing Proxy Automatic Switching ===")
    
    try:
        # Initialize proxy manager
        manager = MihomoProxyManager(
            api_url="http://127.0.0.1:9090",
            secret="",
            rate_limit_codes=[403, 429, 503, 502, 504]
        )
        
        logger.info("✅ Proxy Manager initialized")
        
        # Test 1: Manual proxy switching
        logger.info("\n=== Test 1: Manual Proxy Switching ===")
        current_proxy = manager.get_current_proxy()
        logger.info(f"Current proxy: {current_proxy}")
        
        # Get best proxy (should be different from current if we have multiple)
        best_proxy = manager.get_best_proxy()
        logger.info(f"Best proxy: {best_proxy}")
        
        # Try switching to a different proxy
        available_proxies = manager.available_proxies[:5]  # Test first 5 proxies
        logger.info(f"Testing proxy switching with: {available_proxies}")
        
        for proxy_name in available_proxies:
            if proxy_name != current_proxy:
                logger.info(f"Switching to: {proxy_name}")
                success = manager.switch_proxy(proxy_name)
                if success:
                    time.sleep(1)  # Brief pause
                    new_current = manager.get_current_proxy()
                    logger.info(f"Switch result: {new_current}")
                    break
        
        # Test 2: Health check all proxies
        logger.info("\n=== Test 2: Proxy Health Check ===")
        health_results = manager.health_check_all_proxies()
        
        healthy_count = sum(1 for status in health_results.values() if status)
        total_count = len(health_results)
        logger.info(f"Health check results: {healthy_count}/{total_count} proxies healthy")
        
        # Show some results
        for i, (proxy, status) in enumerate(list(health_results.items())[:10]):
            status_icon = "✅" if status else "❌"
            logger.info(f"  {proxy}: {status_icon}")
        
        # Test 3: Request with automatic switching
        logger.info("\n=== Test 3: Request with Auto-Switching ===")
        test_urls = [
            "https://tcgcsv.com/tcgplayer/categories",
            "https://httpbin.org/ip",
            "https://httpbin.org/user-agent"
        ]
        
        for url in test_urls:
            try:
                logger.info(f"Testing URL: {url}")
                response = manager.make_request_with_auto_switch(url, max_switches=2)
                logger.info(f"✅ Success: HTTP {response.status_code}")
                
                # Show response size
                content_length = len(response.text)
                logger.info(f"Response size: {content_length} bytes")
                
            except Exception as e:
                logger.error(f"❌ Failed: {e}")
        
        # Test 4: Force error handling (simulate rate limit)
        logger.info("\n=== Test 4: Error Handling ===")
        
        # Get current proxy for testing
        test_proxy = manager.get_current_proxy()
        if test_proxy:
            logger.info(f"Testing error handling with proxy: {test_proxy}")
            
            # Simulate rate limit error
            action = manager.handle_request_error(403, test_proxy)
            logger.info(f"Action for HTTP 403: {action}")
            
            # Simulate server error
            action = manager.handle_request_error(503, test_proxy)
            logger.info(f"Action for HTTP 503: {action}")
            
            # Simulate client error
            action = manager.handle_request_error(400, test_proxy)
            logger.info(f"Action for HTTP 400: {action}")
        
        # Test 5: Final statistics
        logger.info("\n=== Test 5: Final Statistics ===")
        stats = manager.get_proxy_statistics()
        
        logger.info(f"Total proxies: {stats['total_proxies']}")
        logger.info(f"Healthy proxies: {stats['healthy_proxies']}")
        logger.info(f"Total requests: {stats['summary']['total_requests']}")
        logger.info(f"Success rate: {stats['summary']['total_successes']}/{stats['summary']['total_requests']}")
        logger.info(f"Rate limits encountered: {stats['summary']['total_rate_limits']}")
        
        # Show top 5 performing proxies
        logger.info("\nTop performing proxies:")
        proxy_scores = []
        for name, details in stats['proxy_details'].items():
            if details['total_requests'] > 0:
                proxy_scores.append((name, details['success_rate'], details['total_requests']))
        
        proxy_scores.sort(key=lambda x: x[1], reverse=True)
        for i, (name, success_rate, total_requests) in enumerate(proxy_scores[:5]):
            logger.info(f"  {i+1}. {name}: {success_rate:.1f}% ({total_requests} requests)")
        
        logger.info("\n✅ All proxy switching tests completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        raise

if __name__ == "__main__":
    main()