#!/usr/bin/env python3
"""
Test script for rate limiting enhancements
"""
import time
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'product_downloader'))
from resumable_downloader import ResumableDownloader
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_rate_limiting_config():
    """Test that rate limiting configuration is properly set"""
    logger.info("=== Testing Rate Limiting Configuration ===")
    
    downloader = ResumableDownloader(
        max_workers=2, 
        request_delay=3.0,
        rate_limit_delay=60  # 1 minute for testing
    )
    
    assert downloader.max_workers == 2
    assert downloader.request_delay == 3.0
    assert downloader.rate_limit_delay == 60
    assert downloader.consecutive_403s == 0
    assert downloader.rate_limited_until == 0
    
    logger.info("✓ Rate limiting configuration test passed")
    return True

def test_failed_groups_management():
    """Test failed groups tracking functionality"""
    logger.info("=== Testing Failed Groups Management ===")
    
    downloader = ResumableDownloader(max_workers=1, request_delay=1.0)
    
    # Test marking groups as failed
    test_group_id = "test_123"
    assert not downloader.is_group_failed(test_group_id)
    
    downloader.mark_group_failed(test_group_id)
    assert downloader.is_group_failed(test_group_id)
    assert test_group_id in downloader.progress['failed_groups']
    
    # Test removing from failed groups
    downloader.remove_from_failed_groups(test_group_id)
    assert not downloader.is_group_failed(test_group_id)
    assert test_group_id not in downloader.progress['failed_groups']
    
    logger.info("✓ Failed groups management test passed")
    return True

def test_rate_limiting_methods():
    """Test rate limiting helper methods"""
    logger.info("=== Testing Rate Limiting Methods ===")
    
    downloader = ResumableDownloader(max_workers=1, request_delay=0.5)
    
    # Test request timing
    start_time = time.time()
    downloader._check_rate_limiting()
    first_request_time = time.time() - start_time
    
    # Second request should be delayed
    start_time = time.time()
    downloader._check_rate_limiting()
    second_request_time = time.time() - start_time
    
    assert second_request_time >= 0.4  # Should wait at least close to request_delay
    
    # Test rate limiting state
    original_delay = downloader.request_delay
    downloader.consecutive_403s = 1
    downloader._handle_rate_limiting()
    
    assert downloader.rate_limited_until > time.time()
    assert downloader.request_delay > original_delay
    
    # Test reset
    downloader._reset_rate_limiting()
    assert downloader.consecutive_403s == 0
    
    logger.info("✓ Rate limiting methods test passed")
    return True

def main():
    """Run all tests"""
    logger.info("Starting Rate Limiting Tests...\n")
    
    tests = [
        test_rate_limiting_config,
        test_failed_groups_management, 
        test_rate_limiting_methods
    ]
    
    passed = 0
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            logger.error(f"Test {test.__name__} failed: {e}")
    
    logger.info(f"\n=== Test Results ===")
    logger.info(f"Passed: {passed}/{len(tests)} tests")
    
    if passed == len(tests):
        logger.info("🎉 All rate limiting tests passed!")
        return True
    else:
        logger.error("❌ Some tests failed")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)