#!/usr/bin/env python3
"""
Mihomo Proxy Manager with Automatic Switching
Manages proxy switching based on rate limits and failures
"""

import requests
import json
import time
import random
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import logging
from dataclasses import dataclass, field
from collections import defaultdict
import threading


@dataclass
class ProxyStats:
    """Proxy performance statistics"""
    name: str
    success_count: int = 0
    failure_count: int = 0
    last_success: Optional[datetime] = None
    last_failure: Optional[datetime] = None
    avg_response_time: float = 0.0
    consecutive_failures: int = 0
    is_healthy: bool = True
    total_requests: int = 0
    rate_limited_count: int = 0
    last_rate_limit: Optional[datetime] = None
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage"""
        if self.total_requests == 0:
            return 100.0
        return (self.success_count / self.total_requests) * 100
    
    def record_success(self, response_time: float):
        """Record successful request"""
        self.success_count += 1
        self.total_requests += 1
        self.last_success = datetime.now()
        self.consecutive_failures = 0
        self.is_healthy = True
        
        # Update average response time
        if self.avg_response_time == 0:
            self.avg_response_time = response_time
        else:
            self.avg_response_time = (self.avg_response_time + response_time) / 2
    
    def record_failure(self, is_rate_limit: bool = False):
        """Record failed request"""
        self.failure_count += 1
        self.total_requests += 1
        self.last_failure = datetime.now()
        self.consecutive_failures += 1
        
        if is_rate_limit:
            self.rate_limited_count += 1
            self.last_rate_limit = datetime.now()
        
        # Mark as unhealthy after multiple consecutive failures
        if self.consecutive_failures >= 3:
            self.is_healthy = False


class MihomoProxyManager:
    def __init__(self,
                 api_url: str = "http://127.0.0.1:9090",
                 secret: str = "",
                 rate_limit_codes: List[int] = None,
                 max_retries_per_proxy: int = 3,
                 proxy_cooldown: int = 300,  # 5 minutes
                 health_check_interval: int = 600):  # 10 minutes
        """
        Initialize Mihomo Proxy Manager
        
        Args:
            api_url: Mihomo API base URL
            secret: API secret for authentication
            rate_limit_codes: HTTP status codes that indicate rate limiting
            max_retries_per_proxy: Max retries before switching proxy
            proxy_cooldown: Seconds to wait before retrying failed proxy
            health_check_interval: Seconds between proxy health checks
        """
        self.api_url = api_url.rstrip('/')
        self.secret = secret
        self.rate_limit_codes = rate_limit_codes or [403, 429, 503, 502, 504]
        self.max_retries_per_proxy = max_retries_per_proxy
        self.proxy_cooldown = proxy_cooldown
        self.health_check_interval = health_check_interval
        
        # Proxy statistics and management
        self.proxy_stats: Dict[str, ProxyStats] = {}
        self.available_proxies: List[str] = []
        self.current_proxy: Optional[str] = None
        self.proxy_groups: Dict[str, List[str]] = {}
        
        # Request session with retries
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            max_retries=requests.packages.urllib3.util.retry.Retry(
                total=3,
                backoff_factor=0.3,
                status_forcelist=[500, 502, 503, 504]
            )
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        
        # Authentication headers - mihomo uses different auth methods
        if self.secret:
            # Try multiple authentication methods
            self.session.headers.update({'Authorization': f'Bearer {self.secret}'})
            # Also add as query parameter for compatibility
            self.session.params = {'secret': self.secret}
        
        # Threading lock for thread-safe operations
        self._lock = threading.Lock()
        
        # Setup logging
        self.logger = logging.getLogger('proxy_manager')
        self.logger.setLevel(logging.INFO)
        
        if not self.logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
        
        # Initialize proxy information
        self._initialize_proxies()
        
        self.logger.info(f"Proxy Manager initialized with {len(self.available_proxies)} proxies")
    
    def _initialize_proxies(self):
        """Initialize proxy information from Mihomo API"""
        try:
            proxies_info = self._api_request('GET', '/proxies')
            
            if 'proxies' in proxies_info:
                for proxy_name, proxy_info in proxies_info['proxies'].items():
                    if proxy_name not in ['DIRECT', 'REJECT', 'GLOBAL']:
                        # Initialize proxy stats
                        self.proxy_stats[proxy_name] = ProxyStats(name=proxy_name)
                        
                        # Add to appropriate groups
                        proxy_type = proxy_info.get('type', 'unknown')
                        if proxy_type in ['Shadowsocks', 'ShadowsocksR', 'Vmess', 'Trojan']:
                            self.available_proxies.append(proxy_name)
                        
                        # Track proxy groups
                        if proxy_type == 'Selector':
                            group_proxies = proxy_info.get('all', [])
                            self.proxy_groups[proxy_name] = group_proxies
            
            self.logger.info(f"Initialized {len(self.available_proxies)} individual proxies")
            self.logger.info(f"Found {len(self.proxy_groups)} proxy groups")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize proxies: {e}")
            # Fallback: assume basic proxy setup
            self.available_proxies = ['auto-switch', 'manual-select']
    
    def _api_request(self, method: str, endpoint: str, data: Any = None) -> Dict[str, Any]:
        """
        Make request to Mihomo API
        
        Args:
            method: HTTP method
            endpoint: API endpoint
            data: Request data
            
        Returns:
            Response JSON data
        """
        url = f"{self.api_url}{endpoint}"
        
        try:
            if method.upper() == 'GET':
                response = self.session.get(url, timeout=10)
            elif method.upper() == 'PUT':
                response = self.session.put(url, json=data, timeout=10)
            elif method.upper() == 'POST':
                response = self.session.post(url, json=data, timeout=10)
            else:
                raise ValueError(f"Unsupported method: {method}")
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed [{method} {endpoint}]: {e}")
            raise
    
    def get_current_proxy(self) -> Optional[str]:
        """Get currently selected proxy from mihomo"""
        try:
            # Check manual-select group first
            if 'manual-select' in self.proxy_groups:
                selector_info = self._api_request('GET', '/proxies/manual-select')
                return selector_info.get('now')
            
            # Fallback to auto-switch group
            if 'auto-switch' in self.proxy_groups:
                selector_info = self._api_request('GET', '/proxies/auto-switch')
                return selector_info.get('now')
            
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to get current proxy: {e}")
            return None
    
    def switch_proxy(self, target_proxy: str, group: str = "manual-select") -> bool:
        """
        Switch to a specific proxy
        
        Args:
            target_proxy: Name of proxy to switch to
            group: Proxy group to switch in
            
        Returns:
            True if switch successful
        """
        try:
            with self._lock:
                self.logger.info(f"Switching to proxy: {target_proxy} in group: {group}")
                
                # Make API call to switch proxy
                self._api_request('PUT', f'/proxies/{group}', {'name': target_proxy})
                
                # Update current proxy
                self.current_proxy = target_proxy
                
                # Verify the switch
                time.sleep(1)  # Brief delay for switch to take effect
                current = self.get_current_proxy()
                
                if current == target_proxy:
                    self.logger.info(f"✅ Successfully switched to: {target_proxy}")
                    return True
                else:
                    self.logger.warning(f"⚠️ Switch verification failed. Expected: {target_proxy}, Got: {current}")
                    return False
            
        except Exception as e:
            self.logger.error(f"Failed to switch proxy to {target_proxy}: {e}")
            return False
    
    def get_best_proxy(self, exclude: List[str] = None) -> Optional[str]:
        """
        Get the best available proxy based on performance stats
        
        Args:
            exclude: List of proxy names to exclude
            
        Returns:
            Name of best proxy, or None if no suitable proxy found
        """
        exclude = exclude or []
        
        # Filter healthy proxies not in exclude list
        candidates = [
            name for name in self.available_proxies 
            if name not in exclude and self.proxy_stats.get(name, ProxyStats(name)).is_healthy
        ]
        
        if not candidates:
            self.logger.warning("No healthy proxies available, falling back to any available proxy")
            candidates = [name for name in self.available_proxies if name not in exclude]
        
        if not candidates:
            self.logger.error("No proxies available")
            return None
        
        # Sort by performance metrics
        def proxy_score(proxy_name: str) -> Tuple[float, float, int]:
            stats = self.proxy_stats.get(proxy_name, ProxyStats(proxy_name))
            # Priority: success rate, low response time, fewer rate limits
            return (stats.success_rate, -stats.avg_response_time, -stats.rate_limited_count)
        
        candidates.sort(key=proxy_score, reverse=True)
        best_proxy = candidates[0]
        
        self.logger.info(f"Selected best proxy: {best_proxy} (success rate: {self.proxy_stats.get(best_proxy, ProxyStats(best_proxy)).success_rate:.1f}%)")
        return best_proxy
    
    def handle_request_error(self, response_code: int, proxy_name: str) -> str:
        """
        Handle request error and determine next action
        
        Args:
            response_code: HTTP response code
            proxy_name: Name of proxy that failed
            
        Returns:
            Action to take: 'switch', 'retry', 'abort'
        """
        is_rate_limit = response_code in self.rate_limit_codes
        
        # Record the failure
        if proxy_name in self.proxy_stats:
            self.proxy_stats[proxy_name].record_failure(is_rate_limit)
        
        if is_rate_limit:
            self.logger.warning(f"Rate limit detected (HTTP {response_code}) on proxy: {proxy_name}")
            return 'switch'
        
        elif response_code >= 500:
            self.logger.warning(f"Server error (HTTP {response_code}) on proxy: {proxy_name}")
            return 'retry' if self.proxy_stats.get(proxy_name, ProxyStats(proxy_name)).consecutive_failures < 2 else 'switch'
        
        else:
            self.logger.error(f"Client error (HTTP {response_code}) on proxy: {proxy_name}")
            return 'abort'
    
    def auto_switch_on_error(self, response_code: int) -> bool:
        """
        Automatically switch proxy when error is detected
        
        Args:
            response_code: HTTP response code that triggered the switch
            
        Returns:
            True if switch successful
        """
        current_proxy = self.get_current_proxy()
        if not current_proxy:
            self.logger.error("Cannot determine current proxy for auto-switch")
            return False
        
        action = self.handle_request_error(response_code, current_proxy)
        
        if action == 'switch':
            # Get next best proxy, excluding current one
            next_proxy = self.get_best_proxy(exclude=[current_proxy])
            
            if next_proxy:
                return self.switch_proxy(next_proxy)
            else:
                self.logger.error("No alternative proxy available for switching")
                return False
        
        return False
    
    def make_request_with_auto_switch(self, 
                                    url: str, 
                                    method: str = 'GET',
                                    max_switches: int = 3,
                                    **kwargs) -> requests.Response:
        """
        Make HTTP request with automatic proxy switching on errors
        
        Args:
            url: URL to request
            method: HTTP method
            max_switches: Maximum number of proxy switches to attempt
            **kwargs: Additional arguments for requests
            
        Returns:
            Response object
            
        Raises:
            requests.RequestException: If all proxies fail
        """
        switches_made = 0
        last_error = None
        
        while switches_made <= max_switches:
            current_proxy = self.get_current_proxy()
            
            try:
                start_time = time.time()
                
                # Make the request
                response = self.session.request(method, url, **kwargs)
                
                # Calculate response time
                response_time = time.time() - start_time
                
                # Check for rate limiting or errors
                if response.status_code in self.rate_limit_codes:
                    self.logger.warning(f"Rate limit response (HTTP {response.status_code}) from {current_proxy}")
                    
                    if switches_made < max_switches:
                        if self.auto_switch_on_error(response.status_code):
                            switches_made += 1
                            time.sleep(1)  # Brief delay before retry
                            continue
                        else:
                            break
                    else:
                        # Record the failure but return the response
                        if current_proxy and current_proxy in self.proxy_stats:
                            self.proxy_stats[current_proxy].record_failure(is_rate_limit=True)
                        return response
                
                # Success - record stats
                if current_proxy and current_proxy in self.proxy_stats:
                    self.proxy_stats[current_proxy].record_success(response_time)
                
                return response
                
            except requests.exceptions.RequestException as e:
                last_error = e
                self.logger.error(f"Request failed on proxy {current_proxy}: {e}")
                
                # Record failure
                if current_proxy and current_proxy in self.proxy_stats:
                    self.proxy_stats[current_proxy].record_failure()
                
                # Try switching proxy
                if switches_made < max_switches:
                    if self.auto_switch_on_error(500):  # Treat network errors as server errors
                        switches_made += 1
                        time.sleep(2)  # Longer delay for network errors
                        continue
                
                break
        
        # All proxies failed
        self.logger.error(f"All proxy attempts failed after {switches_made} switches")
        if last_error:
            raise last_error
        else:
            raise requests.exceptions.RequestException("All proxy attempts failed")
    
    def get_proxy_statistics(self) -> Dict[str, Any]:
        """Get comprehensive proxy statistics"""
        stats = {
            'total_proxies': len(self.available_proxies),
            'healthy_proxies': len([p for p in self.proxy_stats.values() if p.is_healthy]),
            'current_proxy': self.get_current_proxy(),
            'proxy_details': {},
            'summary': {
                'total_requests': sum(p.total_requests for p in self.proxy_stats.values()),
                'total_successes': sum(p.success_count for p in self.proxy_stats.values()),
                'total_failures': sum(p.failure_count for p in self.proxy_stats.values()),
                'total_rate_limits': sum(p.rate_limited_count for p in self.proxy_stats.values())
            }
        }
        
        # Individual proxy details
        for name, proxy_stats in self.proxy_stats.items():
            stats['proxy_details'][name] = {
                'success_rate': proxy_stats.success_rate,
                'total_requests': proxy_stats.total_requests,
                'consecutive_failures': proxy_stats.consecutive_failures,
                'is_healthy': proxy_stats.is_healthy,
                'avg_response_time': proxy_stats.avg_response_time,
                'rate_limited_count': proxy_stats.rate_limited_count,
                'last_success': proxy_stats.last_success.isoformat() if proxy_stats.last_success else None,
                'last_failure': proxy_stats.last_failure.isoformat() if proxy_stats.last_failure else None
            }
        
        return stats
    
    def health_check_all_proxies(self) -> Dict[str, bool]:
        """
        Perform health check on all available proxies
        
        Returns:
            Dict mapping proxy names to health status
        """
        self.logger.info("Starting proxy health check...")
        health_results = {}
        
        # Test URL - using a reliable service
        test_url = "https://httpbin.org/ip"
        
        current_proxy = self.get_current_proxy()
        
        for proxy_name in self.available_proxies:
            try:
                # Switch to proxy
                if self.switch_proxy(proxy_name):
                    # Test the proxy
                    response = self.session.get(test_url, timeout=10)
                    if response.status_code == 200:
                        health_results[proxy_name] = True
                        if proxy_name in self.proxy_stats:
                            self.proxy_stats[proxy_name].is_healthy = True
                            self.proxy_stats[proxy_name].consecutive_failures = 0
                    else:
                        health_results[proxy_name] = False
                        if proxy_name in self.proxy_stats:
                            self.proxy_stats[proxy_name].is_healthy = False
                else:
                    health_results[proxy_name] = False
                    
            except Exception as e:
                self.logger.warning(f"Health check failed for {proxy_name}: {e}")
                health_results[proxy_name] = False
                if proxy_name in self.proxy_stats:
                    self.proxy_stats[proxy_name].is_healthy = False
        
        # Restore original proxy
        if current_proxy and current_proxy != self.get_current_proxy():
            self.switch_proxy(current_proxy)
        
        healthy_count = sum(1 for status in health_results.values() if status)
        self.logger.info(f"Health check complete: {healthy_count}/{len(health_results)} proxies healthy")
        
        return health_results


def main():
    """Test the proxy manager"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Mihomo Proxy Manager')
    parser.add_argument('--api-url', default='http://127.0.0.1:9090', help='Mihomo API URL')
    parser.add_argument('--secret', default='', help='API secret')
    parser.add_argument('--test-url', default='https://tcgcsv.com/tcgplayer/categories', help='URL to test')
    parser.add_argument('--health-check', action='store_true', help='Run health check on all proxies')
    parser.add_argument('--stats', action='store_true', help='Show proxy statistics')
    
    args = parser.parse_args()
    
    # Create proxy manager
    manager = MihomoProxyManager(api_url=args.api_url, secret=args.secret)
    
    if args.health_check:
        # Run health check
        results = manager.health_check_all_proxies()
        print(f"\n=== Proxy Health Check Results ===")
        for proxy, is_healthy in results.items():
            status = "✅ Healthy" if is_healthy else "❌ Unhealthy"
            print(f"{proxy}: {status}")
    
    elif args.stats:
        # Show statistics
        stats = manager.get_proxy_statistics()
        print(f"\n=== Proxy Statistics ===")
        print(f"Total proxies: {stats['total_proxies']}")
        print(f"Healthy proxies: {stats['healthy_proxies']}")
        print(f"Current proxy: {stats['current_proxy']}")
        print(f"Total requests: {stats['summary']['total_requests']}")
        print(f"Success rate: {stats['summary']['total_successes']}/{stats['summary']['total_requests']}")
    
    else:
        # Test automatic switching
        print(f"Testing automatic proxy switching with URL: {args.test_url}")
        try:
            response = manager.make_request_with_auto_switch(args.test_url)
            print(f"✅ Request successful: HTTP {response.status_code}")
            if response.headers.get('content-type', '').startswith('application/json'):
                data = response.json()
                print(f"Response: {len(data.get('results', data))} items")
        except Exception as e:
            print(f"❌ Request failed: {e}")


if __name__ == "__main__":
    main()