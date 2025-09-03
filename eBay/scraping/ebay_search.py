#!/usr/bin/env python3
"""
eBay Research API Search Script
Searches eBay sold items and retrieves price/sales metrics
"""

import argparse
import json
import requests
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, Optional, Any
from urllib.parse import quote, urlencode
import logging
from ebay_excel_utils import create_pivot_dataframes, save_pivot_to_excel

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class eBaySearchAPI:
    """eBay Research API client for searching sold items and getting metrics"""
    
    BASE_URL = "https://www.ebay.com/sh/research/api/search"
    
    # Default headers mimicking browser request
    DEFAULT_HEADERS = {
        'accept': '*/*',
        'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,pl;q=0.5',
        'cache-control': 'no-cache',
        'expires': 'Sat, 01 Jan 2000 00:00:00 GMT',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'sec-ch-ua': '"Not;A=Brand";v="99", "Microsoft Edge";v="139", "Chromium";v="139"',
        'sec-ch-ua-full-version': '"139.0.3405.125"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-model': '""',
        'sec-ch-ua-platform': '"Windows"',
        'sec-ch-ua-platform-version': '"12.0.0"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'x-requested-with': 'XMLHttpRequest',
    }
    
    def __init__(self, proxy: Optional[str] = None, cookies: Optional[str] = None, min_delay: float = 10.0):
        """
        Initialize eBay API client with rate limiting
        
        Args:
            proxy: Proxy URL (e.g., 'http://127.0.0.1:20171')
            cookies: Cookie string for authentication
            min_delay: Minimum delay between requests (default and minimum: 10 seconds)
        """
        self.session = requests.Session()
        self.session.headers.update(self.DEFAULT_HEADERS)
        
        # Rate limiting - enforce minimum 10 second delay
        self.min_delay = max(10.0, min_delay)
        self.last_request_time = 0
        
        if proxy:
            self.session.proxies = {
                'http': proxy,
                'https': proxy
            }
            logger.info(f"Using proxy: {proxy}")
        
        if cookies:
            self.session.headers['cookie'] = cookies
            logger.info("Cookies loaded")
        
        logger.info(f"Rate limiting enabled: {self.min_delay}s minimum delay")
    
    def _apply_rate_limit(self):
        """Apply rate limiting between requests"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_delay:
            wait_time = self.min_delay - time_since_last
            logger.info(f"Rate limiting: waiting {wait_time:.1f}s before next request...")
            
            # Show countdown for waits longer than 5 seconds
            if wait_time > 5:
                import sys
                for i in range(int(wait_time), 0, -1):
                    print(f"\rWaiting: {i}s ", end='', flush=True)
                    time.sleep(1)
                print("\r" + " " * 20 + "\r", end='', flush=True)
                time.sleep(wait_time - int(wait_time))
            else:
                time.sleep(wait_time)
        
        self.last_request_time = time.time()
    
    def search(
        self,
        keywords: str,
        days: int = 1095,  # Default 3 years
        marketplace: str = "EBAY-US",
        category_id: int = 0,
        offset: int = 0,
        limit: int = 50,
        tab_name: str = "SOLD",
        timezone: str = "Asia/Shanghai",
        modules: str = "metricsTrends"
    ) -> Dict[str, Any]:
        """
        Search eBay sold items with metrics and rate limiting
        
        Args:
            keywords: Search keywords
            days: Number of days to look back
            marketplace: eBay marketplace (EBAY-US, EBAY-UK, etc.)
            category_id: Category ID (0 for all categories)
            offset: Result offset for pagination
            limit: Number of results per page
            tab_name: Tab type (SOLD, ACTIVE, etc.)
            timezone: Timezone for date handling
            modules: API modules to include
        
        Returns:
            API response as dictionary
        """
        # Apply rate limiting BEFORE making the request
        self._apply_rate_limit()
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # Convert to milliseconds timestamp
        end_timestamp = int(end_date.timestamp() * 1000)
        start_timestamp = int(start_date.timestamp() * 1000)
        
        # Build query parameters
        params = {
            'marketplace': marketplace,
            'keywords': keywords,
            'dayRange': days,
            'endDate': end_timestamp,
            'startDate': start_timestamp,
            'categoryId': category_id,
            'offset': offset,
            'limit': limit,
            'tabName': tab_name,
            'tz': timezone,
            'modules': modules
        }
        
        # Build URL with parameters
        url = f"{self.BASE_URL}?{urlencode(params)}"
        
        # Set referer header
        referer_params = params.copy()
        referer_url = f"https://www.ebay.com/sh/research?{urlencode(referer_params)}"
        self.session.headers['Referer'] = referer_url
        
        logger.info(f"Searching for: {keywords}")
        logger.info(f"Date range: {start_date.date()} to {end_date.date()}")
        logger.info(f"URL: {url[:100]}...")
        
        try:
            # Make request with retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = self.session.get(url, timeout=30)
                    response.raise_for_status()
                    
                    # Parse JSON response - handle multiple JSON objects
                    if response.text:
                        # Check for authentication required FIRST
                        if "auth_required" in response.text.lower():
                            logger.error("AUTHENTICATION REQUIRED - Cookies have expired!")
                            logger.error("Please update ebay_cookies.txt with fresh cookies from browser")
                            raise RuntimeError("Authentication required - cookies expired. Please refresh cookies from eBay website.")
                        
                        # Save raw API response to permanent storage
                        from pathlib import Path
                        raw_response_dir = Path("raw_api_responses")
                        raw_response_dir.mkdir(exist_ok=True)
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
                        safe_keywords = "".join(c if c.isalnum() or c in '- ' else '_' for c in keywords)[:50]
                        raw_file = raw_response_dir / f"{timestamp}_{safe_keywords}_raw.json"
                        
                        # Save the raw response text
                        with open(raw_file, 'w', encoding='utf-8') as f:
                            f.write(response.text)
                        logger.debug(f"Raw API response saved: {raw_file.name}")
                        
                        # Split by newlines and parse each non-empty line as JSON
                        lines = response.text.strip().split('\n')
                        data = {}
                        
                        for i, line in enumerate(lines):
                            if line.strip():
                                try:
                                    json_obj = json.loads(line)
                                    # Use the module type as key if available
                                    if isinstance(json_obj, dict) and '_type' in json_obj:
                                        data[json_obj['_type']] = json_obj
                                    else:
                                        data[f'module_{i}'] = json_obj
                                except json.JSONDecodeError:
                                    logger.debug(f"Could not parse line {i}: {line[:100]}...")
                        
                        # Add raw response path to data for tracking
                        data['_raw_response_file'] = str(raw_file)
                        
                        # Check for error in response
                        if 'PageErrorModule' in data:
                            logger.error(f"API Error: {data['PageErrorModule']}")
                            if attempt < max_retries - 1:
                                logger.info(f"Retrying... (attempt {attempt + 2}/{max_retries})")
                                time.sleep(2 ** attempt)  # Exponential backoff
                                continue
                            return data
                    else:
                        data = {}
                    
                    logger.info("Search completed successfully")
                    return data
                    
                except requests.exceptions.RequestException as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"Request failed: {e}. Retrying...")
                        time.sleep(2 ** attempt)
                    else:
                        raise
                        
        except RuntimeError as e:
            # Re-raise authentication errors so they can be handled upstream
            if "authentication required" in str(e).lower():
                raise
            else:
                logger.error(f"Search failed: {e}")
                return {'error': str(e)}
                
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return {'error': str(e)}
    
    def extract_metrics(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract key metrics from API response
        
        Args:
            data: API response data
        
        Returns:
            Extracted metrics dictionary
        """
        metrics = {
            'search_timestamp': datetime.now().isoformat(),
            'data_points': 0,
            'avg_prices': [],
            'quantities': [],
            'date_range': {},
            'statistics': {}
        }
        
        # Check if we have valid metrics data
        if not isinstance(data, dict):
            return metrics
            
        # Find MetricsTrendsModule - check both direct key and values
        metrics_module = data.get('MetricsTrendsModule')
        
        if not metrics_module:
            # Search in values if not found by key
            for item in data.values():
                if isinstance(item, dict) and item.get('_type') == 'MetricsTrendsModule':
                    metrics_module = item
                    break
        
        if metrics_module:
            series = metrics_module.get('series', [])
            
            for serie in series:
                if serie.get('id') == 'averageSold':
                    # Extract average prices
                    price_data = serie.get('data', [])
                    metrics['avg_prices'] = [
                        {
                            'date': datetime.fromtimestamp(p[0]/1000).isoformat(),
                            'price': p[1]
                        }
                        for p in price_data if p[1] is not None
                    ]
                    
                    # Calculate statistics
                    prices = [p[1] for p in price_data if p[1] is not None]
                    if prices:
                        metrics['statistics']['avg_price'] = sum(prices) / len(prices)
                        metrics['statistics']['min_price'] = min(prices)
                        metrics['statistics']['max_price'] = max(prices)
                        metrics['statistics']['price_range'] = max(prices) - min(prices)
                
                elif serie.get('id') == 'quantity':
                    # Extract quantities
                    qty_data = serie.get('data', [])
                    metrics['quantities'] = [
                        {
                            'date': datetime.fromtimestamp(q[0]/1000).isoformat(),
                            'quantity': q[1]
                        }
                        for q in qty_data if q[1] is not None
                    ]
                    
                    # Calculate statistics
                    quantities = [q[1] for q in qty_data if q[1] is not None]
                    if quantities:
                        metrics['statistics']['total_sold'] = sum(quantities)
                        metrics['statistics']['avg_weekly_sales'] = sum(quantities) / len(quantities)
                        metrics['statistics']['min_weekly_sales'] = min(quantities)
                        metrics['statistics']['max_weekly_sales'] = max(quantities)
            
            metrics['data_points'] = len(metrics['avg_prices'])
            
            # Get date range
            if metrics['avg_prices']:
                metrics['date_range'] = {
                    'start': metrics['avg_prices'][0]['date'],
                    'end': metrics['avg_prices'][-1]['date']
                }
        
        return metrics


def load_cookies_from_file(filepath: str) -> Optional[str]:
    """Load cookies from a file"""
    try:
        with open(filepath, 'r') as f:
            cookies = f.read().strip()
        logger.info(f"Loaded cookies from {filepath}")
        return cookies
    except FileNotFoundError:
        logger.warning(f"Cookie file not found: {filepath}")
        return None
    except Exception as e:
        logger.error(f"Error loading cookies: {e}")
        return None


def save_results(data: Dict[str, Any], output_file: str, pretty: bool = True):
    """Save results to JSON file"""
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            if pretty:
                json.dump(data, f, indent=2, ensure_ascii=False)
            else:
                json.dump(data, f, ensure_ascii=False)
        logger.info(f"Results saved to: {output_file}")
    except Exception as e:
        logger.error(f"Error saving results: {e}")


def save_results_to_excel(
    data: Dict[str, Any], 
    keywords: str, 
    output_file: str,
    add_charts: bool = True
):
    """
    Save results to Excel file in pivot table format
    
    Args:
        data: API response data
        keywords: Search keywords
        output_file: Output Excel file path
        add_charts: Whether to add trend charts
    """
    try:
        # Prepare data in the format expected by create_pivot_dataframes
        search_results = [{
            'keywords': keywords,
            'data': data
        }]
        
        # Create pivot DataFrames
        price_df, quantity_df = create_pivot_dataframes(search_results)
        
        # Add metadata
        metadata = {
            'Keywords': keywords,
            'Generated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'Marketplace': data.get('search_metadata', {}).get('marketplace', 'EBAY-US'),
            'Days Searched': data.get('search_metadata', {}).get('days', 'N/A'),
            'Data Points': len(price_df.columns) if not price_df.empty else 0
        }
        
        # Extract statistics if available
        if 'extracted_metrics' in data:
            stats = data['extracted_metrics'].get('statistics', {})
            if stats:
                metadata.update({
                    'Average Price': f"${stats.get('avg_price', 0):.2f}",
                    'Total Sold': f"{stats.get('total_sold', 0):,}",
                    'Date Range': f"{data['extracted_metrics'].get('date_range', {}).get('start', 'N/A')[:10]} to {data['extracted_metrics'].get('date_range', {}).get('end', 'N/A')[:10]}"
                })
        
        # Save to Excel
        save_pivot_to_excel(
            price_df,
            quantity_df,
            output_file,
            metadata=metadata,
            add_charts=add_charts,
            add_formatting=True
        )
        
        logger.info(f"Excel file saved to: {output_file}")
        
    except Exception as e:
        logger.error(f"Error saving Excel file: {e}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Search eBay sold items and retrieve price/sales metrics',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s "pokemon cards"
  %(prog)s "magic the gathering" --days 365 --output mtg_data.json
  %(prog)s "vintage electronics" --limit 100 --offset 50
  %(prog)s "sports cards" --marketplace EBAY-UK --no-proxy
  %(prog)s "pokemon cards" --excel --output pokemon_pivot.xlsx
  %(prog)s "trading cards" --excel --no-charts
        """
    )
    
    # Required arguments
    parser.add_argument('keywords', help='Search keywords')
    
    # Optional arguments
    parser.add_argument('--output', '-o', help='Output file (default: auto-generated with .json or .xlsx extension)')
    parser.add_argument('--days', '-d', type=int, default=1095, help='Number of days to look back (default: 1095, ~3 years)')
    parser.add_argument('--marketplace', '-m', default='EBAY-US', help='eBay marketplace (default: EBAY-US)')
    parser.add_argument('--category', '-c', type=int, default=0, help='Category ID (default: 0 for all)')
    parser.add_argument('--offset', type=int, default=0, help='Result offset for pagination')
    parser.add_argument('--limit', '-l', type=int, default=50, help='Results per page (default: 50, max: 200)')
    parser.add_argument('--proxy', '-p', default='http://127.0.0.1:20171', help='Proxy URL (default: http://127.0.0.1:20171)')
    parser.add_argument('--no-proxy', action='store_true', help='Disable proxy')
    parser.add_argument('--cookie-file', help='File containing cookies')
    parser.add_argument('--extract-metrics', action='store_true', help='Extract and display key metrics')
    parser.add_argument('--compact', action='store_true', help='Save JSON in compact format')
    parser.add_argument('--excel', action='store_true', help='Save as Excel pivot table instead of JSON')
    parser.add_argument('--no-charts', action='store_true', help='Disable charts in Excel output')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Configure logging
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Determine output filename and format
    output_format = 'excel' if args.excel else 'json'
    
    if args.output:
        # Check if user specified format via extension
        if args.output.endswith('.xlsx'):
            output_format = 'excel'
        elif args.output.endswith('.json'):
            output_format = 'json'
    else:
        # Auto-generate filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_keywords = args.keywords.replace(' ', '_').replace('/', '_')[:50]
        extension = '.xlsx' if output_format == 'excel' else '.json'
        args.output = f"ebay_search_{safe_keywords}_{timestamp}{extension}"
    
    # Load cookies if provided
    cookies = None
    if args.cookie_file:
        cookies = load_cookies_from_file(args.cookie_file)
    else:
        # Try to load from default location
        cookies = load_cookies_from_file('ebay_cookies.txt')
    
    # Determine proxy
    proxy = None if args.no_proxy else args.proxy
    
    # Create API client
    api = eBaySearchAPI(proxy=proxy, cookies=cookies)
    
    # Perform search
    logger.info(f"Searching eBay for: {args.keywords}")
    results = api.search(
        keywords=args.keywords,
        days=args.days,
        marketplace=args.marketplace,
        category_id=args.category,
        offset=args.offset,
        limit=args.limit
    )
    
    # Check for errors
    if 'error' in results:
        logger.error(f"Search failed: {results['error']}")
        sys.exit(1)
    
    # Extract metrics if requested
    if args.extract_metrics:
        metrics = api.extract_metrics(results)
        
        # Display metrics summary
        if metrics['statistics']:
            logger.info("\n=== METRICS SUMMARY ===")
            stats = metrics['statistics']
            
            if 'avg_price' in stats:
                logger.info(f"Average Price: ${stats['avg_price']:.2f}")
                logger.info(f"Price Range: ${stats['min_price']:.2f} - ${stats['max_price']:.2f}")
            
            if 'total_sold' in stats:
                logger.info(f"Total Items Sold: {stats['total_sold']:,}")
                logger.info(f"Avg Weekly Sales: {stats['avg_weekly_sales']:.0f}")
                logger.info(f"Sales Range: {stats['min_weekly_sales']} - {stats['max_weekly_sales']}")
            
            if metrics['date_range']:
                logger.info(f"Date Range: {metrics['date_range']['start'][:10]} to {metrics['date_range']['end'][:10]}")
            
            logger.info(f"Data Points: {metrics['data_points']}")
        
        # Add metrics to results
        results['extracted_metrics'] = metrics
    
    # Add metadata
    results['search_metadata'] = {
        'keywords': args.keywords,
        'marketplace': args.marketplace,
        'days': args.days,
        'limit': args.limit,
        'offset': args.offset,
        'timestamp': datetime.now().isoformat(),
        'output_file': args.output
    }
    
    # Save results based on format
    if output_format == 'excel':
        save_results_to_excel(
            results, 
            args.keywords, 
            args.output,
            add_charts=not args.no_charts
        )
    else:
        save_results(results, args.output, pretty=not args.compact)
    
    # Display summary
    logger.info(f"\n✓ Search completed successfully")
    logger.info(f"✓ Results saved to: {args.output} ({'Excel pivot table' if output_format == 'excel' else 'JSON'})")
    
    # Check if we got valid data
    has_metrics = 'MetricsTrendsModule' in results or any(
        item.get('_type') == 'MetricsTrendsModule' 
        for item in results.values() 
        if isinstance(item, dict)
    )
    
    if has_metrics:
        logger.info("✓ Metrics data retrieved successfully")
    else:
        logger.warning("⚠ No metrics data found in response (may need valid cookies)")


if __name__ == '__main__':
    main()