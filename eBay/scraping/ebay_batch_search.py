#!/usr/bin/env python3
"""
eBay Batch Search Script
Perform multiple searches and compare results
"""

import argparse
import json
import time
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
import logging

from ebay_search import eBaySearchAPI, load_cookies_from_file

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class eBayBatchSearcher:
    """Batch search handler for multiple eBay searches"""
    
    def __init__(self, api: eBaySearchAPI):
        self.api = api
        self.results = []
    
    def search_multiple(
        self,
        keywords_list: List[str],
        delay: float = 2.0,
        **search_params
    ) -> List[Dict[str, Any]]:
        """
        Perform multiple searches with delay between requests
        
        Args:
            keywords_list: List of search keywords
            delay: Delay between searches in seconds
            **search_params: Common search parameters
        
        Returns:
            List of search results
        """
        results = []
        total = len(keywords_list)
        
        for idx, keywords in enumerate(keywords_list, 1):
            logger.info(f"\n[{idx}/{total}] Searching: {keywords}")
            
            # Perform search
            result = self.api.search(keywords=keywords, **search_params)
            
            # Extract metrics
            metrics = self.api.extract_metrics(result)
            
            # Store result with metadata
            search_result = {
                'keywords': keywords,
                'timestamp': datetime.now().isoformat(),
                'metrics': metrics,
                'raw_data': result
            }
            
            results.append(search_result)
            
            # Display quick summary
            if metrics['statistics']:
                stats = metrics['statistics']
                if 'avg_price' in stats:
                    logger.info(f"  → Avg Price: ${stats['avg_price']:.2f}")
                if 'total_sold' in stats:
                    logger.info(f"  → Total Sold: {stats['total_sold']:,}")
            
            # Delay between requests (except for last one)
            if idx < total:
                logger.info(f"  → Waiting {delay}s before next search...")
                time.sleep(delay)
        
        self.results = results
        return results
    
    def compare_results(self) -> pd.DataFrame:
        """
        Create comparison table of all search results
        
        Returns:
            DataFrame with comparison data
        """
        if not self.results:
            logger.warning("No results to compare")
            return pd.DataFrame()
        
        comparison_data = []
        
        for result in self.results:
            row = {
                'Keywords': result['keywords'],
                'Search Time': result['timestamp'][:19]
            }
            
            stats = result['metrics'].get('statistics', {})
            row.update({
                'Avg Price': f"${stats.get('avg_price', 0):.2f}" if 'avg_price' in stats else 'N/A',
                'Min Price': f"${stats.get('min_price', 0):.2f}" if 'min_price' in stats else 'N/A',
                'Max Price': f"${stats.get('max_price', 0):.2f}" if 'max_price' in stats else 'N/A',
                'Total Sold': f"{stats.get('total_sold', 0):,}" if 'total_sold' in stats else 'N/A',
                'Avg Weekly': f"{stats.get('avg_weekly_sales', 0):.0f}" if 'avg_weekly_sales' in stats else 'N/A',
                'Data Points': result['metrics'].get('data_points', 0)
            })
            
            comparison_data.append(row)
        
        return pd.DataFrame(comparison_data)
    
    def generate_report(self, output_dir: str = '.'):
        """
        Generate comprehensive report with all results
        
        Args:
            output_dir: Directory to save report files
        """
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save raw results
        raw_file = output_path / f"batch_results_{timestamp}.json"
        with open(raw_file, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        logger.info(f"Raw results saved to: {raw_file}")
        
        # Save comparison table
        df = self.compare_results()
        if not df.empty:
            csv_file = output_path / f"comparison_{timestamp}.csv"
            df.to_csv(csv_file, index=False)
            logger.info(f"Comparison table saved to: {csv_file}")
            
            # Also save as Excel if pandas has xlsxwriter
            try:
                excel_file = output_path / f"comparison_{timestamp}.xlsx"
                df.to_excel(excel_file, index=False, engine='openpyxl')
                logger.info(f"Excel report saved to: {excel_file}")
            except ImportError:
                logger.warning("openpyxl not installed, skipping Excel export")
        
        # Generate summary statistics
        self._generate_summary(output_path / f"summary_{timestamp}.txt")
    
    def _generate_summary(self, filepath: Path):
        """Generate text summary of all searches"""
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write("eBay Batch Search Summary\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total Searches: {len(self.results)}\n\n")
            
            for idx, result in enumerate(self.results, 1):
                f.write(f"\n{idx}. {result['keywords']}\n")
                f.write("-" * 40 + "\n")
                
                stats = result['metrics'].get('statistics', {})
                if stats:
                    if 'avg_price' in stats:
                        f.write(f"  Average Price: ${stats['avg_price']:.2f}\n")
                        f.write(f"  Price Range: ${stats['min_price']:.2f} - ${stats['max_price']:.2f}\n")
                    
                    if 'total_sold' in stats:
                        f.write(f"  Total Sold: {stats['total_sold']:,}\n")
                        f.write(f"  Avg Weekly Sales: {stats['avg_weekly_sales']:.0f}\n")
                    
                    date_range = result['metrics'].get('date_range', {})
                    if date_range:
                        f.write(f"  Date Range: {date_range.get('start', 'N/A')[:10]} to {date_range.get('end', 'N/A')[:10]}\n")
                else:
                    f.write("  No metrics data available\n")
                
                f.write("\n")
        
        logger.info(f"Summary saved to: {filepath}")


def load_keywords_from_file(filepath: str) -> List[str]:
    """Load keywords from file (one per line)"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            keywords = [line.strip() for line in f if line.strip()]
        logger.info(f"Loaded {len(keywords)} keywords from {filepath}")
        return keywords
    except Exception as e:
        logger.error(f"Error loading keywords file: {e}")
        return []


def main():
    """Main entry point for batch search"""
    parser = argparse.ArgumentParser(
        description='Batch search eBay for multiple keywords',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s "pokemon cards" "magic the gathering" "yugioh"
  %(prog)s --file keywords.txt --days 365
  %(prog)s --file cards.txt --output-dir results/ --delay 3
        """
    )
    
    # Keyword input options
    parser.add_argument('keywords', nargs='*', help='Search keywords (can specify multiple)')
    parser.add_argument('--file', '-f', help='File containing keywords (one per line)')
    
    # Search parameters
    parser.add_argument('--days', '-d', type=int, default=1095, help='Number of days to look back')
    parser.add_argument('--marketplace', '-m', default='EBAY-US', help='eBay marketplace')
    parser.add_argument('--limit', '-l', type=int, default=50, help='Results per search')
    parser.add_argument('--delay', type=float, default=2.0, help='Delay between searches (seconds)')
    
    # Output options
    parser.add_argument('--output-dir', '-o', default='.', help='Output directory for results')
    parser.add_argument('--no-report', action='store_true', help='Skip report generation')
    
    # Connection options
    parser.add_argument('--proxy', '-p', default='http://127.0.0.1:20171', help='Proxy URL')
    parser.add_argument('--no-proxy', action='store_true', help='Disable proxy')
    parser.add_argument('--cookie-file', help='File containing cookies')
    
    args = parser.parse_args()
    
    # Collect keywords
    keywords_list = []
    
    if args.file:
        keywords_list.extend(load_keywords_from_file(args.file))
    
    if args.keywords:
        keywords_list.extend(args.keywords)
    
    if not keywords_list:
        logger.error("No keywords provided. Use positional arguments or --file option")
        return 1
    
    logger.info(f"Preparing to search {len(keywords_list)} keywords")
    
    # Load cookies
    cookies = None
    if args.cookie_file:
        cookies = load_cookies_from_file(args.cookie_file)
    else:
        cookies = load_cookies_from_file('ebay_cookies.txt')
    
    # Create API client
    proxy = None if args.no_proxy else args.proxy
    api = eBaySearchAPI(proxy=proxy, cookies=cookies)
    
    # Create batch searcher
    searcher = eBayBatchSearcher(api)
    
    # Perform searches
    search_params = {
        'days': args.days,
        'marketplace': args.marketplace,
        'limit': args.limit
    }
    
    logger.info(f"\nStarting batch search with {args.delay}s delay between searches...")
    results = searcher.search_multiple(keywords_list, delay=args.delay, **search_params)
    
    # Generate report
    if not args.no_report:
        logger.info("\nGenerating reports...")
        searcher.generate_report(args.output_dir)
        
        # Display comparison table
        df = searcher.compare_results()
        if not df.empty:
            logger.info("\n" + "=" * 80)
            logger.info("COMPARISON TABLE")
            logger.info("=" * 80)
            print(df.to_string(index=False))
    
    logger.info(f"\n✓ Batch search completed: {len(results)} searches")
    
    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main() or 0)