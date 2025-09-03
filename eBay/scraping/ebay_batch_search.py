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
from typing import List, Dict, Any, Optional
import logging
import sys

from ebay_search import eBaySearchAPI, load_cookies_from_file
from ebay_excel_utils import create_pivot_dataframes, save_pivot_to_excel, create_summary_pivot
from ebay_resume_manager import ResumeManager, list_sessions, cleanup_old_sessions

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    logging.warning("tqdm not installed, progress bar disabled")

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
        delay: float = 10.0,  # Changed default to 10s minimum
        **search_params
    ) -> List[Dict[str, Any]]:
        """
        Perform multiple searches with delay between requests
        NOTE: Minimum delay is 10 seconds, enforced by eBaySearchAPI
        
        Args:
            keywords_list: List of search keywords
            delay: Delay between searches in seconds (minimum 10s)
            **search_params: Common search parameters
        
        Returns:
            List of search results
        """
        results = []
        total = len(keywords_list)
        
        # Note: delay is handled by eBaySearchAPI._apply_rate_limit()
        # This delay parameter is kept for backward compatibility
        
        for idx, keywords in enumerate(keywords_list, 1):
            logger.info(f"\n[{idx}/{total}] Searching: {keywords}")
            
            # Perform search (rate limiting applied internally)
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
        
        self.results = results
        return results
    
    def search_multiple_with_resume(
        self,
        keywords_list: List[str],
        resume_manager: ResumeManager,
        continue_on_error: bool = False,
        retry_failed: bool = False,
        checkpoint_interval: int = 10,
        **search_params
    ) -> List[Dict[str, Any]]:
        """
        Enhanced search with resume capability and progress tracking
        
        Args:
            keywords_list: List of search keywords
            resume_manager: Resume manager instance
            continue_on_error: Continue searching even if some fail
            retry_failed: Retry previously failed searches
            checkpoint_interval: Save checkpoint every N searches
            **search_params: Common search parameters
        
        Returns:
            List of search results
        """
        # Update total keywords in state
        resume_manager.state['total_keywords'] = len(keywords_list)
        resume_manager.state['search_params'] = search_params
        resume_manager.save_state()
        
        # Get already completed keywords
        completed = resume_manager.get_completed_keywords()
        failed = resume_manager.get_failed_keywords() if retry_failed else []
        
        # Determine what needs to be searched
        remaining = resume_manager.get_pending_keywords(keywords_list)
        
        # Add failed keywords for retry if requested
        if retry_failed and failed:
            remaining.extend(failed)
            logger.info(f"Adding {len(failed)} failed keywords for retry")
        
        # Load previous results if resuming
        results = []
        if completed:
            results = resume_manager.load_previous_results()
            logger.info(f"Resuming session {resume_manager.session_id}")
            logger.info(f"  ✓ {len(completed)} completed")
            logger.info(f"  → {len(remaining)} remaining")
            if resume_manager.state['failed'] > 0:
                logger.info(f"  ✗ {resume_manager.state['failed']} failed")
        
        # Progress tracking
        total = len(keywords_list)
        initial = len(completed)
        
        if HAS_TQDM:
            progress_bar = tqdm(
                total=total,
                initial=initial,
                desc="Searching",
                unit="keyword",
                bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]'
            )
        else:
            progress_bar = None
        
        # Process remaining keywords
        for idx, keywords in enumerate(remaining):
            search_start = time.time()
            
            try:
                # Mark as in progress
                resume_manager.mark_in_progress(keywords)
                
                # Log current search
                current_idx = len(completed) + idx + 1
                logger.info(f"\n[{current_idx}/{total}] Searching: {keywords}")
                
                # Perform search (rate limiting applied internally)
                result = self.api.search(keywords=keywords, **search_params)
                
                # Calculate duration
                duration = time.time() - search_start
                
                # Save to resume manager
                resume_manager.save_search_result(
                    keywords, result, "success", duration=duration
                )
                
                # Extract metrics
                metrics = self.api.extract_metrics(result)
                
                # Add to results
                search_result = {
                    'keywords': keywords,
                    'timestamp': datetime.now().isoformat(),
                    'metrics': metrics,
                    'raw_data': result
                }
                results.append(search_result)
                
                # Display summary
                if metrics['statistics']:
                    stats = metrics['statistics']
                    if 'avg_price' in stats:
                        logger.info(f"  → Avg Price: ${stats['avg_price']:.2f}")
                    if 'total_sold' in stats:
                        logger.info(f"  → Total Sold: {stats['total_sold']:,}")
                
                # Record success for rate limiter
                resume_manager.rate_limiter.record_success()
                
                # Checkpoint periodically
                if current_idx % checkpoint_interval == 0:
                    resume_manager.save_checkpoint()
                    logger.debug(f"Checkpoint saved at search {current_idx}")
                
            except KeyboardInterrupt:
                logger.warning("\nSearch interrupted by user")
                resume_manager.save_checkpoint()
                if progress_bar:
                    progress_bar.close()
                raise
                
            except RuntimeError as e:
                # Check for authentication errors
                if "authentication required" in str(e).lower():
                    logger.error("\n" + "="*60)
                    logger.error("AUTHENTICATION ERROR - STOPPING SEARCH")
                    logger.error("="*60)
                    logger.error("Cookies have expired. Please:")
                    logger.error("1. Open https://www.ebay.com/sh/research in browser")
                    logger.error("2. Press F12 → Network tab → Search something")
                    logger.error("3. Find /sh/research/api/search request")
                    logger.error("4. Copy entire 'cookie' header value")
                    logger.error("5. Save to ebay_cookies.txt")
                    logger.error("6. Resume with: python3 ebay_batch_search.py --resume last --file keywords.txt")
                    logger.error("="*60)
                    
                    # Save checkpoint before exiting
                    resume_manager.save_checkpoint()
                    if progress_bar:
                        progress_bar.close()
                    
                    # Exit with error
                    import sys
                    sys.exit(1)
                else:
                    raise  # Re-raise other runtime errors
                
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Failed: {keywords} - {error_msg}")
                
                # Save failed result
                resume_manager.save_search_result(
                    keywords,
                    {"error": error_msg},
                    "failed",
                    error=error_msg,
                    duration=time.time() - search_start
                )
                
                # Record error for rate limiter
                resume_manager.rate_limiter.record_error()
                
                if not continue_on_error:
                    logger.error("Stopping due to error. Use --continue-on-error to skip failures")
                    if progress_bar:
                        progress_bar.close()
                    raise
            
            finally:
                # Update progress
                if progress_bar:
                    progress_bar.update(1)
        
        if progress_bar:
            progress_bar.close()
        
        # Final summary
        summary = resume_manager.get_session_summary()
        logger.info("\n" + "="*60)
        logger.info("SEARCH COMPLETED")
        logger.info("="*60)
        logger.info(f"Total: {summary['progress']['total']}")
        logger.info(f"Completed: {summary['progress']['completed']}")
        logger.info(f"Failed: {summary['progress']['failed']}")
        logger.info(f"Duration: {summary['performance']['total_duration_human']}")
        logger.info(f"Avg time per search: {summary['performance']['avg_search_time']:.1f}s")
        logger.info("="*60)
        
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
    
    def generate_pivot_excel(
        self, 
        output_file: str,
        time_period: str = 'weekly',
        add_charts: bool = True
    ):
        """
        Generate Excel file with pivot tables for all search results
        
        Args:
            output_file: Output Excel file path
            time_period: 'weekly', 'monthly', or 'quarterly'
            add_charts: Whether to add trend charts
        """
        try:
            if not self.results:
                logger.warning("No results to save to Excel")
                return
            
            # Use the utility function to create pivot Excel
            create_summary_pivot(
                self.results,
                output_file,
                time_period=time_period,
                add_statistics=True,
                add_charts=add_charts
            )
            
            logger.info(f"Pivot Excel saved to: {output_file}")
            
        except Exception as e:
            logger.error(f"Error generating pivot Excel: {e}")
    
    def generate_report(self, output_dir: str = '.', generate_pivot: bool = True):
        """
        Generate comprehensive report with all results
        
        Args:
            output_dir: Directory to save report files
            generate_pivot: Whether to generate pivot Excel file
        """
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save raw results
        raw_file = output_path / f"batch_results_{timestamp}.json"
        with open(raw_file, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        logger.info(f"Raw results saved to: {raw_file}")
        
        # Generate pivot Excel if requested
        if generate_pivot:
            pivot_file = output_path / f"pivot_table_{timestamp}.xlsx"
            self.generate_pivot_excel(str(pivot_file))
        
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
        description='Batch search eBay for multiple keywords with resume capability',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s "pokemon cards" "magic the gathering" "yugioh"
  %(prog)s --file keywords.txt --days 365
  %(prog)s --file cards.txt --output-dir results/ --min-delay 15
  %(prog)s "pokemon" "magic" "yugioh" --excel-pivot trading_cards
  %(prog)s --file keywords.txt --excel-pivot analysis --time-period monthly
  
Resume Examples:
  %(prog)s --resume last --file keywords.txt
  %(prog)s --resume 20250902_143022 --file keywords.txt --retry-failed
  %(prog)s --list-sessions
  %(prog)s --cleanup-sessions 7
        """
    )
    
    # Keyword input options
    parser.add_argument('keywords', nargs='*', help='Search keywords (can specify multiple)')
    parser.add_argument('--file', '-f', help='File containing keywords (one per line)')
    
    # Search parameters
    parser.add_argument('--days', '-d', type=int, default=1095, help='Number of days to look back')
    parser.add_argument('--marketplace', '-m', default='EBAY-US', help='eBay marketplace')
    parser.add_argument('--limit', '-l', type=int, default=50, help='Results per search')
    parser.add_argument('--delay', type=float, default=60.0, help='DEPRECATED: Use --min-delay instead')
    parser.add_argument('--min-delay', type=float, default=60.0, help='Minimum delay between requests (default: 60s, minimum: 60s)')
    
    # Resume and session options
    parser.add_argument('--resume', help='Resume session ID or "last" for most recent')
    parser.add_argument('--temp-dir', default='.ebay_temp', help='Temp directory for sessions')
    parser.add_argument('--keep-temp', action='store_true', help='Keep temp files after completion')
    parser.add_argument('--checkpoint-every', type=int, default=10, help='Save checkpoint every N searches')
    parser.add_argument('--retry-failed', action='store_true', help='Retry previously failed searches')
    parser.add_argument('--continue-on-error', action='store_true', help='Continue searching even if some fail')
    parser.add_argument('--list-sessions', action='store_true', help='List all available sessions and exit')
    parser.add_argument('--cleanup-sessions', type=int, metavar='DAYS', help='Clean up sessions older than N days and exit')
    
    # Output options
    parser.add_argument('--output-dir', '-o', default='.', help='Output directory for results')
    parser.add_argument('--no-report', action='store_true', help='Skip report generation')
    parser.add_argument('--excel-pivot', help='Generate pivot Excel file with specified name')
    parser.add_argument('--time-period', choices=['weekly', 'monthly', 'quarterly'], default='weekly', help='Time period for pivot aggregation')
    parser.add_argument('--no-charts', action='store_true', help='Disable charts in Excel output')
    parser.add_argument('--export-on-interrupt', action='store_true', help='Export results if interrupted')
    
    # Connection options
    parser.add_argument('--proxy', '-p', default='http://127.0.0.1:20171', help='Proxy URL')
    parser.add_argument('--no-proxy', action='store_true', help='Disable proxy')
    parser.add_argument('--cookie-file', help='File containing cookies')
    
    args = parser.parse_args()
    
    # Handle special operations
    if args.list_sessions:
        sessions = list_sessions(args.temp_dir)
        if not sessions:
            print("No sessions found")
            return 0
        
        print("\nAvailable Sessions:")
        print("-" * 80)
        for session in sessions:
            print(f"ID: {session['session_id']}")
            print(f"  Started: {session['start_time']}")
            print(f"  Last Update: {session['last_update']}")
            print(f"  Progress: {session['completed']}/{session['total']} completed")
            print(f"  Failed: {session['failed']}")
            print(f"  Status: {session['status']}")
            print()
        return 0
    
    if args.cleanup_sessions is not None:
        cleaned = cleanup_old_sessions(args.temp_dir, days=args.cleanup_sessions)
        print(f"Cleaned up {cleaned} old sessions (older than {args.cleanup_sessions} days)")
        return 0
    
    # Collect keywords
    keywords_list = []
    
    if args.file:
        keywords_list.extend(load_keywords_from_file(args.file))
    
    if args.keywords:
        keywords_list.extend(args.keywords)
    
    # For resume, we might not need keywords immediately
    if not keywords_list and not args.resume:
        logger.error("No keywords provided. Use positional arguments or --file option")
        return 1
    
    # Load cookies
    cookies = None
    if args.cookie_file:
        cookies = load_cookies_from_file(args.cookie_file)
    else:
        cookies = load_cookies_from_file('ebay_cookies.txt')
    
    # Create API client with min_delay
    proxy = None if args.no_proxy else args.proxy
    min_delay = max(args.min_delay, args.delay)  # Use the larger of the two for compatibility
    api = eBaySearchAPI(proxy=proxy, cookies=cookies, min_delay=min_delay)
    
    # Create batch searcher
    searcher = eBayBatchSearcher(api)
    
    # Prepare search parameters
    search_params = {
        'days': args.days,
        'marketplace': args.marketplace,
        'limit': args.limit
    }
    
    # Check if we're resuming or starting fresh
    if args.resume or Path(args.temp_dir).exists():
        # Use resume manager
        try:
            resume_manager = ResumeManager(
                session_id=args.resume,
                temp_dir=args.temp_dir,
                auto_save=True,
                enable_lock=True
            )
            
            # If resuming, load keywords from state if not provided
            if args.resume and not keywords_list:
                # Try to get keywords from previous search params
                if 'keywords_file' in resume_manager.state.get('search_params', {}):
                    keywords_list = load_keywords_from_file(
                        resume_manager.state['search_params']['keywords_file']
                    )
                else:
                    logger.error("Cannot resume: no keywords provided and none found in session")
                    return 1
            
            # Store keywords file for future resume
            if args.file:
                search_params['keywords_file'] = args.file
            
            logger.info(f"\nStarting batch search with resume capability")
            logger.info(f"Session ID: {resume_manager.session_id}")
            logger.info(f"Min delay: {min_delay}s")
            
            # Perform search with resume
            results = searcher.search_multiple_with_resume(
                keywords_list,
                resume_manager,
                continue_on_error=args.continue_on_error,
                retry_failed=args.retry_failed,
                checkpoint_interval=args.checkpoint_every,
                **search_params
            )
            
            # Clean up or archive session
            if not args.keep_temp and resume_manager.state['failed'] == 0:
                resume_manager.cleanup(archive=True)
            
        except Exception as e:
            logger.error(f"Resume manager error: {e}")
            logger.info("Falling back to standard search mode")
            
            # Fall back to regular search
            logger.info(f"\nStarting batch search (standard mode)")
            results = searcher.search_multiple(keywords_list, **search_params)
    
    else:
        # Standard search without resume
        logger.info(f"Preparing to search {len(keywords_list)} keywords")
        logger.info(f"\nStarting batch search (standard mode)")
        logger.info(f"Min delay: {min_delay}s")
        
        results = searcher.search_multiple(keywords_list, **search_params)
    
    # Generate Excel pivot if requested
    if args.excel_pivot:
        output_path = Path(args.output_dir)
        output_path.mkdir(exist_ok=True)
        
        if not args.excel_pivot.endswith('.xlsx'):
            excel_file = output_path / f"{args.excel_pivot}.xlsx"
        else:
            excel_file = output_path / args.excel_pivot
        
        logger.info(f"\nGenerating pivot Excel: {excel_file}")
        searcher.generate_pivot_excel(
            str(excel_file),
            time_period=args.time_period,
            add_charts=not args.no_charts
        )
    
    # Generate report
    if not args.no_report:
        logger.info("\nGenerating reports...")
        searcher.generate_report(args.output_dir, generate_pivot=not args.excel_pivot)
        
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