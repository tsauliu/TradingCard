#!/usr/bin/env python3
"""
Production runner for Enhanced TCG Metadata Downloader
Supports all categories download with screen session management and monitoring
"""
import os
import sys
import argparse
import json
import subprocess
import time
from datetime import datetime
from typing import List, Optional, Dict
from dotenv import load_dotenv

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from enhanced_api_downloader import EnhancedTCGMetadataDownloader

class ProductionDownloadRunner:
    def __init__(self, screen_session_name: str = "tcg_full_download"):
        self.screen_session_name = screen_session_name
        self.log_file = "tcg_full_download.log"
        self.status_file = "download_status.json"
        
    def check_screen_session(self) -> bool:
        """Check if screen session exists"""
        try:
            result = subprocess.run(['screen', '-list'], capture_output=True, text=True)
            return self.screen_session_name in result.stdout
        except:
            return False
    
    def start_screen_session(self, command: str) -> bool:
        """Start download in screen session"""
        try:
            screen_cmd = [
                'screen', '-dmS', self.screen_session_name, 
                'bash', '-c', f"{command} 2>&1 | tee {self.log_file}"
            ]
            subprocess.run(screen_cmd, check=True)
            print(f"✅ Started screen session: {self.screen_session_name}")
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to start screen session: {e}")
            return False
    
    def attach_to_session(self):
        """Attach to existing screen session"""
        if self.check_screen_session():
            print(f"Attaching to session: {self.screen_session_name}")
            os.system(f"screen -r {self.screen_session_name}")
        else:
            print(f"❌ Screen session {self.screen_session_name} not found")
    
    def kill_session(self):
        """Kill existing screen session"""
        if self.check_screen_session():
            try:
                subprocess.run(['screen', '-S', self.screen_session_name, '-X', 'quit'], check=True)
                print(f"✅ Killed screen session: {self.screen_session_name}")
                return True
            except subprocess.CalledProcessError:
                print(f"❌ Failed to kill screen session")
                return False
        else:
            print(f"Screen session {self.screen_session_name} not running")
            return True
    
    def show_logs(self, lines: int = 50):
        """Show recent log entries"""
        if os.path.exists(self.log_file):
            os.system(f"tail -{lines} {self.log_file}")
        else:
            print("❌ Log file not found")
    
    def get_status(self) -> Dict:
        """Get current download status"""
        downloader = EnhancedTCGMetadataDownloader()
        
        status = {
            'timestamp': datetime.now().isoformat(),
            'screen_session_running': self.check_screen_session(),
            'resume_status': downloader.get_resume_status(),
            'log_file': self.log_file,
            'log_exists': os.path.exists(self.log_file)
        }
        
        # Add BigQuery table info
        try:
            from bigquery_loader import BigQueryMetadataLoader
            loader = BigQueryMetadataLoader()
            table_info = loader.get_table_info()
            status['bigquery'] = table_info
        except Exception as e:
            status['bigquery'] = {'error': str(e)}
        
        return status
    
    def print_status(self):
        """Print comprehensive status"""
        status = self.get_status()
        
        print("=" * 70)
        print("📊 PRODUCTION DOWNLOAD STATUS")
        print("=" * 70)
        print(f"Time: {status['timestamp']}")
        print(f"Screen session: {'🟢 RUNNING' if status['screen_session_running'] else '🔴 STOPPED'}")
        print(f"Log file: {'✅ EXISTS' if status['log_exists'] else '❌ MISSING'}")
        print()
        
        # Resume status
        resume = status['resume_status']
        print("📋 Resume Status:")
        print(f"  Started: {resume['started_at']}")
        print(f"  Last updated: {resume['last_updated']}")
        print(f"  Completed categories: {resume['completed_categories']}")
        print(f"  Completed groups: {resume['completed_groups']:,}")
        print(f"  Failed groups: {resume['failed_groups']}")
        print(f"  Total products: {resume['total_products']:,}")
        
        if resume['current_category']:
            print(f"  Current position: Category {resume['current_category']}, Group {resume['current_group']}")
        
        # BigQuery status
        print()
        if 'error' not in status['bigquery']:
            bq = status['bigquery']
            print("💾 BigQuery Status:")
            print(f"  Table: {bq['table_name']}")
            print(f"  Rows: {bq['num_rows']:,}")
            print(f"  Size: {bq['size_mb']:.2f} MB")
            print(f"  Last modified: {bq['modified']}")
        else:
            print(f"💾 BigQuery Error: {status['bigquery']['error']}")
        
        print("=" * 70)

def run_full_download():
    """Run full download of all categories"""
    print("🚀 FULL DOWNLOAD MODE: All TCG Categories")
    print("=" * 60)
    print("⚠️  This will download ALL 89 categories")
    print("⚠️  Estimated time: 15-20 hours")
    print("⚠️  Rate limited to <1 request per second")
    print("⚠️  Supports full resume if interrupted")
    
    # Confirm with user
    response = input("\nProceed with full download? (y/N): ")
    if response.lower() != 'y':
        print("Download cancelled")
        return False
    
    try:
        downloader = EnhancedTCGMetadataDownloader(
            min_request_interval=1.2,
            batch_size=1000  # Larger batches for efficiency
        )
        
        print(f"\n🎬 Starting full download at {datetime.now()}")
        stats = downloader.download_all_categories()
        
        print(f"\n🎉 Full download completed!")
        print(f"📦 Total products: {stats['total_products']:,}")
        print(f"📁 Categories processed: {stats['categories_processed']}")
        
        return True
        
    except KeyboardInterrupt:
        print(f"\n⚠️  Download interrupted by user")
        print(f"📊 Progress saved in checkpoint file")
        return False
    except Exception as e:
        print(f"❌ Full download failed: {e}")
        return False

def run_category_download(category_ids: List[int]):
    """Download specific categories"""
    print(f"🎯 CATEGORY DOWNLOAD: {', '.join(map(str, category_ids))}")
    print("=" * 60)
    
    try:
        downloader = EnhancedTCGMetadataDownloader(
            min_request_interval=1.2,
            batch_size=500
        )
        
        print(f"\n🎬 Starting category download at {datetime.now()}")
        stats = downloader.download_all_categories(specific_categories=category_ids)
        
        print(f"\n✅ Category download completed!")
        print(f"📦 Total products: {stats['total_products']:,}")
        print(f"📁 Categories processed: {stats['categories_processed']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Category download failed: {e}")
        return False

def resume_download():
    """Resume interrupted download"""
    print("🔄 RESUME MODE: Continue from checkpoint")
    print("=" * 60)
    
    downloader = EnhancedTCGMetadataDownloader()
    
    # Show resume status
    downloader.print_resume_status()
    
    status = downloader.get_resume_status()
    if not status['can_resume']:
        print("❌ No checkpoint found - nothing to resume")
        print("Use 'full' mode to start a new download")
        return False
    
    # Confirm resume
    response = input(f"\nResume download from checkpoint? (y/N): ")
    if response.lower() != 'y':
        print("Resume cancelled")
        return False
    
    try:
        print(f"\n🔄 Resuming download at {datetime.now()}")
        stats = downloader.download_all_categories(skip_completed=True)
        
        print(f"\n🎉 Resume download completed!")
        print(f"📦 Total products: {stats['total_products']:,}")
        
        return True
        
    except Exception as e:
        print(f"❌ Resume failed: {e}")
        return False

def retry_failed():
    """Retry failed groups from checkpoint"""
    print("🔁 RETRY MODE: Retry failed groups")
    print("=" * 60)
    
    try:
        downloader = EnhancedTCGMetadataDownloader()
        recovered = downloader.retry_failed_groups()
        
        print(f"\n✅ Retry completed!")
        print(f"📦 Groups recovered: {recovered}")
        
        return True
        
    except Exception as e:
        print(f"❌ Retry failed: {e}")
        return False

def main():
    """Main command-line interface"""
    parser = argparse.ArgumentParser(
        description="Enhanced TCG Metadata Downloader with Resume Support",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 run_full_download.py full                    # Download all categories
  python3 run_full_download.py resume                  # Resume interrupted download
  python3 run_full_download.py category 1 2 3         # Download specific categories
  python3 run_full_download.py retry                   # Retry failed groups
  python3 run_full_download.py status                  # Show current status
  python3 run_full_download.py logs                    # Show recent logs
  python3 run_full_download.py screen --attach         # Attach to screen session
  python3 run_full_download.py screen --kill           # Kill screen session
        """
    )
    
    parser.add_argument('mode', choices=['full', 'resume', 'category', 'retry', 'status', 'logs', 'screen'],
                       help='Download mode')
    parser.add_argument('category_ids', nargs='*', type=int,
                       help='Category IDs for category mode')
    parser.add_argument('--attach', action='store_true',
                       help='Attach to screen session (for screen mode)')
    parser.add_argument('--kill', action='store_true', 
                       help='Kill screen session (for screen mode)')
    parser.add_argument('--lines', type=int, default=50,
                       help='Number of log lines to show (for logs mode)')
    parser.add_argument('--screen-name', type=str, default='tcg_full_download',
                       help='Screen session name')
    parser.add_argument('--project', type=str,
                       help='Google Cloud project ID')
    parser.add_argument('--dataset', type=str, default='tcg_data',
                       help='BigQuery dataset name')
    
    args = parser.parse_args()
    
    # Load environment variables
    load_dotenv()
    
    # Set global project if specified
    if args.project:
        os.environ['GOOGLE_CLOUD_PROJECT'] = args.project
    
    # Initialize runner
    runner = ProductionDownloadRunner(screen_session_name=args.screen_name)
    
    print("🃏 ENHANCED TCG METADATA DOWNLOADER")
    print("=" * 60)
    print(f"Mode: {args.mode}")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Route to appropriate function
    success = False
    
    if args.mode == 'full':
        success = run_full_download()
        
    elif args.mode == 'resume':
        success = resume_download()
        
    elif args.mode == 'category':
        if not args.category_ids:
            print("❌ Category mode requires category IDs")
            print("Example: python3 run_full_download.py category 1 2 3")
            return 1
        success = run_category_download(args.category_ids)
        
    elif args.mode == 'retry':
        success = retry_failed()
        
    elif args.mode == 'status':
        runner.print_status()
        success = True
        
    elif args.mode == 'logs':
        runner.show_logs(args.lines)
        success = True
        
    elif args.mode == 'screen':
        if args.attach:
            runner.attach_to_session()
            success = True
        elif args.kill:
            success = runner.kill_session()
        else:
            print("❌ Screen mode requires --attach or --kill")
            return 1
    
    # Exit with appropriate code
    if success:
        print(f"\n✅ {args.mode.title()} completed successfully")
        return 0
    else:
        print(f"\n❌ {args.mode.title()} failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())