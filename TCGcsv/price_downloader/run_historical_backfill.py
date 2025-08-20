#!/usr/bin/env python3
"""
Main execution script for historical price data backfill
Robust collection of TCG price data from 2024-02-08 to 2025-08-19
"""
import os
import sys
import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from price_logger_config import PriceLoggerConfig, DEFAULT_CONFIG, ROBUST_CONFIG, FAST_CONFIG
from robust_price_logger import RobustPriceLogger
from failure_analyzer import FailureAnalyzer

def print_banner():
    """Print application banner"""
    banner = """
    ╔═══════════════════════════════════════════════════════════════════════════╗
    ║                     🚀 ROBUST TCG PRICE LOGGER 🚀                        ║
    ║                                                                           ║
    ║                   Historical Price Data Backfill System                  ║
    ║                        2024-02-08 to 2025-08-19                         ║
    ║                                                                           ║
    ║  Features:                                                               ║
    ║  • Comprehensive error handling & retry mechanisms                       ║
    ║  • Resumable downloads with checkpointing                               ║
    ║  • Raw data preservation in organized structure                          ║
    ║  • Detailed failure analysis & recovery recommendations                  ║
    ║  • Progress monitoring & performance optimization                        ║
    ╚═══════════════════════════════════════════════════════════════════════════╝
    """
    print(banner)

def estimate_runtime(start_date: str, end_date: str, avg_time_per_date: float = 41.0):
    """Estimate total runtime for the backfill"""
    from datetime import datetime
    
    start = datetime.strptime(start_date, '%Y-%m-%d').date()
    end = datetime.strptime(end_date, '%Y-%m-%d').date()
    
    total_days = (end - start).days + 1
    total_seconds = total_days * avg_time_per_date
    total_hours = total_seconds / 3600
    
    return {
        'total_days': total_days,
        'total_seconds': total_seconds,
        'total_hours': total_hours,
        'estimated_completion': datetime.now() + timedelta(seconds=total_seconds)
    }

def check_prerequisites():
    """Check system prerequisites"""
    print("🔍 Checking prerequisites...")
    
    issues = []
    
    # Check Python packages
    required_packages = ['pandas', 'requests', 'tqdm']
    for package in required_packages:
        try:
            __import__(package)
            print(f"  ✓ {package}")
        except ImportError:
            issues.append(f"Missing Python package: {package}")
            print(f"  ✗ {package}")
    
    # Check BigQuery package separately
    try:
        import google.cloud.bigquery
        print(f"  ✓ google-cloud-bigquery")
    except ImportError:
        issues.append(f"Missing Python package: google-cloud-bigquery")
        print(f"  ✗ google-cloud-bigquery")
    
    # Check 7z availability
    import subprocess
    try:
        result = subprocess.run(['7z'], capture_output=True, text=True)
        print(f"  ✓ 7z")
    except FileNotFoundError:
        issues.append("7z command not found - required for archive extraction")
        print(f"  ✗ 7z")
    
    # Check environment variables
    required_env_vars = ['GOOGLE_APPLICATION_CREDENTIALS', 'GOOGLE_CLOUD_PROJECT']
    for var in required_env_vars:
        if os.getenv(var):
            print(f"  ✓ {var}")
        else:
            issues.append(f"Missing environment variable: {var}")
            print(f"  ✗ {var}")
    
    # Check disk space (reduced requirement since we auto-cleanup CSV files)
    try:
        import shutil
        total, used, free = shutil.disk_usage('.')
        free_gb = free / (1024**3)
        if free_gb < 10:
            issues.append(f"Low disk space: {free_gb:.1f}GB available, ~10GB minimum required")
            print(f"  ✗ Disk space: {free_gb:.1f}GB available (10GB+ required)")
        elif free_gb < 20:
            print(f"  ⚠️  Disk space: {free_gb:.1f}GB available (20GB+ recommended)")
        else:
            print(f"  ✓ Disk space: {free_gb:.1f}GB available")
    except Exception as e:
        print(f"  ⚠️  Could not check disk space: {e}")
    
    if issues:
        print(f"\n❌ Prerequisites check failed:")
        for issue in issues:
            print(f"   • {issue}")
        return False
    else:
        print(f"✅ All prerequisites satisfied")
        return True

def interactive_configuration():
    """Interactive configuration setup"""
    print("\n🔧 Configuration Setup")
    print("=" * 50)
    
    # Choose configuration preset
    print("Select configuration preset:")
    print("1. ROBUST (Recommended) - Maximum reliability, slower but safer")
    print("2. FAST - Higher performance, moderate reliability")
    print("3. CUSTOM - Custom configuration")
    
    while True:
        try:
            choice = input("Enter choice (1-3) [1]: ").strip() or "1"
            if choice in ["1", "2", "3"]:
                break
            else:
                print("Invalid choice. Please enter 1, 2, or 3.")
        except KeyboardInterrupt:
            print("\n\nOperation cancelled.")
            sys.exit(0)
    
    if choice == "1":
        config = ROBUST_CONFIG
        print("✓ Selected ROBUST configuration")
    elif choice == "2":
        config = FAST_CONFIG
        print("✓ Selected FAST configuration")
    else:
        config = create_custom_config()
    
    # Date range configuration
    print(f"\nCurrent date range: {config.date_range.start_date} to {config.date_range.end_date}")
    
    modify_dates = input("Modify date range? (y/N): ").lower().strip() == 'y'
    if modify_dates:
        start_date = input(f"Start date [{config.date_range.start_date}]: ").strip() or config.date_range.start_date
        end_date = input(f"End date [{config.date_range.end_date}]: ").strip() or config.date_range.end_date
        
        # Update configuration
        config.date_range.start_date = start_date
        config.date_range.end_date = end_date
    
    return config

def create_custom_config():
    """Create custom configuration"""
    print("\n🛠️  Custom Configuration")
    
    overrides = {}
    
    # Performance settings
    max_workers = input("Max concurrent workers [5]: ").strip() or "5"
    try:
        overrides["performance"] = {"max_workers": int(max_workers)}
    except ValueError:
        print("Invalid number, using default (5)")
    
    # Retry settings
    max_retries = input("Max retries per operation [5]: ").strip() or "5"
    try:
        if "retry" not in overrides:
            overrides["retry"] = {}
        overrides["retry"]["max_retries"] = int(max_retries)
    except ValueError:
        print("Invalid number, using default (5)")
    
    return PriceLoggerConfig(overrides)

def show_runtime_estimate(config):
    """Show runtime estimation"""
    print(f"\n⏱️  Runtime Estimation")
    print("=" * 50)
    
    estimate = estimate_runtime(config.date_range.start_date, config.date_range.end_date)
    
    print(f"📅 Date range: {config.date_range.start_date} to {config.date_range.end_date}")
    print(f"📊 Total dates: {estimate['total_days']}")
    print(f"⏳ Estimated time: {estimate['total_hours']:.1f} hours ({estimate['total_hours']/24:.1f} days)")
    print(f"🏁 Est. completion: {estimate['estimated_completion'].strftime('%Y-%m-%d %H:%M:%S')}")
    
    print(f"\n💾 Expected data volume:")
    print(f"   • Raw archives: ~{estimate['total_days'] * 3:.0f} MB")
    print(f"   • Processed CSV: ~{estimate['total_days'] * 30:.0f} MB")
    print(f"   • Total storage: ~{estimate['total_days'] * 35:.0f} MB")

def confirm_execution(config):
    """Get user confirmation before starting"""
    print(f"\n🔍 Final Configuration Summary")
    print("=" * 50)
    
    print(f"📁 Base directory: {config.directories.base_dir}")
    print(f"📅 Date range: {config.date_range.start_date} to {config.date_range.end_date}")
    print(f"⚙️  Max workers: {config.performance.max_workers}")
    print(f"🔄 Max retries: {config.retry.max_retries}")
    print(f"💾 Checkpoint interval: {config.performance.checkpoint_interval} dates")
    
    print(f"\n⚠️  Important Notes:")
    print(f"   • This will download data from tcgcsv.com")
    print(f"   • Raw archives will be preserved in {config.directories.raw_archives_path}")
    print(f"   • Progress can be monitored in {config.directories.logs_path}")
    print(f"   • Process can be safely interrupted and resumed")
    
    while True:
        try:
            confirm = input("\n🚀 Start historical backfill? (y/N): ").lower().strip()
            if confirm in ['y', 'yes']:
                return True
            elif confirm in ['n', 'no', '']:
                return False
            else:
                print("Please enter 'y' or 'n'")
        except KeyboardInterrupt:
            print("\n\nOperation cancelled.")
            return False

def monitor_progress(logger):
    """Monitor and display progress"""
    checkpoint_data = logger.checkpoint_manager.checkpoint_data
    
    print(f"\n📊 Current Progress:")
    print(f"   Completed: {checkpoint_data['statistics']['completed_dates']}")
    print(f"   Failed: {checkpoint_data['statistics']['failed_dates']}")
    print(f"   Total records: {checkpoint_data['total_records_processed']:,}")
    
    if checkpoint_data['statistics']['completed_dates'] > 0:
        avg_time = checkpoint_data['statistics']['average_time_per_date']
        print(f"   Average time per date: {avg_time:.1f}s")

def run_backfill(config, resume_run_id=None):
    """Run the historical backfill"""
    print(f"\n🚀 Starting Historical Backfill")
    print("=" * 50)
    
    # Initialize logger
    logger = RobustPriceLogger(config=config, run_id=resume_run_id)
    
    print(f"📝 Run ID: {logger.run_id}")
    print(f"📂 Logs: {config.directories.logs_path}")
    print(f"💾 Checkpoints: {config.directories.checkpoints_path}")
    
    # Show initial progress
    monitor_progress(logger)
    
    try:
        # Run the backfill
        final_report = logger.run_historical_backfill(
            config.date_range.start_date,
            config.date_range.end_date
        )
        
        # Display results
        print(f"\n🎉 Historical Backfill Completed!")
        print("=" * 50)
        
        summary = final_report['summary']
        print(f"✅ Success rate: {summary['success_rate']:.1f}%")
        print(f"📊 Total dates: {summary['total_dates']}")
        print(f"✓ Completed: {summary['completed_dates']}")
        print(f"✗ Failed: {summary['failed_dates']}")
        print(f"📦 Total records: {summary['total_records']:,}")
        print(f"⏱️  Total time: {summary['total_processing_time']/3600:.1f} hours")
        
        if summary['failed_dates'] > 0:
            print(f"\n⚠️  {summary['failed_dates']} dates failed:")
            print(f"   Failed dates: {final_report['failed_dates']}")
            
            # Generate recovery report
            print(f"\n📋 Generating recovery report...")
            recovery_report_path = logger.generate_recovery_report()
            print(f"   Recovery report: {recovery_report_path}")
            
            # Ask about retry
            retry = input("\n🔄 Retry failed dates? (y/N): ").lower().strip() == 'y'
            if retry:
                print(f"🔄 Retrying failed dates...")
                retry_results = logger.retry_failed_dates()
                print(f"   Recovered: {retry_results['recovered_dates']}")
                print(f"   Still failing: {retry_results['still_failing']}")
        
        return True
        
    except KeyboardInterrupt:
        print(f"\n\n⏸️  Process interrupted by user")
        print(f"📝 Progress saved to checkpoint: {logger.checkpoint_manager.checkpoint_file}")
        print(f"🔄 Resume with: python3 run_historical_backfill.py --resume {logger.run_id}")
        return False
        
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        print(f"📝 Progress saved to checkpoint: {logger.checkpoint_manager.checkpoint_file}")
        print(f"📋 Check logs: {config.directories.logs_path}")
        return False

def resume_backfill(run_id):
    """Resume a previous backfill run"""
    print(f"🔄 Resuming backfill run: {run_id}")
    
    # Try to find existing checkpoint
    checkpoint_file = DEFAULT_CONFIG.get_checkpoint_filename(run_id)
    
    if not os.path.exists(checkpoint_file):
        print(f"❌ Checkpoint file not found: {checkpoint_file}")
        return False
    
    try:
        with open(checkpoint_file, 'r') as f:
            checkpoint_data = json.load(f)
        
        print(f"📊 Found checkpoint:")
        print(f"   Completed dates: {len(checkpoint_data.get('completed_dates', []))}")
        print(f"   Failed dates: {len(checkpoint_data.get('failed_dates', []))}")
        print(f"   Total records: {checkpoint_data.get('total_records_processed', 0):,}")
        
        # Use ROBUST config for resume (safer)
        return run_backfill(ROBUST_CONFIG, resume_run_id=run_id)
        
    except Exception as e:
        print(f"❌ Error loading checkpoint: {e}")
        return False

def list_checkpoints():
    """List available checkpoints"""
    checkpoints_dir = DEFAULT_CONFIG.directories.checkpoints_path
    
    if not os.path.exists(checkpoints_dir):
        print("No checkpoints found")
        return
    
    checkpoint_files = [f for f in os.listdir(checkpoints_dir) if f.startswith('checkpoint_')]
    
    if not checkpoint_files:
        print("No checkpoints found")
        return
    
    print("Available checkpoints:")
    print("=" * 50)
    
    for filename in sorted(checkpoint_files):
        filepath = os.path.join(checkpoints_dir, filename)
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            run_id = data.get('run_id', 'unknown')
            started = data.get('started_at', 'unknown')
            completed = len(data.get('completed_dates', []))
            failed = len(data.get('failed_dates', []))
            
            print(f"📝 Run ID: {run_id}")
            print(f"   Started: {started}")
            print(f"   Progress: {completed} completed, {failed} failed")
            print()
            
        except Exception as e:
            print(f"❌ Error reading {filename}: {e}")

def main():
    """Main application entry point"""
    parser = argparse.ArgumentParser(description='Robust TCG Price Logger - Historical Backfill')
    parser.add_argument('--resume', metavar='RUN_ID', help='Resume a previous run')
    parser.add_argument('--list-checkpoints', action='store_true', help='List available checkpoints')
    parser.add_argument('--test', action='store_true', help='Run test suite first')
    parser.add_argument('--config', choices=['robust', 'fast', 'custom'], default='robust',
                       help='Configuration preset to use')
    parser.add_argument('--start-date', help='Override start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='Override end date (YYYY-MM-DD)')
    parser.add_argument('--non-interactive', action='store_true', help='Run without prompts')
    
    args = parser.parse_args()
    
    # Handle special commands
    if args.list_checkpoints:
        list_checkpoints()
        return
    
    if args.resume:
        success = resume_backfill(args.resume)
        sys.exit(0 if success else 1)
    
    # Main execution flow
    print_banner()
    
    # Run tests if requested
    if args.test:
        print("🧪 Running test suite first...")
        from test_robust_logger import run_all_tests
        if not run_all_tests():
            print("❌ Tests failed. Fix issues before running backfill.")
            sys.exit(1)
        print("✅ Tests passed! Continuing with backfill...\n")
    
    # Check prerequisites
    if not check_prerequisites():
        print("\n❌ Prerequisites check failed. Please resolve issues and try again.")
        sys.exit(1)
    
    # Configuration
    if args.non_interactive:
        # Use preset configuration
        if args.config == 'robust':
            config = ROBUST_CONFIG
        elif args.config == 'fast':
            config = FAST_CONFIG
        else:
            config = DEFAULT_CONFIG
        
        # Apply date overrides
        if args.start_date:
            config.date_range.start_date = args.start_date
        if args.end_date:
            config.date_range.end_date = args.end_date
            
        print(f"✓ Using {args.config.upper()} configuration (non-interactive mode)")
        
    else:
        # Interactive configuration
        config = interactive_configuration()
    
    # Show estimates and confirm
    show_runtime_estimate(config)
    
    if not args.non_interactive:
        if not confirm_execution(config):
            print("Operation cancelled.")
            sys.exit(0)
    
    # Run backfill
    success = run_backfill(config)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()