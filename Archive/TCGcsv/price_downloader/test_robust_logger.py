#!/usr/bin/env python3
"""
Test script for the robust price logger
Validates functionality with a small date range before running the full historical backfill
"""
import os
import sys
import json
from datetime import datetime, timedelta
from pathlib import Path

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from price_logger_config import PriceLoggerConfig, TEST_CONFIG, ROBUST_CONFIG
from robust_price_logger import RobustPriceLogger
from failure_analyzer import FailureAnalyzer

def test_basic_functionality():
    """Test basic configuration and setup"""
    print("=== Testing Basic Functionality ===")
    
    # Test configuration
    config = TEST_CONFIG
    print(f"✓ Configuration loaded")
    print(f"  Date range: {config.date_range.start_date} to {config.date_range.end_date}")
    print(f"  Max workers: {config.performance.max_workers}")
    print(f"  Base directory: {config.directories.base_dir}")
    
    # Test directory creation
    config._ensure_directories()
    required_dirs = [
        config.directories.base_dir,
        config.directories.raw_archives_path,
        config.directories.extracted_path,
        config.directories.processed_path,
        config.directories.logs_path,
        config.directories.checkpoints_path,
        config.directories.failures_path
    ]
    
    for directory in required_dirs:
        if os.path.exists(directory):
            print(f"✓ Directory exists: {directory}")
        else:
            print(f"✗ Directory missing: {directory}")
            return False
    
    # Test logger initialization
    try:
        logger = RobustPriceLogger(config=config, run_id="test_run")
        print(f"✓ Logger initialized with run_id: {logger.run_id}")
    except Exception as e:
        print(f"✗ Logger initialization failed: {e}")
        return False
    
    print("✅ Basic functionality test passed\n")
    return True

def test_date_range_generation():
    """Test date range generation"""
    print("=== Testing Date Range Generation ===")
    
    config = TEST_CONFIG
    logger = RobustPriceLogger(config=config, run_id="test_dates")
    
    # Test date range generation
    dates = logger.generate_date_range()
    print(f"✓ Generated {len(dates)} dates")
    print(f"  First date: {dates[0] if dates else 'None'}")
    print(f"  Last date: {dates[-1] if dates else 'None'}")
    
    # Test custom date range
    custom_dates = logger.generate_date_range("2024-12-01", "2024-12-03")
    expected_dates = ["2024-12-01", "2024-12-02", "2024-12-03"]
    
    if custom_dates == expected_dates:
        print(f"✓ Custom date range correct: {custom_dates}")
    else:
        print(f"✗ Custom date range incorrect: {custom_dates} != {expected_dates}")
        return False
    
    print("✅ Date range generation test passed\n")
    return True

def test_checkpoint_functionality():
    """Test checkpoint save/load functionality"""
    print("=== Testing Checkpoint Functionality ===")
    
    config = TEST_CONFIG
    logger = RobustPriceLogger(config=config, run_id="test_checkpoint")
    
    # Test initial checkpoint
    checkpoint_file = logger.checkpoint_manager.checkpoint_file
    print(f"✓ Checkpoint file: {checkpoint_file}")
    
    # Test marking a date as completed
    test_date = "2024-12-01"
    logger.checkpoint_manager.mark_date_completed(test_date, 1000, 45.5)
    
    # Verify checkpoint data
    checkpoint_data = logger.checkpoint_manager.checkpoint_data
    if test_date in checkpoint_data['completed_dates']:
        print(f"✓ Date marked as completed: {test_date}")
    else:
        print(f"✗ Date not marked as completed: {test_date}")
        return False
    
    if checkpoint_data['total_records_processed'] == 1000:
        print(f"✓ Record count updated: {checkpoint_data['total_records_processed']}")
    else:
        print(f"✗ Record count incorrect: {checkpoint_data['total_records_processed']}")
        return False
    
    # Test checkpoint persistence
    logger.checkpoint_manager.save_checkpoint()
    
    # Load new instance and verify data
    logger2 = RobustPriceLogger(config=config, run_id="test_checkpoint")
    if test_date in logger2.checkpoint_manager.checkpoint_data['completed_dates']:
        print(f"✓ Checkpoint data persisted correctly")
    else:
        print(f"✗ Checkpoint data not persisted")
        return False
    
    print("✅ Checkpoint functionality test passed\n")
    return True

def test_failure_analyzer():
    """Test failure analysis functionality"""
    print("=== Testing Failure Analyzer ===")
    
    config = TEST_CONFIG
    analyzer = FailureAnalyzer(config)
    
    # Test failure recording
    test_error = Exception("Test network error")
    failure = analyzer.record_failure("2024-12-01", "download", test_error, retry_count=2)
    
    print(f"✓ Failure recorded: {failure.date} - {failure.failure_type}")
    
    # Test failure file creation
    failure_file = config.get_failure_log_filename("2024-12-01")
    if os.path.exists(failure_file):
        print(f"✓ Failure file created: {failure_file}")
        
        # Verify file content
        with open(failure_file, 'r') as f:
            failure_data = json.load(f)
        
        if len(failure_data) > 0 and failure_data[0]['date'] == "2024-12-01":
            print(f"✓ Failure data saved correctly")
        else:
            print(f"✗ Failure data incorrect")
            return False
    else:
        print(f"✗ Failure file not created")
        return False
    
    # Test pattern analysis
    patterns = analyzer.analyze_patterns()
    print(f"✓ Pattern analysis completed: {len(patterns)} patterns found")
    
    print("✅ Failure analyzer test passed\n")
    return True

def test_single_date_processing():
    """Test processing a single date end-to-end"""
    print("=== Testing Single Date Processing ===")
    
    # Use a recent date that should have data
    test_date = "2024-12-01"
    
    config = TEST_CONFIG
    logger = RobustPriceLogger(config=config, run_id="test_single_date")
    
    print(f"Testing single date processing for {test_date}")
    print("This will attempt to download, extract, process, and upload real data")
    print("(This test requires network connectivity and may take several minutes)")
    
    # Ask for confirmation
    confirm = input("Proceed with single date test? (y/N): ").lower().strip()
    if confirm != 'y':
        print("⚠️  Single date test skipped")
        return True
    
    try:
        result = logger.process_single_date(test_date)
        
        if result['success']:
            print(f"✅ Single date processing successful!")
            print(f"  Records processed: {result['record_count']:,}")
            print(f"  Processing time: {result['processing_time']:.2f}s")
            print(f"  Steps completed: {result['steps_completed']}")
        else:
            print(f"❌ Single date processing failed: {result['error']}")
            print(f"  Steps completed: {result['steps_completed']}")
            return False
            
    except Exception as e:
        print(f"✗ Single date processing exception: {e}")
        return False
    
    print("✅ Single date processing test passed\n")
    return True

def test_recovery_functionality():
    """Test recovery and retry functionality"""
    print("=== Testing Recovery Functionality ===")
    
    config = TEST_CONFIG
    logger = RobustPriceLogger(config=config, run_id="test_recovery")
    
    # Simulate some failures
    analyzer = logger.failure_analyzer
    
    # Add some test failures
    failures = [
        ("2024-12-01", "download", Exception("Connection timeout")),
        ("2024-12-02", "extract", Exception("7z error: corrupt archive")),
        ("2024-12-03", "download", Exception("HTTP 404 Not Found")),
        ("2024-12-04", "upload", Exception("BigQuery quota exceeded")),
    ]
    
    for date, failure_type, error in failures:
        analyzer.record_failure(date, failure_type, error)
    
    print(f"✓ Added {len(failures)} test failures")
    
    # Test pattern analysis
    patterns = analyzer.analyze_patterns()
    print(f"✓ Found {len(patterns)} failure patterns")
    
    # Test recovery report generation
    try:
        report_path = analyzer.save_recovery_report("test_recovery_report.json")
        print(f"✓ Recovery report saved: {report_path}")
        
        # Verify report content
        with open(report_path, 'r') as f:
            report = json.load(f)
        
        if 'summary' in report and 'patterns' in report:
            print(f"✓ Recovery report format correct")
            print(f"  Total failures: {report['summary']['total_failures']}")
            print(f"  Patterns found: {len(report['patterns'])}")
        else:
            print(f"✗ Recovery report format incorrect")
            return False
            
    except Exception as e:
        print(f"✗ Recovery report generation failed: {e}")
        return False
    
    print("✅ Recovery functionality test passed\n")
    return True

def test_configuration_presets():
    """Test different configuration presets"""
    print("=== Testing Configuration Presets ===")
    
    from price_logger_config import FAST_CONFIG, ROBUST_CONFIG
    
    configs = {
        "TEST": TEST_CONFIG,
        "FAST": FAST_CONFIG,
        "ROBUST": ROBUST_CONFIG
    }
    
    for name, config in configs.items():
        print(f"✓ {name} config loaded:")
        print(f"  Max workers: {config.performance.max_workers}")
        print(f"  Max retries: {config.retry.max_retries}")
        print(f"  Checkpoint interval: {config.performance.checkpoint_interval}")
        
        # Test logger initialization with each config
        try:
            logger = RobustPriceLogger(config=config, run_id=f"test_{name.lower()}")
            print(f"  ✓ Logger initialized successfully")
        except Exception as e:
            print(f"  ✗ Logger initialization failed: {e}")
            return False
    
    print("✅ Configuration presets test passed\n")
    return True

def run_all_tests():
    """Run all test functions"""
    print("🚀 Starting Robust Price Logger Test Suite")
    print("=" * 60)
    
    tests = [
        ("Basic Functionality", test_basic_functionality),
        ("Date Range Generation", test_date_range_generation),
        ("Checkpoint Functionality", test_checkpoint_functionality),
        ("Failure Analyzer", test_failure_analyzer),
        ("Configuration Presets", test_configuration_presets),
        ("Recovery Functionality", test_recovery_functionality),
        ("Single Date Processing", test_single_date_processing)  # This one last as it's most intensive
    ]
    
    passed_tests = 0
    total_tests = len(tests)
    
    for test_name, test_func in tests:
        print(f"🧪 Running test: {test_name}")
        try:
            if test_func():
                passed_tests += 1
            else:
                print(f"❌ Test failed: {test_name}")
                break  # Stop on first failure for easier debugging
        except Exception as e:
            print(f"❌ Test exception in {test_name}: {e}")
            break
    
    print("=" * 60)
    if passed_tests == total_tests:
        print(f"🎉 All tests passed! ({passed_tests}/{total_tests})")
        print("\n✅ The robust price logger is ready for historical backfill!")
        return True
    else:
        print(f"❌ Tests failed: {passed_tests}/{total_tests} passed")
        print("\n⚠️  Please fix issues before running historical backfill")
        return False

def main():
    """Main test execution"""
    if len(sys.argv) > 1:
        if sys.argv[1] == "--single-date":
            test_single_date_processing()
        elif sys.argv[1] == "--basic":
            test_basic_functionality()
            test_date_range_generation()
            test_checkpoint_functionality()
        elif sys.argv[1] == "--help":
            print("Usage:")
            print("  python3 test_robust_logger.py           # Run all tests")
            print("  python3 test_robust_logger.py --basic   # Run basic tests only")
            print("  python3 test_robust_logger.py --single-date # Test single date processing")
    else:
        success = run_all_tests()
        sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()