#!/usr/bin/env python3
"""
Configuration module for the robust price logger
Centralized configuration management for retry policies, timeouts, and directory structures
"""
import os
from datetime import datetime, timedelta
from typing import Dict, Any
from dataclasses import dataclass

@dataclass
class RetryConfig:
    """Configuration for retry mechanisms"""
    max_retries: int = 5
    base_delay: float = 2.0
    max_delay: float = 300.0  # 5 minutes max
    backoff_factor: float = 2.0
    jitter_range: tuple = (0.1, 0.5)  # Jitter as fraction of delay

@dataclass
class PerformanceConfig:
    """Configuration for performance settings"""
    max_workers: int = 5  # Optimal based on testing
    chunk_size: int = 10000  # Records per chunk for CSV writing
    checkpoint_interval: int = 10  # Save checkpoint every N dates
    memory_limit_mb: int = 2048  # Memory usage limit
    cleanup_temp_files: bool = True  # Clean up temporary files after processing

@dataclass  
class DirectoryConfig:
    """Configuration for directory structure"""
    base_dir: str = "pricedata"
    raw_archives_dir: str = "raw_archives"
    extracted_dir: str = "extracted" 
    processed_dir: str = "processed"
    logs_dir: str = "logs"
    checkpoints_dir: str = "checkpoints"
    failures_dir: str = "failures"
    
    def get_full_path(self, subdir: str) -> str:
        """Get full path for a subdirectory"""
        return os.path.join(self.base_dir, subdir)
    
    @property
    def raw_archives_path(self) -> str:
        return self.get_full_path(self.raw_archives_dir)
    
    @property
    def extracted_path(self) -> str:
        return self.get_full_path(self.extracted_dir)
    
    @property
    def processed_path(self) -> str:
        return self.get_full_path(self.processed_dir)
    
    @property
    def logs_path(self) -> str:
        return self.get_full_path(self.logs_dir)
    
    @property
    def checkpoints_path(self) -> str:
        return self.get_full_path(self.checkpoints_dir)
    
    @property
    def failures_path(self) -> str:
        return self.get_full_path(self.failures_dir)

@dataclass
class LoggingConfig:
    """Configuration for logging"""
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    max_log_size_mb: int = 100
    backup_count: int = 5
    console_logging: bool = True
    file_logging: bool = True

@dataclass
class DateRangeConfig:
    """Configuration for date range processing"""
    start_date: str = "2024-02-08"
    end_date: str = "2025-08-19"  # Today
    batch_size: int = 30  # Process N dates before checkpoint
    skip_weekends: bool = False  # Skip weekends (markets closed)
    skip_holidays: bool = False  # Skip known holidays

@dataclass
class ValidationConfig:
    """Configuration for data validation"""
    min_records_per_date: int = 100000  # Minimum expected records
    max_records_per_date: int = 1000000  # Maximum expected records
    required_columns: list = None
    validate_price_ranges: bool = True
    min_price: float = 0.01
    max_price: float = 100000.0
    
    def __post_init__(self):
        if self.required_columns is None:
            self.required_columns = [
                'price_date', 'product_id', 'sub_type_name',
                'market_price', 'category_id', 'group_id'
            ]

class PriceLoggerConfig:
    """Main configuration class for the robust price logger"""
    
    def __init__(self, config_overrides: Dict[str, Any] = None):
        self.retry = RetryConfig()
        self.performance = PerformanceConfig()
        self.directories = DirectoryConfig()
        self.logging = LoggingConfig()
        self.date_range = DateRangeConfig()
        self.validation = ValidationConfig()
        
        # Apply any configuration overrides
        if config_overrides:
            self._apply_overrides(config_overrides)
        
        # Ensure directories exist
        self._ensure_directories()
    
    def _apply_overrides(self, overrides: Dict[str, Any]):
        """Apply configuration overrides"""
        for section, values in overrides.items():
            if hasattr(self, section):
                section_config = getattr(self, section)
                for key, value in values.items():
                    if hasattr(section_config, key):
                        setattr(section_config, key, value)
                    else:
                        print(f"Warning: Unknown config key {section}.{key}")
            else:
                print(f"Warning: Unknown config section {section}")
    
    def _ensure_directories(self):
        """Ensure all configured directories exist"""
        directories = [
            self.directories.base_dir,
            self.directories.raw_archives_path,
            self.directories.extracted_path,
            self.directories.processed_path,
            self.directories.logs_path,
            self.directories.checkpoints_path,
            self.directories.failures_path
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
    
    def get_log_filename(self, logger_name: str = "price_logger") -> str:
        """Get log filename with timestamp"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return os.path.join(
            self.directories.logs_path,
            f"{logger_name}_{timestamp}.log"
        )
    
    def get_checkpoint_filename(self, run_id: str = None) -> str:
        """Get checkpoint filename"""
        if run_id is None:
            run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        return os.path.join(
            self.directories.checkpoints_path,
            f"checkpoint_{run_id}.json"
        )
    
    def get_failure_log_filename(self, date_str: str) -> str:
        """Get failure log filename for a specific date"""
        return os.path.join(
            self.directories.failures_path,
            f"failures_{date_str}.json"
        )
    
    def get_raw_archive_path(self, date_str: str) -> str:
        """Get path for raw archive file"""
        return os.path.join(
            self.directories.raw_archives_path,
            f"prices-{date_str}.ppmd.7z"
        )
    
    def get_extracted_path(self, date_str: str) -> str:
        """Get path for extracted data"""
        return os.path.join(
            self.directories.extracted_path,
            date_str
        )
    
    def get_processed_csv_path(self, date_str: str) -> str:
        """Get path for processed CSV file"""
        return os.path.join(
            self.directories.processed_path,
            f"tcg_prices_{date_str}.csv"
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary for serialization"""
        return {
            "retry": {
                "max_retries": self.retry.max_retries,
                "base_delay": self.retry.base_delay,
                "max_delay": self.retry.max_delay,
                "backoff_factor": self.retry.backoff_factor,
                "jitter_range": self.retry.jitter_range
            },
            "performance": {
                "max_workers": self.performance.max_workers,
                "chunk_size": self.performance.chunk_size,
                "checkpoint_interval": self.performance.checkpoint_interval,
                "memory_limit_mb": self.performance.memory_limit_mb,
                "cleanup_temp_files": self.performance.cleanup_temp_files
            },
            "directories": {
                "base_dir": self.directories.base_dir,
                "raw_archives_dir": self.directories.raw_archives_dir,
                "extracted_dir": self.directories.extracted_dir,
                "processed_dir": self.directories.processed_dir,
                "logs_dir": self.directories.logs_dir,
                "checkpoints_dir": self.directories.checkpoints_dir,
                "failures_dir": self.directories.failures_dir
            },
            "logging": {
                "log_level": self.logging.log_level,
                "log_format": self.logging.log_format,
                "max_log_size_mb": self.logging.max_log_size_mb,
                "backup_count": self.logging.backup_count,
                "console_logging": self.logging.console_logging,
                "file_logging": self.logging.file_logging
            },
            "date_range": {
                "start_date": self.date_range.start_date,
                "end_date": self.date_range.end_date,
                "batch_size": self.date_range.batch_size,
                "skip_weekends": self.date_range.skip_weekends,
                "skip_holidays": self.date_range.skip_holidays
            },
            "validation": {
                "min_records_per_date": self.validation.min_records_per_date,
                "max_records_per_date": self.validation.max_records_per_date,
                "required_columns": self.validation.required_columns,
                "validate_price_ranges": self.validation.validate_price_ranges,
                "min_price": self.validation.min_price,
                "max_price": self.validation.max_price
            }
        }

# Default configuration instance
DEFAULT_CONFIG = PriceLoggerConfig()

# Configuration presets for different scenarios
FAST_CONFIG = PriceLoggerConfig({
    "performance": {
        "max_workers": 10,
        "checkpoint_interval": 5
    },
    "retry": {
        "max_retries": 3,
        "max_delay": 60
    }
})

ROBUST_CONFIG = PriceLoggerConfig({
    "performance": {
        "max_workers": 5,
        "checkpoint_interval": 1  # Checkpoint after every date
    },
    "retry": {
        "max_retries": 10,
        "max_delay": 600  # 10 minutes max delay
    }
})

TEST_CONFIG = PriceLoggerConfig({
    "date_range": {
        "start_date": "2024-12-01",
        "end_date": "2024-12-03"
    },
    "performance": {
        "max_workers": 2,
        "checkpoint_interval": 1
    },
    "retry": {
        "max_retries": 2,
        "max_delay": 30
    }
})