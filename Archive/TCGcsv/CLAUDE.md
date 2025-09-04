# TCGcsv Data Downloader and BigQuery Importer

Download trading card data from TCGcsv.com and load into BigQuery. Supports both product catalog data and historical price data with automatic retry, resumable downloads, and deduplication.

## Project Structure
```
TCGcsv/
├── requirements.txt             # Python dependencies (includes pyarrow)
├── .env                        # Environment variables
├── service-account.json         # GCP credentials
├── downloader.py               # Download and denormalize TCG product data
├── bigquery_loader.py          # Load product catalog to BigQuery
├── resumable_downloader.py     # Resumable download with retry and checkpoints
├── run_in_screen.sh            # Screen session management for background runs
├── price_downloader.py         # Download historical price archives  
├── bigquery_price_loader.py    # Load price data to BigQuery
├── main.py                     # Main pipeline (products or prices)
├── main_price.py              # Price-specific pipeline with advanced options
├── download_progress.json      # Download progress checkpoint data
├── tcg_download.log           # Download logs
└── data/
    ├── tcg_products_full.csv   # Generated product catalog data
    ├── tcg_products_full_resumable.csv # Resumable download output
    ├── tcg_chunk_*.csv        # Temporary chunk files during download
    ├── tcg_prices_YYYY-MM-DD.csv # Generated price data by date
    └── archive/                 # Temporary storage for price archives
        └── prices/              # Extracted price data
```

## Quick Start

1. Install dependencies: `pip install -r requirements.txt`
2. Set credentials: `export GOOGLE_APPLICATION_CREDENTIALS=service-account.json`
3. Run the pipelines:
   - Products: `python3 main.py products`
   - Prices (test): `python3 main.py prices`

## Features

### Product Catalog Pipeline
- **Single Denormalized Table**: All categories, groups, and products in one BigQuery table
- **Bi-weekly Updates**: `update_date` column for tracking data refreshes
- **Automatic Schema**: BigQuery schema auto-detected with proper data types
- **Resumable Downloads**: Checkpoint-based recovery with `download_progress.json`
- **Automatic Retry**: Exponential backoff retry with jitter for network resilience
- **Multithreading**: Configurable worker threads for concurrent downloads
- **Deduplication**: Built-in BigQuery deduplication to handle multiple runs

### Price Data Pipeline  
- **Daily Price Archives**: Download compressed price archives from TCGcsv
- **Partitioned Storage**: BigQuery table partitioned by date for efficient queries
- **Historical Backfill**: Support for loading historical price data since Feb 2024
- **Incremental Updates**: Daily price updates with deduplication

## Data Architecture

### Table: `tcg_data.tcg_products` (Product Catalog)
- **Category fields**: `category_categoryId`, `category_name`, etc.
- **Group fields**: `group_groupId`, `group_name`, etc.  
- **Product fields**: `product_productId`, `product_name`, etc.
- **Update tracking**: `update_date` (DATE)

### Table: `tcg_data.tcg_prices` (Daily Prices)
- **Core fields**: `price_date`, `product_id`, `sub_type_name`
- **Price fields**: `low_price`, `mid_price`, `high_price`, `market_price`, `direct_low_price`
- **References**: `category_id`, `group_id` 
- **Metadata**: `update_timestamp`
- **Partitioning**: Daily partitions by `price_date`
- **Clustering**: Clustered by `product_id` for fast lookups

## Usage

### Product Catalog
```bash
# Test data (2 categories, few groups)
python3 main.py products

# Full scale resumable download (all ~480K products)
python3 resumable_downloader.py

# Run in background with screen (recommended for full scale)
./run_in_screen.sh start
./run_in_screen.sh status
./run_in_screen.sh logs
```

### Price Data
```bash
# Test with 1 day (limited data)
python3 main.py prices

# Daily price update (yesterday)  
python3 main_price.py daily

# Daily price for specific date
python3 main_price.py daily 2024-12-01

# Historical backfill (Feb 8, 2024 onwards)
python3 main_price.py backfill 2024-02-08 2024-12-01
```

## Resumable Downloads & Reliability

### Checkpoint System
The resumable downloader (`resumable_downloader.py`) provides robust checkpoint-based recovery:

- **Progress Tracking**: Saves `download_progress.json` with completed categories/groups
- **Automatic Resume**: Restarts from last checkpoint on interruption or failure
- **Granular Checkpoints**: Saves progress every 10 groups to minimize data loss
- **Status Monitoring**: Real-time progress updates with detailed logging

### Retry Mechanism
Automatic retry with exponential backoff for network resilience:

```python
@retry_with_backoff(max_retries=3, base_delay=2, max_delay=30)
def _download_products_with_retry(self, category_id: str, group_id: str, limit=None):
    return self.downloader.download_products(category_id, group_id, limit=limit)
```

- **Exponential Backoff**: 2s, 4s, 8s delays with jitter
- **Configurable**: Adjustable retry counts and delay limits
- **Resilient**: Handles temporary network failures automatically

### BigQuery Deduplication
Built-in strategies to handle multiple runs on the same day:

```python
# Remove duplicates by keeping latest records
loader.deduplicate_table_by_date()

# Delete existing data for current date before inserting
loader.delete_data_by_date('2025-08-19')

# Comprehensive load with deduplication
loader.load_tcg_products(
    append_mode=True,
    deduplicate=True,
    delete_existing_date=True
)
```

### Screen Session Management
For long-running downloads, use the screen session manager:

```bash
# Start download in background screen session
./run_in_screen.sh start

# Check if download is running
./run_in_screen.sh status

# View live logs
./run_in_screen.sh logs

# Attach to screen session
./run_in_screen.sh attach

# Stop download
./run_in_screen.sh stop
```

### Performance Optimizations
- **Multithreading**: 10 concurrent workers for API calls (configurable)
- **Chunked Processing**: Saves data in 10K record chunks to prevent memory issues
- **Incremental Saves**: Periodic checkpoint saves during download
- **Resource Efficiency**: Memory-efficient processing for large datasets

### Manual Operations
```python
# Resumable full download
from resumable_downloader import ResumableDownloader
downloader = ResumableDownloader(max_workers=10)
success = downloader.run_full_download()

# BigQuery operations with deduplication
from bigquery_loader import BigQueryLoader
loader = BigQueryLoader()

# Load with automatic deduplication
loader.load_tcg_products(
    csv_path="data/tcg_products_full_resumable.csv",
    append_mode=True,
    deduplicate=True
)

# Manual deduplication
loader.deduplicate_table_by_date()

# Price data operations
from price_downloader import TCGPriceDownloader
from bigquery_price_loader import BigQueryPriceLoader

price_downloader = TCGPriceDownloader()
price_downloader.download_and_process_date('2024-12-01')

price_loader = BigQueryPriceLoader()
price_loader.load_price_data('data/tcg_prices_2024-12-01.csv')
```

## Environment Variables (.env)
```
GOOGLE_CLOUD_PROJECT=your-project-id
GOOGLE_APPLICATION_CREDENTIALS=service-account.json
BIGQUERY_DATASET=tcg_data
```

## TCG Metadata Downloader Pipeline

High-performance metadata downloader with pure BigQuery buffering for maximum throughput.

### Features
- **Pure BigQuery Buffering**: Zero artificial delays, uses BigQuery operation latency (0.15-0.35s) as natural rate limiter
- **Ultra-Fast Downloads**: 190+ products/second (6x faster than traditional rate limiting)
- **Category Exclusion**: Skip specific categories (e.g., Pokemon variants)
- **Screen Session Management**: Background processing with comprehensive logging
- **Checkpoint Recovery**: Resume from last successful operation
- **Error Handling**: Exponential backoff retry with detailed error logging

### Table: `tcg_data.tcg_metadata` (Enhanced Metadata)
- **Product metadata**: Complete product information with enhanced fields
- **Category filtering**: Excludes Pokemon (3) and Pokemon Japan (85) by default
- **Real-time streaming**: Direct BigQuery streaming inserts during download
- **Deduplication**: Built-in duplicate prevention

### Usage

#### Full Categories Download (Excluding Pokemon)
```bash
# Run all 87 non-Pokemon categories in screen session
cd metadata_downloader
python3 run_full_categories_exclude_pokemon.py

# Monitor progress
screen -r tcg_full
tail -f full_categories_screen_fixed.log

# Check status
ps aux | grep python3
```

#### Configuration
```python
# Enhanced API downloader with pure BigQuery buffering
downloader = EnhancedAPIDownloader(
    min_request_interval=0.0,  # No artificial delays
    max_request_interval=0.0,  # Pure BigQuery buffering
    backoff_factor=1.0,
    max_retries=3
)

# Exclude specific categories
excluded_categories = [3, 85]  # Pokemon, Pokemon Japan
```

### Performance Metrics
- **Download Rate**: 190+ products/second
- **Error Rate**: <0.1% with automatic retry
- **Memory Usage**: Optimized streaming processing
- **BigQuery Buffer**: 0.15-0.35 seconds natural delay
- **Total Categories**: 87 (excluding Pokemon variants)

### File Structure
```
metadata_downloader/
├── enhanced_api_downloader.py      # Core downloader with BigQuery buffering
├── bigquery_loader.py             # Streaming BigQuery insertion
├── run_full_categories_exclude_pokemon.py  # Full categories runner
├── full_categories_checkpoint.json # Recovery checkpoint
├── full_categories_download.log    # Detailed download logs
└── full_categories_screen_fixed.log # Screen session logs
```

### Monitoring Commands
```bash
# Check screen session status
screen -list

# Attach to running download
screen -r tcg_full

# View live progress
tail -f metadata_downloader/full_categories_download.log

# Check BigQuery data
bq query --use_legacy_sql=false 'SELECT COUNT(*) FROM `rising-environs-456314-a3.tcg_data.tcg_metadata`'
```