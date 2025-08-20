# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a BigQuery ETL pipeline for Trading Card Game (TCG) price data. The system processes JSON files containing product pricing information and loads them into Google BigQuery for analysis. The main purpose is to extract, transform, and load TCG pricing data with product IDs extracted from filenames.

## Key Commands

### Environment Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Set up Google Cloud authentication (choose one)
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account.json"
# OR
gcloud auth application-default login

# Set required environment variables
export GOOGLE_CLOUD_PROJECT="your-gcp-project-id"
export BIGQUERY_DATASET="tcg_data"  # Optional, defaults to tcg_data
```

### Running the Pipeline

```bash
# Test processing logic first (recommended)
python test_script.py

# Standard batch loading (slower, good for incremental loads)
python load_to_bigquery.py

# Optimized bulk loading (faster, replaces all data)
python bulk_load_optimized.py

# Load all data with progress tracking (append mode)
python load_all_data.py

# Verify data after loading
python verify_data.py
```

### Data Management
```bash
# Delete legacy table data
python delete_legacy_table.py

# Load test subset
python load_test_data.py
```

## Architecture

### Data Flow
1. **Input**: JSON files in `./product_details/` with naming pattern `{product_id}.0.json`
2. **Processing**: Extract product ID from filename, flatten nested JSON structure
3. **Transformation**: Convert data types, handle buckets array (price history)
4. **Output**: BigQuery table `tcg_prices_bda` with normalized schema

### Core Components

**Main Scripts:**
- `load_to_bigquery.py` - Standard ETL pipeline with batch processing
- `bulk_load_optimized.py` - High-performance bulk loader (processes locally first)
- `load_all_data.py` - Comprehensive loader with detailed progress tracking
- `verify_data.py` - Data validation and quality checks

**Configuration:**
- `config.py` - BigQuery settings, schema mappings, and data type conversions
- `requirements.txt` - Python dependencies (BigQuery, pandas, dotenv)

**Utilities:**
- `test_script.py` - Validation script for testing processing logic
- `delete_legacy_table.py` - Table cleanup utility

### Data Schema

The BigQuery table schema includes:
- **Product Info**: product_id (from filename), sku_id, variant, language, condition
- **Aggregates**: average_daily_quantity_sold, total_quantity_sold, etc.
- **Price Buckets**: bucket_start_date, market_price, quantity_sold, price ranges
- **Metadata**: file_processed_at timestamp

### Key Functions

**Data Processing (`load_to_bigquery.py:21`):**
- `extract_product_id()` - Extracts product ID from filename pattern
- `process_json_file()` - Flattens JSON structure, creates records per bucket
- `create_bigquery_table_schema()` - Defines BigQuery table schema

**Performance Optimizations (`bulk_load_optimized.py:105`):**
- `process_all_files_locally()` - Processes all files in memory before upload
- Single bulk upload instead of multiple batch uploads
- Better error handling and progress tracking

## Configuration Notes

- Update `config.py` with your GCP project details before running
- The `service-account.json` file contains GCP credentials (keep secure)
- Default table name is `tcg_prices_bda` in the `tcg_data` dataset
- Batch size is configurable (default: 1000 records for standard, full dataset for bulk)

## Error Handling

- Individual file failures don't stop the entire process
- Progress tracking shows processing rates and ETAs
- Data type conversion errors are handled gracefully with coercion
- Validation script helps identify issues before full loads