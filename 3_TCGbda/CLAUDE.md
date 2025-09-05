# TCG BDA Data Pipeline - AI Assistant Context

## Project Overview
This project processes Trading Card Game (TCG) price data from BDA (Big Data Analytics) source into Google BigQuery for historical price tracking and analysis. The system is designed to handle daily price snapshots while preventing duplicate data uploads.

## Core Architecture

### Data Flow
1. **ZIP Files** → Located in `~/fileuploader/uploads/` with naming pattern `YYYY-MM-DD.zip`
2. **JSON Extraction** → Preserves directory structure, handles nested ZIPs automatically
3. **Batch Processing** → Memory-managed processing in configurable batch sizes
4. **BigQuery Upload** → Partitioned table with clustering for optimal query performance

### Key Design Principles
- **Historical Preservation**: Same product from different dates creates separate records (intentional)
- **Deduplication Scope**: Only prevents re-uploading the exact same file from the same scrape date
- **Date Source**: `scrape_date` extracted from ZIP filename, NOT processing date
- **Memory Management**: Batch processing with garbage collection to handle large datasets

## File Structure

```
/home/caoliu/TradingCard/3_TCGbda/
├── process_tcg_data.py          # Main processing engine
├── run_tcg_processor.sh          # Runner script with screen management
├── recreate_bq_table.py         # Table management utility
├── PARTITIONING_GUIDE.md        # Query optimization documentation
├── uploaded_files_tracker.csv   # Deduplication tracking (DO NOT DELETE)
├── product_details/             # Extracted JSON files directory
└── zip_backups/                 # Backup copies of processed ZIPs
```

## Critical Components

### 1. process_tcg_data.py
Main processor class (`TCGDataProcessor`) with:
- **Automatic nested ZIP extraction**: Handles ZIPs within ZIPs (e.g., 2025-08-20.zip contains product_details_jp.zip)
- **Deduplication tracking**: CSV-based system tracking `(filepath, scrape_date)` tuples
- **Batch processing**: Default 500 files per batch with memory monitoring
- **Date extraction**: Multiple regex patterns for YYYY-MM-DD, YYYYMMDD formats

Key methods:
- `copy_and_extract_zip()`: Extracts with structure preservation and nested ZIP handling
- `_is_file_uploaded()`: Checks tracking CSV to prevent duplicates
- `process_all_files_batched()`: Memory-efficient batch processing
- `_save_uploaded_file()`: Records successful uploads to tracking CSV

### 2. BigQuery Table Configuration
```
Table: tcg_data.tcg_prices_bda
Partitioning: Daily by scrape_date (DATE field)
Clustering: product_id, language, condition
```

Schema includes:
- `scrape_date` (DATE, REQUIRED) - Partition field from ZIP filename
- `product_id` (STRING, REQUIRED) - Primary identifier
- Price fields: `market_price`, `low_sale_price`, `high_sale_price`
- Volume fields: `quantity_sold`, `transaction_count`
- Metadata: `language`, `condition`, `variant`

### 3. Deduplication System

**How it works:**
```python
# Tracking key: (relative_filepath, scrape_date)
# Example: ("2025-09/product_details_nonjp/157623.json", "2025-09-04")
```

**Important**: 
- Product 157623 from 2025-08-20 AND 2025-09-04 will BOTH be uploaded (different dates)
- Product 157623 from 2025-09-04 uploaded twice will be skipped (duplicate prevention)
- This is INTENTIONAL for price history tracking

### 4. run_tcg_processor.sh
Wrapper script providing:
- Screen session management for long-running processes
- Logging to `/logs/` directory
- Process monitoring commands
- Tracker statistics viewing

Usage:
```bash
# Standard processing with ZIP extraction
./run_tcg_processor.sh start append ./product_details yes

# Check tracking statistics
./run_tcg_processor.sh tracker

# Monitor status
./run_tcg_processor.sh status
```

## Common Operations

### Process a new ZIP file
```bash
# Automatically finds latest ZIP in ~/fileuploader/uploads/
./run_tcg_processor.sh start append ./product_details yes
```

### Check upload progress
```bash
# Attach to screen session
screen -r tcg_processor

# View log
tail -f /logs/tcg_processor_*.log
```

### Verify data in BigQuery
```sql
-- Check data by scrape_date (uses partition)
SELECT scrape_date, COUNT(*) as records, COUNT(DISTINCT product_id) as products
FROM `tcg_data.tcg_prices_bda`
WHERE scrape_date >= '2025-08-01'
GROUP BY scrape_date
ORDER BY scrape_date DESC;
```

### Clean and restart (DANGEROUS)
```bash
# Only if absolutely necessary
python3 recreate_bq_table.py  # Will prompt for confirmation
rm uploaded_files_tracker.csv  # Reset tracking
```

## Important Implementation Details

### ZIP File Processing
1. ZIP files may contain nested ZIPs (handled automatically since commit 638bdfb)
2. Directory structure MUST be preserved during extraction
3. scrape_date extracted from main ZIP filename, not nested files
4. Backup created in `zip_backups/` before processing

### Memory Management
- Default batch size: 500 files
- Max memory: 512 MB (configurable)
- Garbage collection after each batch
- Memory monitoring in logs

### Error Handling
- Failed batches don't update tracking CSV
- Partial uploads can be resumed (deduplication prevents re-upload)
- Screen session preserves process on disconnect

## Query Optimization

Due to partitioning and clustering, ALWAYS include `scrape_date` in WHERE clause:

```sql
-- ✅ FAST: Uses partition
SELECT * FROM `tcg_data.tcg_prices_bda`
WHERE scrape_date = '2025-09-04'
  AND product_id = '157623'

-- ❌ SLOW: Full table scan
SELECT * FROM `tcg_data.tcg_prices_bda`
WHERE product_id = '157623'  -- Missing scrape_date!
```

## Troubleshooting

### "Batch X: Uploaded Y files" where Y < 500
- **Normal behavior**: Some files already uploaded (check tracking CSV)
- Files from same scrape_date being skipped as duplicates

### Memory errors
- Reduce batch size: `./run_tcg_processor.sh start append ./product_details yes 250`
- Increase max memory: Last parameter, e.g., `... yes 500 1024`

### Wrong scrape_date
- Check ZIP filename format (must be YYYY-MM-DD.zip or YYYYMMDD.zip)
- Date extracted from filename, not system date

### Duplicate data concerns
- Check tracking CSV: `grep "product_id" uploaded_files_tracker.csv`
- Each (file, date) pair should appear only once
- Same product, different dates = expected behavior

## Best Practices for AI Assistants

1. **Always preserve existing tracking data** - Never delete uploaded_files_tracker.csv unless explicitly requested
2. **Check for nested ZIPs** - 2025-08-20.zip structure is different from 2025-09-04.zip
3. **Use screen sessions** - Long uploads can take 10-15 minutes
4. **Monitor memory usage** - Logs show memory per batch
5. **Verify partitioning** - Ensure scrape_date is correctly set before upload
6. **Test with small batches first** - Use `--batch-size 10` for testing
7. **Never modify schema** without recreating table (partitioning requires recreation)

## Recent Changes
- **2025-09-05**: Added automatic nested ZIP extraction
- **2025-09-05**: Implemented partitioning by scrape_date
- **2025-09-05**: Added deduplication tracking system
- **2025-09-05**: Fixed scrape_date extraction from ZIP filenames

## Environment Requirements
- Python 3.12+
- Google Cloud SDK configured
- Environment variables in `.env`:
  - `GOOGLE_CLOUD_PROJECT=rising-environs-456314-a3`
- Dependencies: `google-cloud-bigquery`, `pandas`, `python-dotenv`

## Data Volume Expectations
- Each ZIP: ~57,000-58,000 JSON files
- Each upload: ~10 million records
- Processing time: 10-15 minutes per ZIP
- Table growth: ~10M records per daily snapshot

## Contact & Support
This is a data pipeline for TCG price tracking. The system is designed for daily batch processing of price snapshots from BDA source, maintaining historical price data for analysis.