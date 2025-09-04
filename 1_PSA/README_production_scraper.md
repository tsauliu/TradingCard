# PSA Production Scraper - Usage Guide

## Quick Start

### Dry Run Test (Recommended First)
```bash
python scrape_psa_production.py --dry-run
```

### Full Production Run
```bash
python scrape_psa_production.py
```

## What's Included

### 1. Enhanced Duplicate Prevention
- ✅ Checks existing BigQuery records before scraping
- ✅ Skips combinations already processed today
- ✅ Tracks processed combinations in memory

### 2. Production Rate Limiting
- ✅ 35 seconds between API requests (exceeds 30s requirement)
- ✅ Exponential backoff for failed requests (5, 7, 11, 19 seconds)
- ✅ Maximum 5 retry attempts per request

### 3. Top 10 SKUs Processing
- ✅ Automatically loads first 10 cards from `psa_card_list.csv`
- ✅ Processes all 19 PSA grades per card
- ✅ Total: 190 API requests (10 cards × 19 grades)

### 4. Data Safety Features
- ✅ Checkpoint saves every 2 cards
- ✅ Automatic BigQuery table creation
- ✅ Data validation before insertion
- ✅ Comprehensive error handling

### 5. Monitoring & Reporting
- ✅ Real-time progress tracking with ETA
- ✅ Detailed logging to `psa_production_scraper.log`
- ✅ Final summary report with statistics

## Expected Performance

- **Runtime**: ~110 minutes (35s × 190 requests)
- **Data Volume**: 20,000-50,000 records
- **BigQuery Table**: `tcg_data.psa_auction_prices`
- **Checkpoint Files**: `checkpoint_cards_1_to_N_TIMESTAMP.csv`

## Deduplication Queries

Use `bigquery_deduplication_queries.sql` for post-processing cleanup:
- Check for duplicates
- Remove duplicate records
- Data quality validation
- Coverage analysis

## Resume Capability

If interrupted, the scraper will:
1. Check existing data for today
2. Skip already-processed combinations
3. Continue from where it left off

## Usage Examples

```bash
# Test run (simulates API calls)
python scrape_psa_production.py --dry-run

# Production run (real API calls + BigQuery)
python scrape_psa_production.py

# Monitor progress
tail -f psa_production_scraper.log
```

Ready for production use!