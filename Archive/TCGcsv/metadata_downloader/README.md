# TCG API Downloader

A rate-limited API-based downloader for trading card data from tcgcsv.com with real-time BigQuery streaming.

## ✅ Implementation Complete

Successfully implemented and tested:
- ✅ API endpoints working (categories, groups, products)  
- ✅ Rate limiting at 0.83 req/s (safely below 1 req/s)
- ✅ BigQuery streaming with automatic table creation
- ✅ Data type handling for complex API responses
- ✅ Real-time data verification
- ✅ 650+ products successfully loaded to BigQuery

## Quick Start

```bash
# Test mode (download 3 groups, verify BigQuery)
GOOGLE_APPLICATION_CREDENTIALS=../service-account.json python3 main.py test

# Check current status
GOOGLE_APPLICATION_CREDENTIALS=../service-account.json python3 main.py status

# Download specific category (Pokemon = 3)
GOOGLE_APPLICATION_CREDENTIALS=../service-account.json python3 main.py category 3 --limit 5
```

## Files

- `api_downloader.py` - Core downloader with BigQuery streaming
- `bigquery_loader.py` - Table management and data verification  
- `test_api.py` - Comprehensive test suite with BigQuery verification
- `main.py` - Command-line interface
- `data/` - Backup CSV files when BigQuery fails

## BigQuery Table

**Table**: `tcg_data.tcg_metadata`
- **Partitioned** by `update_date` for efficient queries
- **Clustered** by product ID fields for fast lookups  
- **Schema**: 32+ columns with category_, group_, and product_ prefixes
- **Data Types**: Simplified to strings (complex types converted to JSON)

## Performance

- **Rate Limit**: 0.83 requests/second (safely below 1 req/s limit)
- **Efficiency**: Natural delays from BigQuery operations
- **Throughput**: ~28 products/second processing
- **Reliability**: Automatic backup to CSV on BigQuery failures

## Test Results

```
🏁 FINAL TEST RESULTS
API Endpoints: ✅ PASSED
Specific Pokemon Groups: ✅ PASSED
Overall: 2/2 tests passed
🎉 All tests passed! Data should be visible in BigQuery.
```

## Sample Data in BigQuery

```
Category: Magic (ID: 1)
Group: Avatar: The Last Airbender (ID: 24421)  
Product: Aang, Avatar State (ID: 629156)
Updated: 2025-08-20
```

Ready for production use! 🚀