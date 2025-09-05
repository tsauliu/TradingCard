# PSA Weekly Analysis Script - Usage Guide

## Updated Script Features

The `create_psa_weekly_analysis.py` script has been updated to filter data by the `scraped_at` date. This allows you to:
- Process only data from specific scraping sessions
- Avoid reprocessing all historical data
- Create targeted analyses for specific data collection runs

## Key Changes

1. **Date-based filtering**: Now filters by `scraped_at` date instead of pulling all historical data
2. **Efficient partition filtering**: Uses BigQuery table partitioning for optimal query performance
3. **Parameterized queries**: Safer and more efficient query execution
4. **Enhanced output naming**: Includes scrape date in the output filename

## Usage

### Basic Usage with Specific Date
```bash
python create_psa_weekly_analysis.py --date 2025-01-05
```

### Using Latest Scrape Date
```bash
python create_psa_weekly_analysis.py --date latest
```

### Examples
```bash
# Analyze data scraped on January 5, 2025
python create_psa_weekly_analysis.py --date 2025-01-05

# Analyze the most recent scraping session
python create_psa_weekly_analysis.py --date latest

# Check available dates (will be shown if invalid date is provided)
python create_psa_weekly_analysis.py --date 2099-01-01
```

## Output

The script will generate an Excel file with the following naming pattern:
```
YYYY-MM-DD_HHMMSS_psa_weekly_analysis_scraped_YYYY-MM-DD.xlsx
```

Example: `2025-01-05_143025_psa_weekly_analysis_scraped_2025-01-05.xlsx`

## How It Works

1. **Partition Filtering**: The script uses efficient timestamp range filtering:
   ```sql
   WHERE scraped_at >= TIMESTAMP(@scrape_date)
     AND scraped_at < TIMESTAMP(DATE_ADD(@scrape_date, INTERVAL 1 DAY))
   ```
   This leverages BigQuery's table partitioning for optimal performance.

2. **Both CTEs Filtered**: Both `sales_data` and `summary_data` CTEs are filtered by the same `scraped_at` date

3. **Date Validation**: The script validates the date format and shows available dates if no data exists for the specified date

4. **Latest Option**: When using `--date latest`, the script automatically queries for the most recent `scraped_at` date in the table

## Error Handling

If no data exists for the specified date, the script will:
1. Display an error message
2. Show the 10 most recent available scrape dates with record counts
3. Suggest using one of those dates or the `latest` option

## Performance Benefits

- **Reduced data scanning**: Only scans data from the specific partition
- **Faster execution**: Processes only relevant data instead of entire table
- **Lower costs**: Reduces BigQuery data processing costs
- **Targeted analysis**: Focuses on specific data collection sessions