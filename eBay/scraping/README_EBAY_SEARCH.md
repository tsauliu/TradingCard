# eBay Search API Scripts

Advanced Python scripts for searching eBay sold items and extracting price/sales metrics with Excel pivot table support.

## Features

- **Single & Batch Search**: Search one or multiple keywords
- **Excel Pivot Tables**: Generate pivot tables with prices and quantities by week
- **Metrics Extraction**: Automatic extraction of average prices, sales volumes, and trends
- **Proxy Support**: Built-in support for HTTP proxy (default: 127.0.0.1:20171)
- **Cookie Management**: Handle authentication via cookies
- **Multiple Output Formats**: JSON, CSV, Excel reports with pivot tables
- **Pagination Support**: Handle large result sets with offset/limit
- **Error Handling**: Automatic retries with exponential backoff
- **Time Aggregation**: Weekly, monthly, or quarterly data aggregation
- **Visualization**: Optional charts and formatting in Excel output

## Installation

```bash
pip install -r requirements.txt
```

## Setup

1. **Get eBay Cookies** (Required for full data access):
   - Open eBay Research page in browser: https://www.ebay.com/sh/research
   - Open Developer Tools (F12)
   - Go to Network tab
   - Search for something on the page
   - Find a request to `/sh/research/api/search`
   - Copy the entire `cookie` header value
   - Save to `ebay_cookies.txt`

## Usage

### Single Search

Basic search (JSON output):
```bash
python ebay_search.py "pokemon cards"
```

Excel pivot table output:
```bash
python ebay_search.py "pokemon cards" --excel --output pokemon.xlsx
python ebay_search.py "magic cards" --excel --no-charts  # Without charts
```

With options:
```bash
python ebay_search.py "magic the gathering" \
  --days 365 \
  --limit 100 \
  --extract-metrics \
  --output mtg_data.json
```

Search without proxy:
```bash
python ebay_search.py "vintage electronics" --no-proxy
```

### Batch Search

Search multiple keywords:
```bash
python ebay_batch_search.py "pokemon cards" "magic the gathering" "yugioh"
```

Generate Excel pivot table:
```bash
# Create pivot table with all searches
python ebay_batch_search.py "pokemon" "magic" "yugioh" --excel-pivot trading_cards

# Monthly aggregation
python ebay_batch_search.py --file keywords.txt --excel-pivot analysis --time-period monthly

# Quarterly without charts
python ebay_batch_search.py "pokemon" "magic" --excel-pivot quarterly_report --time-period quarterly --no-charts
```

From file (one keyword per line):
```bash
python ebay_batch_search.py --file keywords.txt --days 365
```

With custom output directory:
```bash
python ebay_batch_search.py --file cards.txt --output-dir results/ --delay 3
```

## Command Line Options

### ebay_search.py

| Option | Description | Default |
|--------|-------------|---------|
| `keywords` | Search keywords (required) | - |
| `--output, -o` | Output JSON filename | Auto-generated |
| `--days, -d` | Number of days to look back | 1095 (3 years) |
| `--marketplace, -m` | eBay marketplace | EBAY-US |
| `--category, -c` | Category ID (0 for all) | 0 |
| `--offset` | Result offset for pagination | 0 |
| `--limit, -l` | Results per page (max 200) | 50 |
| `--proxy, -p` | Proxy URL | http://127.0.0.1:20171 |
| `--no-proxy` | Disable proxy | False |
| `--cookie-file` | Cookie file path | ebay_cookies.txt |
| `--extract-metrics` | Extract and display metrics | False |
| `--compact` | Save JSON in compact format | False |
| `--excel` | Save as Excel pivot table | False |
| `--no-charts` | Disable charts in Excel | False |
| `--verbose, -v` | Enable verbose logging | False |

### ebay_batch_search.py

| Option | Description | Default |
|--------|-------------|---------|
| `keywords` | Search keywords (multiple) | - |
| `--file, -f` | File with keywords | - |
| `--days, -d` | Number of days to look back | 1095 |
| `--marketplace, -m` | eBay marketplace | EBAY-US |
| `--limit, -l` | Results per search | 50 |
| `--delay` | Delay between searches (seconds) | 2.0 |
| `--output-dir, -o` | Output directory | . |
| `--no-report` | Skip report generation | False |
| `--excel-pivot` | Generate pivot Excel with name | None |
| `--time-period` | Aggregation (weekly/monthly/quarterly) | weekly |
| `--no-charts` | Disable charts in Excel | False |
| `--proxy, -p` | Proxy URL | http://127.0.0.1:20171 |
| `--no-proxy` | Disable proxy | False |
| `--cookie-file` | Cookie file path | ebay_cookies.txt |

## Output Files

### Single Search
- **JSON File**: Complete API response with extracted metrics
  ```json
  {
    "PageErrorModule": {...},
    "MetricsTrendsModule": {...},
    "extracted_metrics": {
      "statistics": {
        "avg_price": 33.34,
        "total_sold": 12221985,
        ...
      }
    },
    "search_metadata": {...}
  }
  ```

- **Excel File** (with --excel flag): Pivot table format with 4 sheets:
  - **Prices**: Weekly average prices (columns are dates)
  - **Quantities**: Weekly sales volumes (columns are dates)
  - **Statistics**: Summary statistics for the search
  - **Metadata**: Search parameters and metadata

### Batch Search
- **batch_results_*.json**: Raw results for all searches
- **comparison_*.csv**: Comparison table
- **comparison_*.xlsx**: Excel report with formatting
- **summary_*.txt**: Text summary of all searches
- **pivot_table_*.xlsx** (or custom name): Multi-keyword pivot table with:
  - **Sheet 1 - Prices**: Rows are keywords, columns are weeks
  - **Sheet 2 - Quantities**: Rows are keywords, columns are weeks
  - **Sheet 3 - Statistics**: Summary stats for all keywords
  - **Sheet 4 - Metadata**: Search parameters

### Excel Pivot Table Structure
```
Sheet 1 - Prices:
Keywords         | 2024-08-05 | 2024-08-12 | ... | 2025-08-26 |
pokemon cards    |     45.60  |     48.25  | ... |     58.64  |
magic gathering  |     32.30  |     34.80  | ... |     35.12  |
yugioh cards     |     24.75  |     26.20  | ... |     28.12  |

Sheet 2 - Quantities:
Keywords         | 2024-08-05 | 2024-08-12 | ... | 2025-08-26 |
pokemon cards    |     18234  |     19567  | ... |     19682  |
magic gathering  |     12456  |     13234  | ... |     14096  |
yugioh cards     |       856  |       923  | ... |       732  |
```

## API Response Structure

The eBay API returns multiple modules:
- **PageErrorModule**: Error information (if any)
- **MetricsTrendsModule**: Price and sales data over time
  - `averageSold`: Weekly average prices
  - `quantity`: Weekly sales volumes
  - `quantityRegressionLine`: Trend line

## Examples

### Example 1: Search Trading Cards
```bash
# Search Pokemon cards for last year
python ebay_search.py "pokemon cards charizard" --days 365 --extract-metrics

# Output shows:
# Average Price: $41.74
# Total Sold: 6,042,856
# Data saved to: ebay_search_pokemon_cards_charizard_20250902_101500.json
```

### Example 2: Compare Multiple Products
```bash
# Create keywords file
echo "pokemon cards booster box" > keywords.txt
echo "magic the gathering booster box" >> keywords.txt
echo "yugioh booster box" >> keywords.txt

# Run batch search
python ebay_batch_search.py --file keywords.txt --days 90

# Creates comparison table showing price differences
```

### Example 3: Track Price Trends
```bash
# Get 3 years of data with metrics
python ebay_search.py "vintage rolex submariner" \
  --days 1095 \
  --extract-metrics \
  --output rolex_trends.json

# Extracts weekly price points for trend analysis
```

## Troubleshooting

### "auth_required" Error
- You need valid cookies from eBay
- Cookies may have expired - get fresh ones

### PageErrorModule in Response
- This is normal - the API returns both error and data modules
- Check if MetricsTrendsModule is present for actual data

### Proxy Connection Issues
- Check if proxy is running: `curl -x http://127.0.0.1:20171 http://example.com`
- Use `--no-proxy` to bypass proxy

### No Metrics Data
- Some searches may not have enough sales data
- Try broader search terms or longer time periods

## Advanced Usage

### Programmatic Usage
```python
from ebay_search import eBaySearchAPI

# Create API client
api = eBaySearchAPI(proxy="http://127.0.0.1:20171")

# Search
results = api.search(
    keywords="rare pokemon cards",
    days=365,
    limit=100
)

# Extract metrics
metrics = api.extract_metrics(results)
print(f"Average price: ${metrics['statistics']['avg_price']:.2f}")
```

### Custom Analysis
```python
import pandas as pd
import json

# Load results
with open("pokemon_test.json") as f:
    data = json.load(f)

# Convert to DataFrame
metrics = data['extracted_metrics']
df = pd.DataFrame(metrics['avg_prices'])
df['date'] = pd.to_datetime(df['date'])
df.set_index('date', inplace=True)

# Analyze trends
print(df['price'].resample('M').mean())  # Monthly averages
```

## Notes

- The API returns weekly aggregated data, not individual listings
- Maximum lookback period is typically 3 years
- Rate limiting: Use delays between requests in batch mode
- Cookies expire periodically and need refreshing
- Some marketplaces may have different data availability

## Excel Pivot Table Features

### Time Period Aggregation
The scripts support different time period aggregations:
- **Weekly** (default): Raw weekly data as returned by API
- **Monthly**: Averages grouped by month (YYYY-MM format)
- **Quarterly**: Averages grouped by quarter (YYYY-Q# format)

### Formatting and Styling
Excel files include:
- Color-coded headers and keywords column
- Number formatting (currency for prices, thousands separator for quantities)
- Auto-adjusted column widths
- Borders for better readability
- Conditional formatting for high/low values

### Charts (optional)
When charts are enabled (default), the Excel includes:
- Price trend line charts
- Sales volume trend charts
- Each keyword as a separate series
- Proper axis labels and titles

### Use Cases
1. **Price Comparison**: Compare prices across different products over time
2. **Trend Analysis**: Identify seasonal patterns and trends
3. **Market Research**: Analyze market size and competition
4. **Investment Analysis**: Track collectible card values over time
5. **Reporting**: Generate professional reports with charts

## Files

- `ebay_search.py` - Main search script with Excel support
- `ebay_batch_search.py` - Batch search with pivot tables
- `ebay_excel_utils.py` - Excel pivot table utilities
- `requirements.txt` - Python dependencies
- `ebay_cookies.txt.example` - Cookie file template
- `README_EBAY_SEARCH.md` - This documentation