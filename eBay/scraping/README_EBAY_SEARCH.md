# eBay Search API Scripts

Advanced Python scripts for searching eBay sold items and extracting price/sales metrics.

## Features

- **Single & Batch Search**: Search one or multiple keywords
- **Metrics Extraction**: Automatic extraction of average prices, sales volumes, and trends
- **Proxy Support**: Built-in support for HTTP proxy (default: 127.0.0.1:20171)
- **Cookie Management**: Handle authentication via cookies
- **Multiple Output Formats**: JSON, CSV, Excel reports
- **Pagination Support**: Handle large result sets with offset/limit
- **Error Handling**: Automatic retries with exponential backoff

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

Basic search:
```bash
python ebay_search.py "pokemon cards"
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

### Batch Search
- **batch_results_*.json**: Raw results for all searches
- **comparison_*.csv**: Comparison table
- **comparison_*.xlsx**: Excel report with formatting
- **summary_*.txt**: Text summary of all searches

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

## Files

- `ebay_search.py` - Main search script
- `ebay_batch_search.py` - Batch search with comparison
- `requirements.txt` - Python dependencies
- `ebay_cookies.txt.example` - Cookie file template
- `README_EBAY_SEARCH.md` - This documentation